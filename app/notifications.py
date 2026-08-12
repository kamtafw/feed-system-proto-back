"""
notifications.py — Notification subsystem: domain layer and event consumers.

This module is the SINGLE entry point into the Notification subsystem, in
both directions:
    - Writes:   event bus consumers (on_post_created, on_follow_created)
                that translate PostCreated / FollowCreated into
                notification rows.
    - Reads:    list_notifications / mark_read / mark_all_read /
                get_unread_count, called exclusively by app.py's HTTP
                routes.

app.py never calls db.py's notification functions directly, and never
will — everything notification-related goes through this module. This is
the same encapsulation already kept between app.py and cache.py for
Redis: a subsystem should have exactly one place that knows how its own
state is stored.

-----------------------------------------------------------
Why this is not part of consumers.py
-----------------------------------------------------------
fanout_consumer and realtime_consumer both react to PostCreated — so does
on_post_created below, and it would have been easy to just add a third
loop to fanout_consumer. Deliberately not done: timeline fanout and
notification creation are different business capabilities that happen to
react to the same event. Growing consumers.py into "everything that
happens after a post" would blur a boundary worth keeping.

-----------------------------------------------------------
Why every notification-producing event enters via the event bus
-----------------------------------------------------------
FollowCreated has exactly one recipient — no fanout at all — and could
have been written synchronously inside the HTTP handler, the same way
the follow row itself is. It isn't, on purpose: if some notification
types entered synchronously while others went through the bus, every
future notification type (likes, mentions, reposts) would force a fresh
"sync or async?" decision, and notification creation would gradually
scatter across HTTP handlers and consumers instead of staying owned by
one subsystem. Uniform entry means that decision only ever gets made
once — cardinality (1:1 vs 1:N) becomes an implementation detail inside
this module, invisible to the rest of the architecture.

-----------------------------------------------------------
Identity, idempotency, and why M7's hybrid fanout does not apply here
-----------------------------------------------------------
A notification's identity is (recipient_id, actor_id, type, object_type,
object_id) — a domain concept, deliberately never a Redis Stream message
ID, so it survives a future bus migration (e.g. the roadmap's planned
M13 Streams -> Kafka move) untouched. See db.create_notification()'s
docstring and docs/milestone-8-notification-store.md ADR-1.

Unlike a timeline entry, a notification's read/unread state is
irreplaceable per-recipient state with no source to derive it from
later. M7's read-time-merge trick is safe only because fanout-on-write
and fanout-on-read are OUTPUT-EQUIVALENT for a reconstructible
collection — a follower genuinely cannot tell which happened. That
equivalence does not hold here: there is no way to reconstruct, after
the fact, whether a specific recipient has acknowledged a specific
event. Deferring the write wouldn't defer computation, it would simply
never create the data. So on_post_created below writes one row per
follower unconditionally, with no light/heavy branch — a known,
deliberately deferred scalability gap for celebrity-scale accounts (see
ADR-4 / ADR-5 in the milestone doc for the reasoning and candidate future
strategies).
"""

import time
from typing import List, Optional, Tuple

from app import db

NEW_POST = "NEW_POST"
NEW_FOLLOWER = "NEW_FOLLOWER"

# Event consumers — the subsystem's write-side entry point.
# Both are registered in worker.py, alongside fanout_consumer and
# realtime_consumer, but deliberately live in their own module.


async def on_post_created(payload: dict) -> None:
    """
    Subscribed to 'PostCreated'. Writes one notification per follower,
    unconditionally — no light/heavy branch (see module docstring and
    milestone doc ADR-4). Known celebrity-scale gap, deferred by design
    (ADR-5): suppression, digest/aggregation, and batched writes are the
    named candidate strategies, none implemented here.

    created_at is captured ONCE for the whole fan-out batch, not
    per-notification. This is deliberate, not an oversight: it's exactly
    why db.get_notifications()'s cursor is (created_at, id) rather than
    created_at alone — a real fanout run is precisely the case where many
    rows for the same recipient legitimately share one timestamp.

    The author never gets a notification for their own post.
    """
    post_id = payload["post_id"]
    author_id = payload["author_id"]
    batch_created_at = time.time()

    followers = await db.get_followers(author_id)
    for follower_id in followers:
        await db.create_notification(
            recipient_id=follower_id,
            actor_id=author_id,
            notif_type=NEW_POST,
            object_type="post",
            object_id=post_id,
            created_at=batch_created_at,
        )


async def on_follow_created(payload: dict) -> None:
    """
    Subscribed to 'FollowCreated'. Always exactly one recipient — this
    notification type is structurally immune to the celebrity fan-out
    problem on_post_created has, no matter how large anyone's follower
    count ever gets.

    NEW_FOLLOWER represents the follower RELATIONSHIP, not the follow
    ACTION (milestone doc ADR-3): `follows` is a current-state table
    (PK on (follower_id, followee_id)), not an event log, so refollowing
    after an unfollow does not create a second notification and does NOT
    reset and already-read notification back to unread. ON CONFLICT DO
    NOTHING is therefore a PRODUCT decision here, not merely
    infrastructure dedup — it happens to reuse the same SQL clause that
    also absorbs Streams redelivery, but the two purposes are distinct
    and this is the one that's a real behavioral choice.

    object_type/object_id are self-referential (object_id =
    recipient_id): the "object" of a follow notification is the
    relationship targeting the recipient them-self, not a distinct
    entity. This keeps the uniqueness constraint NULL-free and uniform
    across every notification type — see db.py's schema comment for why
    a nullable object column would have silently broken deduplication
    for exactly this notification type.
    """
    follower_id = payload["follower_id"]
    followee_id = payload["followee_id"]
    created_at = payload["created_at"]

    await db.create_notification(
        recipient_id=followee_id,
        actor_id=follower_id,
        notif_type=NEW_FOLLOWER,
        object_type="user",
        object_id=followee_id,  # self-referential
        created_at=created_at,
    )


# Read-side domain functions — the subsystem's only read entry point.
# app.py's HTTP routes call these exclusively; nothing outside this
# module queries the notification table.


async def list_notifications(
    recipient_id: str,
    cursor: Optional[Tuple[float, int]] = None,
    limit: int = 50,
) -> Tuple[List[dict], Optional[Tuple[float, int]]]:
    """
    cursor is (created_at, id) — see db.get_notifications()'s docstring
    for why a single created_at value isn't a stable ordering here, the
    way it is for M6's timeline cursor. Returns (items, next_cursor);
    next_cursor is None once there's no further page.
    """
    fetch_n = limit + 1
    cursor_created_at, cursor_id = cursor if cursor else (None, None)

    rows = await db.get_notifications(
        recipient_id,
        cursor_created_at=cursor_created_at,
        cursor_id=cursor_id,
        limit=fetch_n,
    )

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = (page[-1]["created_at"], page[-1]["id"]) if (has_more and page) else None
    return page, next_cursor


async def mark_read(notification_id: int, recipient_id: str) -> bool:
    """False means "doesn't exist for this recipient" — see
    db.mark_notification_read()'s docstring for why that's the only
    distinction the caller needs (opaque id, ownership is the real
    boundary, not obscurity)."""
    return await db.mark_notification_read(notification_id, recipient_id)


async def mark_all_read(recipient_id: str) -> int:
    """Returns the number of notifications actually transitioned to read."""
    return await db.mark_all_notifications_read(recipient_id)


async def get_unread_count(recipient_id: str) -> int:
    """Served by the partial index on (recipient_id, created_at DESC, id
    DESC) WHERE read_at IS NULL — stays cheap regardless of how large
    total notification history grows, since its size tracks unread
    volume, not total volume."""
    return await db.count_unread_notifications(recipient_id)
