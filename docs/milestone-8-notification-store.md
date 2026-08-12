# Milestone 8 — Persistent Notification Store

## FanoutFeed · `milestone-8-notification-store`

---

## Goal

Give users a durable record of user-specific events — "Bob followed you,"
"Alice posted" — that survives being offline, independent of whether a
WebSocket happened to be open at the moment the event occurred. Turn the
existing best-effort `NEW_POST` WebSocket push from the *only* delivery
mechanism into an optional live hint layered on top of a durable store.

---

## Why now

WebSocket notifications today are purely ephemeral: if you're offline,
or your client misses the push, it's gone — there is no record of "you
had 5 notifications while you were away." Roadmap-driven (M8 was always
the named next milestone after M7.5), reinforced by M7.5's own
Section-9 observation that `realtime_consumer`'s O(followers) push loop
can't be fixed in isolation, because a persistent store changes what
"notify a follower" even *means* — a durable write plus an optional live
push, rather than only a live push. `realtime_consumer`'s loop remains
explicitly out of scope for M8 (see Known Limitations).

---

## The domain model — what a notification actually is

A notification is not "a log entry for an event." Its identity is
**(recipient, occurrence)**, and the useful shape for describing an
occurrence turns out to be the Activity Streams pattern: **actor acted
on object, for recipient** — "Alice posted Post 52, notifying Bob,"
"Alice followed Bob." That structure is not invented for this project;
it's the same shape behind GitHub's notification feed and ActivityPub.

The property that actually defines the subsystem boundary: **a
notification's read/unread state is irreplaceable per-recipient state,
with no source it can be derived from.** No query against `posts` or
`follows` can answer "did Bob see notification #4471" — only the
notification row itself can. Contrast a timeline entry, which is a
pointer into a reconstructible collection: wipe every `timeline:{id}`
key and it can be rebuilt from `posts` + `follows`. Nothing about a
notification's read state can be rebuilt the same way. That distinction
is the reason this is its own subsystem rather than a side effect living
inside `fanout_consumer`, and the reason ADR-4 below exists.

---

## Schema

```sql
CREATE TABLE IF NOT EXISTS notifications (
    id            BIGSERIAL        PRIMARY KEY,
    recipient_id  TEXT             NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    actor_id      TEXT             NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type          TEXT             NOT NULL,   -- 'NEW_POST' | 'NEW_FOLLOWER'
    object_type   TEXT             NOT NULL,   -- 'post' | 'user'
    object_id     TEXT             NOT NULL,
    created_at    DOUBLE PRECISION NOT NULL,
    read_at       DOUBLE PRECISION,            -- NULL = unread

    UNIQUE (recipient_id, actor_id, type, object_type, object_id)
);

CREATE INDEX IF NOT EXISTS idx_notifications_unread
    ON notifications (recipient_id, created_at DESC, id DESC)
    WHERE read_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_notifications_recipient
    ON notifications (recipient_id, created_at DESC, id DESC);
```

**Identity**: `(recipient_id, actor_id, type, object_type, object_id)`.
Domain-derived, not a Redis Stream message ID — see ADR-1.

**`id` is `BIGSERIAL`, not the `TEXT` UUID convention `posts`/`users`
use.** Deliberate deviation: post IDs are user-facing (appear in URLs);
a notification ID is only ever an opaque handle for `mark_read()`'s
ownership check, never rendered as content. `BIGSERIAL` gives a smaller
index for a table written far more often, and read far more narrowly,
than `posts`.

**`NEW_FOLLOWER`'s object is self-referential**: `object_type='user'`,
`object_id=recipient_id`. Postgres treats `NULL <> NULL` inside a
`UNIQUE` constraint, so a nullable object column — the naive choice,
since a follow has no object distinct from the recipient — would have
silently broken deduplication for exactly the one notification type
that needs it most. Making the object "the relationship targeting the
recipient themself" keeps the constraint uniform and NULL-free across
every current and future notification type.

**Two indexes, not one**, because the two real query shapes differ:
"unread since last check" (badge, default inbox view) is the hot path
and gets the partial index, whose size tracks unread volume rather than
total history; "full paginated history" is the cold path and gets the
general index.

---

## Domain functions (`app/notifications.py`)

```
on_post_created(payload)     — event consumer: one row per follower
on_follow_created(payload)   — event consumer: exactly one row

list_notifications(recipient_id, cursor, limit) -> (items, next_cursor)
mark_read(notification_id, recipient_id) -> bool
mark_all_read(recipient_id) -> int
get_unread_count(recipient_id) -> int
```

`app/notifications.py` is the Notification subsystem's **single entry
point**, in both directions. `app.py` never calls `app/db.py`'s
notification functions directly — the same encapsulation already kept
between `app.py` and `app/cache.py` for Redis. `app/db.py` remains the
only module that writes raw SQL (see `create_notification`,
`get_notifications`, `mark_notification_read`,
`mark_all_notifications_read`, `count_unread_notifications`).

`mark_notification_read`'s `COALESCE(read_at, $3)` is worth calling out:
it makes the operation idempotent on the *value* of `read_at` (marking
an already-read notification read again doesn't bump the timestamp),
while still matching the row and returning success — an already-read
notification is not a 404, since the desired end state already holds.

---

## Event payloads

**`PostCreated`** — unchanged from M2/M7: `post_id`, `author_id`,
`author_name`, `created_at`.

**`FollowCreated`** — new, published from `app.py`'s `follow_user`
handler, immediately after `db.add_follow()` commits:

```json
{ "follower_id": "bob", "followee_id": "alice", "created_at": 1234567890.0 }
```

`follower_id` → `actor_id`, `followee_id` → `recipient_id`.

---

## Consumer topology

```
bus.subscribe("PostCreated",   fanout_consumer)
bus.subscribe("PostCreated",   realtime_consumer)
bus.subscribe("PostCreated",   on_post_created)      # new
bus.subscribe("FollowCreated", on_follow_created)    # new
```

Both new consumers live in `app/notifications.py`, not `consumers.py` —
timeline fanout and notification creation are different business
capabilities that happen to react to the same event; growing
`consumers.py` into "everything that reacts to a post" would blur that
boundary. `event_bus.py` required **zero changes**: `_create_consumer_groups()`
already iterates `self._handlers`, so the new `FollowCreated` stream and
its consumer group are created automatically — the same "adding a
consumer requires no changes to the bus" guarantee M2 established,
holding under a second real use.

`on_post_created` has no ordering dependency on `fanout_consumer` or
`realtime_consumer` (unlike fanout→realtime's timeline-before-push
guarantee from M0.5) — registered last in `worker.py` purely for
readability.

---

## HTTP API

```
GET  /notifications?cursor=&limit=     → { notifications: [...], next_cursor }
POST /notifications/{id}/read          → mark one as read (recipient-scoped)
POST /notifications/read-all           → mark all as read
GET  /notifications/unread-count       → { count: N }
```

All behind `get_current_user`; `recipient_id` always comes from the JWT
`sub`, never a route param — the discipline M1 established for
`author_id`.

`POST /notifications/{id}/read` returns 404 for both "no such
notification" and "exists but belongs to someone else," identically.
The id is an opaque client handle, not a secret — `mark_read`'s
`WHERE id = $1 AND recipient_id = $2` is the actual security boundary.
Guessing a sequential ID gets a 404, never someone else's data.

---

## Architecture Decision Records

### ADR-1: Notification identity is a domain concept, decoupled from transport identity

**Decision:** identity = `(recipient_id, actor_id, type, object_type,
object_id)`, never a Redis Stream message ID.

**Reason:** transport identity is an artifact of how the *current* bus
implements redelivery — it has no meaning to the notification domain and
would not survive the roadmap's planned M13 Streams→Kafka migration.

**Note:** for M8's two types, this tuple happens to double as occurrence
identity — at most one meaningful occurrence exists per (actor, type,
object) today. That is not a universal claim. A future type like
`LikeCreated` may permit multiple genuine occurrences of the same
actor/type/object (like → unlike → like again); such a type must define
its own occurrence identity rather than assume this constraint
generalizes.

**Revisit When:** a new notification type's real-world semantics don't
fit "at most one occurrence per (actor, object)."

### ADR-2: Durable notification row is authoritative; WebSocket push is a hint

**Decision:** the Postgres row is the sole source of truth for
existence and read-state. Any future live push is a best-effort
optimization layered on top, never load-bearing.

**Reason:** the third occurrence of this project's recurring
architectural pattern — durable source of truth, disposable optimization
on top — already established by M2 (Redis Streams vs. Pub/Sub) and M5
(Postgres vs. Redis post cache). See `architecture-review.md`'s
"Recurring Architectural Patterns" section, added alongside this
milestone.

**Revisit When:** never, by design — keeping this consistent with the
rest of the prototype's architectural language is the point.

### ADR-3: `NEW_FOLLOWER` represents current relationship state, not historical event

**Decision:** an unfollow followed by a refollow does **not** create a
new notification and does **not** reset an existing notification's
`read_at` back to unread.

**Reason:** `follows` is a current-state relationship table (PK on
`(follower_id, followee_id)`), not an event log — this respects the
model that already exists rather than retrofitting fake history onto
it. `ON CONFLICT (recipient_id, actor_id, type, object_type, object_id)
DO NOTHING` is the mechanism, but the decision it encodes is a
**product** one, not merely infrastructure deduplication: the same SQL
clause also absorbs genuine Streams redelivery, and the two purposes are
distinct even though they share an implementation.

**Revisit When:** a feature requiring per-action follow history (e.g.
follow requests, "X followed you 3 times this month") is introduced —
that needs an actual follow-event identity/history model, not a
reinterpretation of this constraint.

### ADR-4: M7's hybrid fanout does not transfer to notification persistence

**Decision:** `on_post_created` writes one row per follower
unconditionally. No light/heavy branch.

**Reason:** M7's technique is safe specifically because fanout-on-write
and fanout-on-read are **output-equivalent** for a reconstructible
collection — a follower cannot tell which happened. A notification's
read/unread state is irreplaceable per-recipient state with no source to
derive it from at read time; deferring the write doesn't defer
computation, it simply never creates the data. The mechanism that made
M7 safe has no analog here.

**Revisit When:** see ADR-5.

### ADR-5: Celebrity-scale `NEW_POST` notification volume is a known, deferred gap

**Decision:** ship the correct, unconditional write for M8. Do not
pre-optimize before the notification product itself has taken shape —
consistent with this project's established discipline of deferring
optimization until a limitation is actually felt (M0.5, M2, M7's tiny
dev-only `HEAVY_FANOUT_THRESHOLD`).

**Candidate future strategies** (none chosen or implemented):
  (a) suppress `NEW_POST` notifications above a follower threshold —
      cheapest, a product decision as much as an engineering one;
  (b) digest/aggregate ("Alice and 12 others posted") — real added
      complexity, not attempted here;
  (c) batched/backpressured async writes — survive the volume rather
      than avoid it.

**Revisit When:** an account's follower count makes per-post
notification write volume observable in monitoring, *or* the product
decides celebrity `NEW_POST` notifications shouldn't be 1:1 rows
regardless of scale.

---

## Cross-cutting design notes

**Domain identity vs. transport/message identity** — see ADR-1. Every
identity-bearing decision in this milestone (notification uniqueness,
idempotency under retry) is anchored to fields the domain already owns
(`post_id`, `follower_id`/`followee_id`), never to anything the event
bus's current implementation happens to assign.

**Durable state vs. best-effort delivery** — see ADR-2. Formalized as a
named cross-milestone principle in `architecture-review.md`.

**Current relationship vs. historical event semantics** — see ADR-3.
`NEW_FOLLOWER` deliberately mirrors the semantics already implied by
`follows`'s schema, rather than inventing history the table was never
designed to hold.

**Why M7's hybrid fanout doesn't apply here** — see ADR-4. The
determining property is output-equivalence between write-time and
read-time delivery; a notification's read state has no such equivalence
to exploit.

**Cursor tie-break diverges from M6, deliberately.** M6's timeline
cursor is `created_at` alone, accepted because same-timestamp collisions
were judged rare for individually-created posts, and a compound cursor
against a Redis sorted set would have needed a Lua script or an extra
round trip. Neither justification holds here: `on_post_created` captures
**one** `created_at` per fan-out batch, so many notifications for the
same recipient can legitimately share an identical timestamp — a likely
scenario, not an edge case — and a composite Postgres predicate
(`WHERE (created_at, id) < ($1, $2)`) costs nothing extra over a
single-column one. M8 therefore uses `(created_at, id)` from the start.
Verified directly in `test_notifications.py` §4, including the case M6's
cursor could not have handled safely: a new notification inserted
between page fetches at the exact same shared timestamp.

**Publish-after-commit gap, inherited not introduced.**
`follow_user`'s new `bus.publish("FollowCreated", ...)` call is not
wrapped in a try/except, deliberately matching `create_post`'s existing
`bus.publish()` call. If Redis is unavailable at that instant, the
follow row commits but the event never publishes, and the request 500s
despite the write having succeeded. This is the same architectural gap
`create_post` already had — M8 does not fix it, and should not: a proper
fix (Outbox pattern) is its own future reliability milestone, and
attempting it here would expand M8's scope well past "introduce the
Notification subsystem."

**Retry semantics are inherited, not new.** `event_bus.py`'s
`_process()` ACKs only after *every* handler on an event succeeds, with
no per-handler acknowledgment. `on_post_created` is a third handler on
`PostCreated`, alongside `fanout_consumer` and `realtime_consumer`. A
crash after all three succeed but before ACK causes full redelivery:
`fanout_consumer` and `on_post_created` tolerate this for free (`ZADD`
and `ON CONFLICT DO NOTHING` are both idempotent); `realtime_consumer`
does not, and can duplicate a WebSocket toast. This exposure already
existed with two handlers — M8 adds a third instance of the same known
property, not a new kind of risk.

---

## What was built

### New files

```text
app/notifications.py    — Notification subsystem: consumers + domain functions
test_notifications.py   — direct verification against real db.py/notifications.py
docs/milestone-8-notification-store.md
```

### Modified files

```text
app/db.py       — notifications table + indexes in SCHEMA; create_notification,
                  get_notifications, mark_notification_read,
                  mark_all_notifications_read, count_unread_notifications
app/app.py      — FollowCreated publish in follow_user; four new
                  /notifications routes
worker.py       — subscribes on_post_created (PostCreated) and
                  on_follow_created (FollowCreated)
architecture-review.md — new "Recurring Architectural Patterns" section
                  naming the durable-state/best-effort-optimization
                  principle across M2, M5, and M8
```

### Unchanged

`consumers.py`, `event_bus.py`, `cache.py`, `ws_manager.py`, `ws_router.py`,
all frontend files — confirms the goal that `fanout_consumer` and
`realtime_consumer` never needed to change, and that `event_bus.py`'s
generic handler-registration design absorbed a brand-new event type
(`FollowCreated`) with zero modification.

---

## Verification

`test_notifications.py`, run directly against `db.py`/`notifications.py`
(no HTTP layer, no live event bus):

- **§1 — NEW_POST fan-out:** every follower receives a notification;
  the author does not receive one for their own post.
- **§2 — Idempotency under simulated redelivery:** re-invoking
  `on_post_created` for the identical event with a *different*
  `created_at` (simulating `XAUTOCLAIM` retry after a crash) does not
  duplicate the notification — confirms identity is genuinely domain-
  derived, not time-based.
- **§3 — `NEW_FOLLOWER` + refollow (ADR-3):** a follow produces exactly
  one notification with a self-referential object; marking it read
  persists; an unfollow-then-refollow resolves to the *same* row,
  creates no duplicate, and does not reset `read_at`.
- **§4 — Cursor pagination under a shared-timestamp batch:** 12
  notifications seeded with an identical `created_at` page exactly
  once, in stable order, across a multi-page scroll; a notification
  inserted mid-scroll at the *same* shared timestamp does not leak into
  the in-progress session but does surface on a fresh top-of-feed fetch
  — the exact scenario M6's single-column cursor was never exercised
  against.
- **§5 — Ownership:** `mark_read` refuses a notification belonging to a
  different recipient.

---

## Known limitations

- **`realtime_consumer`'s O(followers) push loop is unaffected by this
  milestone**, as anticipated in M7.5's own Section-9 note. M8
  establishes that a persistent notification *does* exist independent of
  live delivery — offline users no longer lose the notification, only
  the instantaneous push — but the loop's write-volume shape is
  untouched and was never M8's target.
- **Celebrity-scale `NEW_POST` notification write volume** — see ADR-5.
  Deliberately deferred; ship correct behavior first, choose an
  optimization once real usage informs which of suppression/digest/batch
  fits the product.
- **`FollowCreated` publish-after-commit gap** — see cross-cutting notes
  above. Same shape as `create_post`'s existing gap; deferred to a
  future Outbox-pattern milestone.
- **No delivery tracking, archival, or actionable-notification lifecycle
  stages** — M8 implements exactly the minimal lifecycle needed (create
  → unread → read). Explicitly not modeled, and not needed yet.

---

## Next milestone

Two independent threads are now unblocked, neither forced by this one:

1. **Celebrity `NEW_POST` notification strategy** (ADR-5) — once real
   usage patterns exist to inform the suppression/digest/batch choice.
2. **`realtime_consumer` O(followers) push-loop redesign** — now
   informed by M8's actual shape: a live push is confirmed to be a
   best-effort hint layered on a durable store, not the sole delivery
   mechanism, which changes what "fixing" that loop should even optimize
   for.

Per the discipline established throughout this project, neither is
pursued speculatively — both wait for a concrete, felt trigger.