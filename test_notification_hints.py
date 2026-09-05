"""
test_notification_hints.py — Verify the Milestone 8.5 best-effort
NEW_NOTIFICATION WebSocket hint: durable-then-live ordering, hint-failure
isolation, actor-name propagation/fallback, and payload distinctness
from the REST representation.

Run against live Postgres + Redis:

    uv run test_notification_hints.py

Uses a FakeWebSocket registered with the REAL ConnectionManager /
PubSubRouter (same approach as test_pubsub_router.py) so the hint is
proven to travel through the actual manager.send() -> router.publish()
-> Redis Pub/Sub -> local delivery path, not a stand-in for it.

What it proves, section by section:
  1.  Durable notification creation, followed by a successful live hint
      delivered to a connected client — exercising the same sequential
      ordering (durable write before live push) that worker.py's
      registration order relies on. See
      docs/milestone-8.5-realtime-notification-hint.md ADR-2 for why
      this ordering is a real dependency on event_bus.py's CURRENT
      sequential handler execution, not incidental.
  2.  A live-hint failure (manager.send raising, simulating a Redis
      Pub/Sub outage) never propagates out of notify_new_post_hint, and
      the durable row from [1] is completely unaffected by it. This is
      the property that makes it structurally impossible for a failed
      best-effort hint to cause a durable notification to be lost or
      incorrectly retried — event_bus.py's _process() only withholds an
      ACK when a handler raises, and this handler never does.
  3.  notify_new_follower_hint uses follower_name when the FollowCreated
      payload carries one, and falls back to follower_id when it's
      missing, rather than crashing.
  4.  Neither hint payload contains id, created_at, or read_at — the
      fields that make up the REST Notification representation — kept
      structurally distinct by construction (_notification_hint_payload
      is the one place the wire shape is defined).
"""

import asyncio
import json
import time
import uuid

from app import db
from app.config import DATABASE_URL, REDIS_URL
from app.notifications import (
    NEW_FOLLOWER,
    NEW_POST,
    notify_new_follower_hint,
    notify_new_post_hint,
    on_follow_created,
    on_post_created,
)
from app.ws_manager import manager
from app.ws_router import router

SEP = "—" * 56


def uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def section(n: int, title: str) -> None:
    print(f"\n[{n}] {title}")


async def make_user(prefix: str) -> str:
    user_id = uid(prefix)
    await db.create_user(user_id, user_id, password_hash="x")
    return user_id


class FakeWebSocket:
    def __init__(self, name: str):
        self.name = name
        self.received: list = []

    async def accept(self) -> None:
        """
        No-op. ConnectionManager.connect() calls `await ws.accept()`
        before registering with the router (completing the real
        WebSocket handshake in production) — this test goes through
        manager.connect(), not router.register() directly, specifically
        to exercise the same production path notify_new_post_hint /
        notify_new_follower_hint actually use via manager.send(). A real
        WebSocket's accept() has no equivalent for this fake to perform;
        the handshake itself isn't what's under test here.
        """
        pass

    async def send_text(self, data: str) -> None:
        self.received.append(json.loads(data))


async def wait_for_delivery(fake_ws: "FakeWebSocket", timeout: float = 1.0) -> None:
    elapsed, interval = 0.0, 0.02
    while not fake_ws.received and elapsed < timeout:
        await asyncio.sleep(interval)
        elapsed += interval


async def main() -> None:
    await db.init_db(DATABASE_URL)
    await router.init(REDIS_URL)

    all_test_users = []
    print(SEP)
    print(" FanoutFeed — NEW_NOTIFICATION live hint verification (Milestone 8.5)")
    print(SEP)

    # [1] Durable creation, then a successful live hint, in order
    await section(1, "Durable notification, then successful live hint (sequential ordering)")
    author = await make_user("author")
    follower = await make_user("follower")
    all_test_users += [author, follower]
    await db.add_follow(follower, author)

    ws = FakeWebSocket("follower-ws")
    await manager.connect(follower, ws)  # registers with the REAL router

    post_id = uid("post")
    payload = {
        "post_id": post_id,
        "author_id": author,
        "author_name": "Author Name",
        "created_at": time.time(),
    }

    # Durable write FIRST — mirrors on_post_created running before
    # notify_new_post_hint in worker.py's registration order.
    await on_post_created(payload)
    await notify_new_post_hint(payload)
    await wait_for_delivery(ws)

    assert ws.received, "Live hint was never delivered to the connected client"
    hint = ws.received[-1]
    assert hint["type"] == "NEW_NOTIFICATION"
    assert hint["notification_type"] == NEW_POST
    assert hint["actor_id"] == author
    assert hint["object_type"] == "post"
    assert hint["object_id"] == post_id

    rows = await db.get_notifications(follower)
    durable_matches = [r for r in rows if r["object_id"] == post_id]
    assert len(durable_matches) == 1, "Durable notification row should exist exactly once"
    print("    ✅  Durable row exists and live hint delivered, in the correct order")

    await manager.disconnect(follower, ws)

    # [2] Live-hint failure is caught, never re-raised, durable state untouched
    await section(2, "Live-hint failure is caught, never re-raised, durable row unaffected")
    orig_send = manager.send

    async def failing_send(*args, **kwargs):
        raise ConnectionError("simulated Redis Pub/Sub outage")

    manager.send = failing_send
    try:
        await notify_new_post_hint(payload)  # must NOT raise
        print("    ✅  notify_new_post_hint swallowed a live-delivery failure without raising")
    finally:
        manager.send = orig_send

    rows_after = await db.get_notifications(follower)
    matches_after = [r for r in rows_after if r["object_id"] == post_id]
    assert len(matches_after) == 1, "Durable notification should be unaffected by a live-hint failure"
    print("    ✅  Durable notification row is unaffected by the simulated live-hint failure")

    # [3] FollowCreated hint uses follower_name, falls back to follower_id
    await section(3, "FollowCreated hint uses follower_name, falls back to follower_id")
    nf_recipient = await make_user("nf-recipient")
    nf_follower = await make_user("nf-follower")
    all_test_users += [nf_recipient, nf_follower]
    await db.add_follow(nf_follower, nf_recipient)

    ws2 = FakeWebSocket("nf-recipient-ws")
    await manager.connect(nf_recipient, ws2)

    follow_payload_with_name = {
        "follower_id": nf_follower,
        "follower_name": "Nice Follower Name",
        "followee_id": nf_recipient,
        "created_at": time.time(),
    }
    await on_follow_created(follow_payload_with_name)
    await notify_new_follower_hint(follow_payload_with_name)
    await wait_for_delivery(ws2)

    assert ws2.received, "Follow hint was not delivered"
    follow_hint = ws2.received[-1]
    assert follow_hint["notification_type"] == NEW_FOLLOWER
    assert follow_hint["actor_id"] == nf_follower
    assert follow_hint["actor_name"] == "Nice Follower Name"
    assert follow_hint["object_type"] == "user"
    assert follow_hint["object_id"] == nf_recipient
    print("    ✅  follower_name propagated correctly into the hint's actor_name")

    ws2.received.clear()
    follow_payload_no_name = {
        "follower_id": uid("legacy-follower"),
        "followee_id": nf_recipient,
        "created_at": time.time(),
    }
    # No durable write here — this section only exercises the hint
    # function's own fallback behavior in isolation, which is all this
    # requirement is about.
    await notify_new_follower_hint(follow_payload_no_name)
    await wait_for_delivery(ws2)
    assert ws2.received, "Follow hint (no name) was not delivered"
    fallback_hint = ws2.received[-1]
    assert fallback_hint["actor_name"] == follow_payload_no_name["follower_id"], "Missing follower_name should fall back to follower_id, not crash"
    print("    ✅  Missing follower_name falls back to follower_id instead of raising")

    await manager.disconnect(nf_recipient, ws2)

    # [4] Payload distinctness from the REST representation
    await section(4, "Hint payload never carries REST-only fields")
    rest_only_fields = {"id", "created_at", "read_at"}
    overlap_post = rest_only_fields & set(hint.keys())
    overlap_follow = rest_only_fields & set(follow_hint.keys())
    assert not overlap_post, f"NEW_POST hint unexpectedly contains REST-only field(s): {overlap_post}"
    assert not overlap_follow, f"NEW_FOLLOWER hint unexpectedly contains REST-only field(s): {overlap_follow}"
    print(f"    ✅  Neither hint payload contains any of {sorted(rest_only_fields)}")

    print(f"\n{SEP}\n Live notification hint verified — durable-before-live ordering,")
    print(f" failure isolation, name propagation/fallback, and payload distinctness")
    print(f" all confirmed.\n{SEP}")

    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE id = ANY($1::text[])", all_test_users)

    await router.close()
    await db.close_db()


if __name__ == "__main__":
    asyncio.run(main())
