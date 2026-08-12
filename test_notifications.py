"""
test_notifications.py — Verify the Milestone 8 Notification subsystem
directly against real Postgres, without going through the HTTP layer or
the event bus. Exercises the actual on_post_created / on_follow_created
consumers and the actual db.py functions worker.py and app.py call.

Run against a live Postgres:

    uv run test_notifications.py

What it proves, section by section:
  1.  NEW_POST fan-out: every follower gets a notification, the author
      never gets one for their own post.
  2.  Idempotency under simulated Streams redelivery: re-running
      on_post_created() for the identical event does NOT duplicate rows,
      even though the redelivered call captures a DIFFERENT created_at —
      proving identity is genuinely (recipient, actor, type, object_type,
      object_id), not time-based.
  3.  NEW_FOLLOWER + refollow semantics (M8 ADR-3): a follow produces one
      notification; marking it read persists; an unfollow-then-refollow
      does NOT create a second notification and does NOT reset read_at
      back to unread.
  4.  Cursor pagination under a same-timestamp batch: many notifications
      sharing one created_at (the realistic on_post_created batch shape)
      still page exactly-once, in a stable order, with no drift when a
      new notification is inserted mid-scroll at the SAME timestamp —
      this is exactly the scenario M6's score-only cursor could not have
      handled safely, and why M8 uses (created_at, id) instead.
  5.  Ownership: mark_read() refuses to touch a notification that
      belongs to someone else.
"""

import asyncio
import time
import uuid

from app import db
from app.config import DATABASE_URL
from app.notifications import (
    NEW_FOLLOWER,
    NEW_POST,
    get_unread_count,
    list_notifications,
    mark_read,
    on_follow_created,
    on_post_created,
)

SEP = "—" * 56


def uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def section(n: int, title: str) -> None:
    print(f"\n[{n}] {title}")


async def make_user(prefix: str) -> str:
    user_id = uid(prefix)
    await db.create_user(user_id, user_id, password_hash="x")
    return user_id


async def fetch_page(recipient_id, cursor, limit=5):
    return await list_notifications(recipient_id, cursor=cursor, limit=limit)


async def main() -> None:
    await db.init_db(DATABASE_URL)

    all_test_users = []
    print(SEP)
    print(" FanoutFeed — Notification subsystem verification (Milestone 8)")
    print(SEP)

    # [1] NEW_POST fan-out, no self-notification
    await section(1, "NEW_POST fan-out")
    author = await make_user("author")
    follower_a = await make_user("follower-a")
    follower_b = await make_user("follower-b")
    all_test_users += [author, follower_a, follower_b]

    await db.add_follow(follower_a, author)
    await db.add_follow(follower_b, author)

    post_id = uid("post")
    payload = {"post_id": post_id, "author_id": author, "author_name": author, "created_at": time.time()}
    await on_post_created(payload)

    for follower_id in (follower_a, follower_b):
        items, _ = await fetch_page(follower_id, cursor=None)
        matching = [n for n in items if n["type"] == NEW_POST and n["object_id"] == post_id]
        assert matching, f"{follower_id} did not receive a NEW_POST notification for {post_id}"
        assert matching[0]["actor_id"] == author, "actor_id should be the post's author"

    author_items, _ = await fetch_page(author, cursor=None)
    assert not any(n["object_id"] == post_id for n in author_items), "Author should not be notified of their own post"
    print(f"    ✅  Both followers notified of {post_id!r}; author received no self-notification")

    # [2] Idempotency under simulated redelivery
    await section(2, "Idempotency under simulated Streams redelivery")
    # Re-run with the SAME post_id/author_id but a genuinely different
    # created_at, simulating XAUTOCLAIM re-delivering the event after a
    # crash. If identity were time-based this would produce a duplicate.
    redelivered_payload = {**payload, "created_at": time.time() + 5}
    await on_post_created(redelivered_payload)

    items, _ = await fetch_page(follower_a, cursor=None, limit=50)
    matches = [n for n in items if n["object_id"] == post_id]
    assert len(matches) == 1, f"Expected exactly 1 notification for {post_id}, got {len(matches)} — redelivery duplicated a row"
    print(f"    ✅  Redelivered event did not duplicate the notification (identity is not time-based)")

    # [3] NEW_FOLLOWER + refollow semantics (ADR-3)
    await section(3, "NEW_FOLLOWER + refollow semantics (M8 ADR-3)")
    nf_recipient = await make_user("nf-recipient")
    nf_follower = await make_user("nf-follower")
    all_test_users += [nf_recipient, nf_follower]

    await db.add_follow(nf_follower, nf_recipient)
    await on_follow_created({"follower_id": nf_follower, "followee_id": nf_recipient, "created_at": time.time()})

    items, _ = await fetch_page(nf_recipient, cursor=None)
    nf_matches = [n for n in items if n["type"] == NEW_FOLLOWER and n["actor_id"] == nf_follower]
    assert len(nf_matches) == 1, "Expected exactly one NEW_FOLLOWER notification"
    notif = nf_matches[0]
    assert notif["object_type"] == "user" and notif["object_id"] == nf_recipient, "object should be self-referential (object_id == recipient_id)"
    assert notif["read_at"] is None, "New notification should start unread"
    print(f"    ✅  Follow produced exactly one notification, self-referential object, unread")

    ok = await mark_read(notif["id"], nf_recipient)
    assert ok, "mark_read should succeed for the actual owner"
    items, _ = await fetch_page(nf_recipient, cursor=None)
    refetched = next(n for n in items if n["id"] == notif["id"])
    assert refetched["read_at"] is not None, "Notification should now be read"
    print(f"    ✅  Notification marked read")

    # Unfollow, then refollow — Option A: no new notification, no reset to unread
    await db.remove_follow(nf_follower, nf_recipient)
    await db.add_follow(nf_follower, nf_recipient)
    await on_follow_created({"follower_id": nf_follower, "followee_id": nf_recipient, "created_at": time.time()})

    items, _ = await fetch_page(nf_recipient, cursor=None)
    nf_matches_after = [n for n in items if n["type"] == NEW_FOLLOWER and n["actor_id"] == nf_follower]
    assert len(nf_matches_after) == 1, f"Refollow should NOT create a second notification, got {len(nf_matches_after)}"
    assert nf_matches_after[0]["id"] == notif["id"], "Refollow should resolve to the SAME notification row"
    assert nf_matches_after[0]["read_at"] is not None, "Refollow must NOT reset read_at back to unread (ADR-3)"
    print(f"    ✅  Refollow created no new notification and did not reset read_at — ADR-3 confirmed")

    # [4] Cursor pagination under a same-timestamp batch
    await section(4, "Cursor pagination — many notifications sharing one created_at")
    cp_recipient = await make_user("cursor-recipient")
    # actor_id has a real FK to users(id) (same as recipient_id) — this
    # section is exercising the (created_at, id) tie-break via object_id,
    # not actor identity, so one real actor, reused across every seeded
    # row, is sufficient and keeps setup simple.
    cp_actor = await make_user("cursor-actor")
    all_test_users += [cp_recipient, cp_actor]

    shared_ts = time.time()
    seeded_ids = []
    for i in range(12):
        row = await db.create_notification(
            recipient_id=cp_recipient,
            actor_id=cp_actor,
            notif_type=NEW_POST,
            object_type="post",
            object_id=f"orig-{i}",
            created_at=shared_ts,  # identical timestamp for every row — the realistic batch shape
        )
        seeded_ids.append(row["id"])

    print(f"    Seeded 12 notifications, all sharing created_at={shared_ts}")

    collected, injected = [], []
    cursor, page_num = None, 0
    while True:
        page_num += 1
        page, cursor = await fetch_page(cp_recipient, cursor, limit=5)
        page_ids = [n["object_id"] for n in page]
        print(f"    [Page {page_num}] {page_ids}  (next_cursor={cursor})")
        collected.extend(page_ids)

        if cursor is None:
            break

        # simulate a concurrent notification landing at the SAME shared
        # timestamp between page fetches — the exact case M6's score-only
        # cursor could not have handled safely.
        new_object_id = f"new-{page_num}"
        await db.create_notification(
            recipient_id=cp_recipient,
            actor_id=cp_actor,
            notif_type=NEW_POST,
            object_type="post",
            object_id=new_object_id,
            created_at=shared_ts,
        )
        injected.append(new_object_id)
        print(f"      ↳ inserted {new_object_id!r} at the SAME shared timestamp between fetches")

    expected = [f"orig-{i}" for i in range(11, -1, -1)]  # id DESC within the tie == newest-inserted-first
    assert collected == expected, f"Order/completeness mismatch.\n  expected: {expected}\n  got:      {collected}"
    print(f"    ✅  All 12 original notifications returned exactly once, correctly ordered despite identical timestamps")

    leaked = [oid for oid in injected if oid in collected]
    assert not leaked, f"Injected notifications leaked into the in-progress scroll: {leaked}"
    print(f"    ✅  None of the {len(injected)} concurrently-inserted notifications leaked into the scroll session")

    fresh_page, _ = await fetch_page(cp_recipient, None, limit=50)
    fresh_ids = [n["object_id"] for n in fresh_page]
    assert all(oid in fresh_ids for oid in injected), "Injected notifications should surface on a fresh top-of-feed fetch"
    print(f"    ✅  A fresh fetch (cursor=None) correctly surfaces the injected notifications")

    # [5] Ownership
    await section(5, "Ownership — mark_read refuses cross-recipient access")
    stranger = await make_user("stranger")
    all_test_users.append(stranger)
    denied = await mark_read(notif["id"], stranger)
    assert denied is False, "A user should never be able to mark someone else's notification as read"
    print(f"    ✅  mark_read correctly refused for a non-owning recipient")

    unread = await get_unread_count(follower_a)
    print(f"    (sanity) unread count for {follower_a}: {unread}")

    print(f"\n{SEP}\n Notification subsystem verified — identity, idempotency, refollow")
    print(f" semantics, cursor tie-break, and ownership all confirmed.\n{SEP}")

    # Cleanup — FK ON DELETE CASCADE on recipient_id/actor_id removes every
    # notification created above automatically.
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE id = ANY($1::text[])", all_test_users)

    await db.close_db()


if __name__ == "__main__":
    asyncio.run(main())
