"""
consumers.py — Fanout and Realtime consumers.

Milestone 7: fanout_consumer now branches on follower count.

    len(followers) <= HEAVY_FANOUT_THRESHOLD  → LIGHT path (unchanged):
        loop over followers, push_to_timeline() for each.

    len(followers) > HEAVY_FANOUT_THRESHOLD   → HEAVY path (new):
        skip the per-follower loop. One write instead:
        push_to_authored(author_id, post_id, created_at). Followers pick
        this up via the read-time merge in app.py's get_timeline().

len(followers) is read off the SAME db.get_followers() call this
function already made — no separate COUNT query, no cached counter (see
ADR-1). The branch is recomputed fresh on every single post; there is no
"promote/demote" step. This is what makes the hybrid behavior trivially
observable in dev: follow/unfollow across HEAVY_FANOUT_THRESHOLD, post
again, and the very next fanout_consumer run takes the other branch.

realtime_consumer is UNCHANGED — it has the identical O(followers) shape
as the old fanout loop, but fixing it is explicitly out of scope for this
milestone (see the milestone doc's Follow-up section).
"""

import time

from app import db
from app import cache
from app.config import HEAVY_FANOUT_THRESHOLD
from app.ws_manager import manager, system


async def fanout_consumer(payload: dict) -> None:
    post_id = payload["post_id"]
    author_id = payload["author_id"]
    author_name = payload["author_name"]
    created_at = payload["created_at"]
    

    followers = await db.get_followers(author_id)
    is_heavy = len(followers) > HEAVY_FANOUT_THRESHOLD

    print(f"[INFO] - {len(followers)} follower; HEAVY THRESHOLD: {HEAVY_FANOUT_THRESHOLD}")

    if is_heavy:
        print(f"\n👑  [Fanout/HEAVY] {post_id!r} — {len(followers)} followers, writing authored:{author_id} only")
        await cache.push_to_authored(author_id, post_id, created_at)
        await system.broadcast(
            {
                "event": "FANOUT_HEAVY",
                "post_id": post_id,
                "author": author_name,
                "follower_count": len(followers),
                "ts": time.time(),
            }
        )
    else:
        print(f"\n📢  [Fanout/LIGHT] {post_id!r} → {len(followers)} timelines")
        await system.broadcast(
            {
                "event": "FANOUT_START",
                "post_id": post_id,
                "author": author_name,
                "followers": followers,
                "ts": time.time(),
            }
        )
        for follower_id in followers:
            await cache.push_to_timeline(follower_id, post_id, created_at)
            print(f"    ✅  {follower_id} ← #{post_id}")
            await system.broadcast(
                {
                    "event": "FANOUT_WRITE",
                    "target": follower_id,
                    "post_id": post_id,
                    "ts": time.time(),
                }
            )

    # author always sees their own post in their own feed, regardless of
    # which branch was taken — unchanged from every prior milestone.
    await cache.push_to_timeline(author_id, post_id, created_at)


async def realtime_consumer(payload: dict) -> None:
    """Unchanged from Milestone 3/4 — see module docstring."""
    post_id = payload["post_id"]
    author_id = payload["author_id"]
    author_name = payload["author_name"]

    followers = await db.get_followers(author_id)

    # Local-only view — for debug context, not for gating delivery.
    # In multi-worker setups some followers may be online on another worker
    # and will show as "offline" here even though they'll receive the push.
    locally_online = [f for f in followers if manager.is_online(f)]
    locally_offline = [f for f in followers if not manager.is_online(f)]

    print(
        f"\n⚡  [Realtime] publishing to {len(followers)} channel(s) "
        f"(locally online: {locally_online}, "
        f"locally offline/other worker: {locally_offline})"
    )

    await system.broadcast(
        {
            "event": "REALTIME_START",
            "author": author_name,
            "online": locally_online,
            "offline": locally_offline,
            "ts": time.time(),
        }
    )

    # Publish to every follower regardless of local online status.
    # manager.send() → redis.publish("ws:notify:{follower_id}", data)
    # The subscribing worker forwards it to the right WebSocket.
    for follower_id in followers:
        await manager.send(
            follower_id,
            {
                "type": "NEW_POST",
                "post_id": post_id,
                "author_id": author_id,
                "author_name": author_name,
            },
        )
        print(f"    📡  Published → ws:notify:{follower_id}")
        await system.broadcast(
            {
                "event": "REALTIME_SEND",
                "target": follower_id,
                "ts": time.time(),
            }
        )
