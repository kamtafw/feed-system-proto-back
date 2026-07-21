"""
consumers.py — Fanout and Realtime consumers.

Milestone 3 change: realtime_consumer now publishes to ALL followers via
Redis Pub/Sub, regardless of whether they appear online on this worker.

Before (single-process only):
    realtime_consumer checked manager.is_online(follower_id) and only called
    manager.send() for locally-connected users. Offline users were skipped.

After (multi-worker ready):
    realtime_consumer publishes to ws:notify:{follower_id} for every follower.
    The worker holding each user's WebSocket receives the message via its
    _listen() task and forwards it. Users with no active connection on any
    worker simply have no subscriber on their channel — Redis discards the
    publish silently. The result is identical to the old "skip" behaviour
    but without requiring the consumer to know which worker holds what.

    is_online() is kept for the debug panel (REALTIME_START event) to show
    which users are connected to this specific worker. It must NOT be used
    to gate notification delivery.

fanout_consumer is unchanged.
"""

import time

from app import db
from app import cache
from app.ws_manager import manager, system


async def fanout_consumer(payload: dict) -> None:
    """
    Fanout-on-Write: write the post ID into every follower's timeline.
    Unchanged from Milestone 2.
    """
    post_id = payload["post_id"]
    author_id = payload["author_id"]
    author_name = payload["author_name"]
    created_at = payload["created_at"]  # used as sorted set score

    followers = await db.get_followers(author_id)

    print(f"\n📢  [Fanout] {post_id!r} → {len(followers)} timelines")

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

    # author sees their own post
    await cache.push_to_timeline(author_id, post_id, created_at)


async def realtime_consumer(payload: dict) -> None:
    """
    Realtime push: publish a NEW_POST notification to every follower's
    Redis channel. Delivery is handled by whichever worker holds their
    WebSocket connection.
    """
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
