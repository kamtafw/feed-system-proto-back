"""
consumers.py — Fanout and Realtime consumers.

Logic is identical to the prototype. What changed:
    - get_followers()          → db.get_followers()      (was FOLLOWERS dict)
    - TIMELINES[...].insert()  → cache.push_to_timeline() (was list prepend)
    - created_at score         → from event payload (not re-fetched from store)

Note: consumers now receive `created_at` in the payload (added in main.py's
create_post handler) so the sorted set score is accurate without a DB round-trip.
"""

import time
from app import db
from app import cache
from app.ws_manager import manager, system


async def fanout_consumer(payload: dict) -> None:
    # raise Exception("deliberate failure — testing XAUTOCLAIM")  # ← TEMP

    post_id     = payload["post_id"]
    author_id   = payload["author_id"]
    author_name = payload["author_name"]
    created_at  = payload["created_at"]   # used as sorted set score

    followers = await db.get_followers(author_id)

    print(f"\n📢  [Fanout] {post_id!r} → {len(followers)} timelines")

    await system.broadcast({
        "event":     "FANOUT_START",
        "post_id":   post_id,
        "author":    author_name,
        "followers": followers,
        "ts":        time.time(),
    })

    for follower_id in followers:
        await cache.push_to_timeline(follower_id, post_id, created_at)
        print(f"    ✅  {follower_id} ← #{post_id}")
        await system.broadcast({
            "event":   "FANOUT_WRITE",
            "target":  follower_id,
            "post_id": post_id,
            "ts":      time.time(),
        })

    # author sees their own post
    await cache.push_to_timeline(author_id, post_id, created_at)


async def realtime_consumer(payload: dict) -> None:
    post_id     = payload["post_id"]
    author_id   = payload["author_id"]
    author_name = payload["author_name"]

    followers = await db.get_followers(author_id)
    online    = [f for f in followers if manager.is_online(f)]
    offline   = [f for f in followers if not manager.is_online(f)]

    print(f"\n⚡  [Realtime] online={online}  offline={offline}")

    await system.broadcast({
        "event":   "REALTIME_START",
        "author":  author_name,
        "online":  online,
        "offline": offline,
        "ts":      time.time(),
    })

    for follower_id in online:
        await manager.send(follower_id, {
            "type":        "NEW_POST",
            "post_id":     post_id,
            "author_id":   author_id,
            "author_name": author_name,
        })
        await system.broadcast({
            "event":  "REALTIME_SEND",
            "target": follower_id,
            "ts":     time.time(),
        })

    for follower_id in offline:
        await system.broadcast({
            "event":  "REALTIME_SKIP",
            "target": follower_id,
            "reason": "offline",
            "ts":     time.time(),
        })
