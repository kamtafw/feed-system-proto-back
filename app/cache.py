"""
cache.py — Redis layer for timelines.

Replaces the TIMELINES dict in store.py.

Data structure: Redis Sorted Set per user
    key   = "timeline:{user_id}"
    member = post_id
    score  = created_at (Unix timestamp as float)

Why sorted sets instead of a list?
    - zadd is O(log n) — faster than list.insert(0) which is O(n)
    - zrevrange gives newest-first without sorting on read
    - zremrangebyrank caps memory at TIMELINE_MAX entries automatically
    - Pagination via offset is built-in (no full scan)
"""

import redis.asyncio as aioredis
from typing import List, Optional
from app.config import TIMELINE_MAX

_client: Optional[aioredis.Redis] = None


# Lifecycle

async def init_cache(url: str) -> None:
    global _client
    _client = aioredis.from_url(url, decode_responses=True)
    await _client.ping()  # fail fast if Redis isn't reachable
    print("✅  Redis ready")


async def close_cache() -> None:
    if _client:
        await _client.aclose()


# Timeline operations

async def push_to_timeline(user_id: str, post_id: str, score: float) -> None:
    """
    Add post_id to the user's timeline sorted set and trim to TIMELINE_MAX.

    Pipeline wraps both ops in a single round-trip — zadd + zremrangebyrank
    are sent together, not sequentially. At 800-1000 users this matters when
    fanout fires for popular accounts with many followers simultaneously.
    """
    key = f"timeline:{user_id}"
    async with _client.pipeline(transaction=True) as pipe:
        pipe.zadd(key, {post_id: score})
        # zremrangebyrank(key, 0, -(N+1)) removes all but the N highest scores
        pipe.zremrangebyrank(key, 0, -(TIMELINE_MAX + 1))
        await pipe.execute()


async def get_timeline_ids(
    user_id: str,
    limit: int = 50,
    offset: int = 0,
) -> List[str]:
    """
    Return post IDs, newest first.
    offset + limit enables simple cursor-based pagination:
        page 1 → offset=0,  limit=50
        page 2 → offset=50, limit=50
    """
    key = f"timeline:{user_id}"
    return await _client.zrevrange(key, offset, offset + limit - 1)


async def remove_from_timeline(user_id: str, post_id: str) -> None:
    """For delete/moderation flows added later."""
    await _client.zrem(f"timeline:{user_id}", post_id)
