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

Milestone 6: pagination moved from offset-based (ZREVRANGE with a
positional start/stop) to cursor-based (ZREVRANGEBYSCORE with a value
boundary). See get_timeline_ids() docstring and
docs/milestone-6-cursor-pagination.md for why.
"""

import json
import redis.asyncio as aioredis
from typing import List, Optional
from app.config import TIMELINE_MAX, POST_CACHE_TTL_SECONDS

_client: Optional[aioredis.Redis] = None


# Lifecycle


async def init_cache(url: str) -> None:
    global _client
    _client = aioredis.from_url(url, decode_responses=True)
    await _client.ping()  # fail fast if Redis isn't reachable
    print("✅  Redis ready")


async def close_cache() -> None:
    if _client:
        await _client.close()


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


async def get_timeline_ids(user_id: str, cursor: Optional[float] = None, limit: int = 50) -> List[str]:
    """
    Return post IDs, newest first, using CURSOR-based pagination (Milestone 6).

    cursor = created_at of the last post the CLIENT has already seen —
    a value, not a position. We query with an EXCLUSIVE upper bound
    ("(cursor" in Redis's range syntax) so that post is never re-returned.

    This is the entire fix for offset drift: a new post inserted above the
    cursor changes nothing about "everything scored below X" — unlike an
    offset, which is a position that shifts every time something is
    inserted ahead of it.

    cursor=None → first page, starts from "+inf" (now).
    """
    key = f"timeline:{user_id}"
    max_score = f"({cursor}" if cursor is not None else "+inf"
    return await _client.zrevrangebyscore(key, max_score, "-inf", start=0, num=limit)


async def remove_from_timeline(user_id: str, post_id: str) -> None:
    """For delete/moderation flows added later."""
    await _client.zrem(f"timeline:{user_id}", post_id)


# Post cache (Milestone 5)
#
# Posts are cached as JSON strings (post:{id}), not Redis hashes — see the
# ADR in architecture-review.md. Short version: timeline reads are bulk
# fetches by post ID list, which MGET serves in one round trip; posts
# are immutable right now, so there's no field to update atomically.
# Revisit if posts become editable/deletable, or gain independently
# mutating fields (reaction counts, view counts, etc).


def _post_key(post_id: str) -> str:
    return f"post:{post_id}"


async def get_posts(post_ids: List[str]) -> dict:
    """Bulk cache lookup. Returns {post_id: post_dict} for HITS only —
    callers detect misses via `set(post_ids) - result.keys()`"""
    if not post_ids:
        return {}
    raw_values = await _client.mget([_post_key(pid) for pid in post_ids])
    return {pid: json.loads(raw) for pid, raw in zip(post_ids, raw_values) if raw is not None}


async def set_post(post: dict, ttl: int = POST_CACHE_TTL_SECONDS) -> None:
    """Cache a single post. Called on write to warm the cache before fanout runs."""
    await _client.set(_post_key(post["id"]), json.dumps(post), ex=ttl)


async def set_posts(posts: List[dict], ttl: int = POST_CACHE_TTL_SECONDS) -> None:
    """Bulk backfill after a read-path miss. Pipelined — one round trip."""
    if not posts:
        return
    async with _client.pipeline(transaction=False) as pipe:
        for post in posts:
            pipe.set(_post_key(post["id"]), json.dumps(post), ex=ttl)
        await pipe.execute()
