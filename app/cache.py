"""
cache.py — Redis layer for timelines.

Replaces the TIMELINES dict in store.py.

Data structure: Redis Sorted Set per user
    key   = "timeline:{user_id}"
    member = post_id
    score  = created_at (Unix timestamp as float)

Milestone 6: pagination is cursor-based (ZREVRANGEBYSCORE, exclusive
value boundary) rather than offset-based.

Milestone 7: hybrid fanout. Heavy accounts (see consumers.py) skip
per-follower timeline writes entirely and write ONCE to a dedicated
`authored:{author_id}` sorted set instead. That set is NOT the same key
as `timeline:{author_id}` — the personal timeline is "this user's feed"
(their own posts + everyone THEY follow); `authored:` is specifically
"posts BY this user," which is what a heavy account's followers need to
merge in at read time. Conflating the two would leak the celebrity's own
following list into every follower's merged feed.
"""

import json
import redis.asyncio as aioredis
from typing import Dict, List, Optional, Tuple
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
    Pipeline wraps both ops in a single round-trip.
    """
    key = f"timeline:{user_id}"
    async with _client.pipeline(transaction=True) as pipe:
        pipe.zadd(key, {post_id: score})
        pipe.zremrangebyrank(key, 0, -(TIMELINE_MAX + 1))
        await pipe.execute()


async def get_timeline_ids(user_id: str, cursor: Optional[float] = None, limit: int = 50) -> List[str]:
    """
    Plain ID variant — unchanged from Milestone 6. Kept stable rather
    than folded into the scored variant below, since test_cursor_pagination.py
    calls this directly and its contract shouldn't shift underneath it.
    """
    key = f"timeline:{user_id}"
    max_score = f"({cursor}" if cursor is not None else "+inf"
    return await _client.zrevrangebyscore(key, max_score, "-inf", start=0, num=limit)


async def remove_from_timeline(user_id: str, post_id: str) -> None:
    """For delete/moderation flows added later."""
    await _client.zrem(f"timeline:{user_id}", post_id)


# Hybrid fanout (Milestone 7)


def _authored_key(author_id: str) -> str:
    return f"authored:{author_id}"


async def push_to_authored(author_id: str, post_id: str, score: float) -> None:
    """
    The heavy path's simgle write, replacing an O(followers) loop.
    Created lazily — this key doesn't exist until an account's first
    post while over HEAVY_FANOUT_THRESHOLD. No TIMELINE_MAX trimming
    here: pruning happens naturally at merge time via the cursor+limit
    query, same as any other source.
    """
    await _client.zadd(_authored_key(author_id), {post_id: score})


async def get_timeline_candidates(user_id: str, cursor: Optional[float] = None, limit: int = 50) -> List[Tuple[str, float]]:
    """
    Scored variant of get_timeline_ids(), used by the Milestone 7 merge
    path. Returns (post_id, score) pairs so app.py's get_timeline() can
    merge multiple sources by score BEFORE resolving any post bodies.
    """
    key = f"timeline:{user_id}"
    max_score = f"({cursor}" if cursor is not None else "+inf"
    pairs = await _client.zrevrangebyscore(key, max_score, "-inf", start=0, num=limit, withscores=True)
    return [(pid, score) for pid, score in pairs]


async def get_authored_candidates_bulk(author_ids: List[str], cursor: Optional[float] = None, limit: int = 50) -> Dict[str, List[Tuple[str, float]]]:
    """
    One ZREVRANGEBYSCORE per followed account against authored:{id},
    pipelined into a single Redis round-trip. Accounts that have never
    gone heavy simply have no authored: key — Redis returns an empty
    list for them. Deliberately no "is this account currently heavy"
    index is maintained; see ADR-2 in architecture-review.md.
    """
    if not author_ids:
        return {}
    max_score = f"({cursor}" if cursor is not None else "+inf"
    async with _client.pipeline(transaction=False) as pipe:
        for author_id in author_ids:
            pipe.zrevrangebyscore(_authored_key(author_id), max_score, "-inf", start=0, num=limit, withscores=True)
        results = await pipe.execute()
    return {author_id: [(pid, score) for pid, score in pairs] for author_id, pairs in zip(author_ids, results)}


# Post cache (Milestone 5)


def _post_key(post_id: str) -> str:
    return f"post:{post_id}"


async def get_posts(post_ids: List[str]) -> dict:
    if not post_ids:
        return {}
    raw_values = await _client.mget([_post_key(pid) for pid in post_ids])
    return {pid: json.loads(raw) for pid, raw in zip(post_ids, raw_values) if raw is not None}


async def set_post(post: dict, ttl: int = POST_CACHE_TTL_SECONDS) -> None:
    await _client.set(_post_key(post["id"]), json.dumps(post), ex=ttl)


async def set_posts(posts: List[dict], ttl: int = POST_CACHE_TTL_SECONDS) -> None:
    if not posts:
        return
    async with _client.pipeline(transaction=False) as pipe:
        for post in posts:
            pipe.set(_post_key(post["id"]), json.dumps(post), ex=ttl)
        await pipe.execute()
