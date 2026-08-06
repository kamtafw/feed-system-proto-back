"""
test_hybrid_fanout.py — Verify the hybrid fanout branch (Milestone 7):
light accounts still fan out on write; heavy accounts skip it entirely
and are only visible to followers through the read-time merge.

Run against a live Postgres + Redis:

    uv run test_hybrid_fanout.py

Exercises the REAL fanout_consumer() and cache functions directly — the
same functions worker.py's event loop calls. This test only verifies the
Redis-level fanout/merge behavior, not full post-body resolution
(that's already covered by Milestone 5's verification) — no rows are
written to the posts table here on purpose, to keep this focused.
"""

import asyncio
import time
import uuid

import redis.asyncio as aioredis

from app import cache, db
from app.config import DATABASE_URL, HEAVY_FANOUT_THRESHOLD, REDIS_URL
from app.consumers import fanout_consumer

SEP = "—" * 56


def uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


async def make_user_with_followers(author_name: str, follower_count: int):
    author_id = uid(author_name)
    await db.create_user(author_id, author_name, password_hash="x")
    follower_ids = []
    for i in range(follower_count):
        fid = uid(f"{author_name}-f{i}")
        await db.create_user(fid, fid, password_hash="x")
        await db.add_follow(fid, author_id)
        follower_ids.append(fid)
    return author_id, follower_ids


async def read_merged_timeline(user_id: str, following: list, limit: int = 50) -> list:
    """
    Mirrors app.py's get_timeline() merge logic exactly, so this proves
    what a real GET /timeline request would return for this follower —
    not just that the write path branched correctly.
    """
    own = await cache.get_timeline_candidates(user_id, cursor=None, limit=limit + 1)
    authored = await cache.get_authored_candidates_bulk(following, cursor=None, limit=limit + 1)

    pool = {}
    for pid, score in own:
        pool[pid] = score
    for candidates in authored.values():
        for pid, score in candidates:
            pool.setdefault(pid, score)

    ranked = sorted(pool.items(), key=lambda item: item[1], reverse=True)
    return [pid for pid, _ in ranked[:limit]]


async def main() -> None:
    await db.init_db(DATABASE_URL)
    await cache.init_cache(REDIS_URL)

    print(SEP)
    print(" FanoutFeed — Hybrid fanout verification")
    print(f" HEAVY_FANOUT_THRESHOLD = {HEAVY_FANOUT_THRESHOLD}")
    print(SEP)

    # [1] LIGHT account — followers at the threshold
    print("\n[1] Light account")
    light_author, light_followers = await make_user_with_followers("light", HEAVY_FANOUT_THRESHOLD)
    light_post_id = uid("post")
    await fanout_consumer(
        {"post_id": light_post_id, "author_id": light_author, "author_name": "light", "created_at": time.time()}
    )

    for fid in light_followers:
        ids = await cache.get_timeline_ids(fid, cursor=None, limit=10)
        assert light_post_id in ids, f"Light-path follower {fid} did NOT receive the post directly"
    authored_empty = await cache.get_authored_candidates_bulk([light_author], cursor=None, limit=10)
    assert authored_empty[light_author] == [], "authored:{light_author} should be empty for a light account"
    print(f"    ✅  {len(light_followers)} follower(s) received the post via direct fanout")
    print(f"    ✅  authored:{light_author} is empty, as expected")

    # [2] HEAVY account — followers over the threshold
    print("\n[2] Heavy account")
    heavy_author, heavy_followers = await make_user_with_followers("heavy", HEAVY_FANOUT_THRESHOLD + 3)
    heavy_post_id = uid("post")
    await fanout_consumer(
        {"post_id": heavy_post_id, "author_id": heavy_author, "author_name": "heavy", "created_at": time.time()}
    )

    for fid in heavy_followers:
        ids = await cache.get_timeline_ids(fid, cursor=None, limit=10)
        assert heavy_post_id not in ids, f"Heavy-path follower {fid} should NOT have a direct timeline write"
    authored_heavy = await cache.get_authored_candidates_bulk([heavy_author], cursor=None, limit=10)
    authored_ids = [pid for pid, _ in authored_heavy[heavy_author]]
    assert authored_ids == [heavy_post_id], f"Expected exactly [{heavy_post_id}], got {authored_ids}"
    print(f"    ✅  None of {len(heavy_followers)} followers got a direct timeline write")
    print(f"    ✅  authored:{heavy_author} contains exactly the one post")

    # [3] Read-time merge — a heavy-path follower who received ZERO
    # direct writes must still see the post via the merge logic.
    print("\n[3] Read-time merge for a heavy-account follower")
    sample_follower = heavy_followers[0]
    merged = await read_merged_timeline(sample_follower, following=[heavy_author])
    assert heavy_post_id in merged, (
        f"{sample_follower} follows a heavy account but the merged timeline doesn't surface the post: {merged}"
    )
    print(f"    ✅  {sample_follower}'s merged timeline surfaces {heavy_post_id!r} despite receiving no direct write")

    print(f"\n{SEP}\n Hybrid fanout verified — light writes fan out, heavy writes merge at read time.\n{SEP}")

    # Cleanup
    all_ids = [light_author, *light_followers, heavy_author, *heavy_followers]
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE id = ANY($1::text[])", all_ids)  # cascades follows

    cleanup_redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    keys = [f"timeline:{uid_}" for uid_ in all_ids] + [f"authored:{light_author}", f"authored:{heavy_author}"]
    await cleanup_redis.delete(*keys)
    await cleanup_redis.close()

    await cache.close_cache()
    await db.close_db()


if __name__ == "__main__":
    asyncio.run(main())