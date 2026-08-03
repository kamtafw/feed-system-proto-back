"""
test_cursor_pagination.py — Verify cursor-based timeline pagination survives
concurrent writes (Milestone 6).

Run independently against a live Redis instance:

    uv run test_cursor_pagination.py

What it proves:
  1.  Single-boundary correctness: a new post inserted between page fetches
      doesn't shift or duplicate anything on the next page — the property
      offset-based pagination could NOT guarantee.
  2.  Multi-page session correctness: scrolling through several pages while
      new posts land between EACH page fetch still yields every original
      post exactly once, in the same order, with none of the newly-inserted
      posts leaking into the already-in-progress scroll session.

Exercises the real cache.get_timeline_ids() — the same function app.py's
GET /timeline calls — not a reimplementation of its logic.
"""

import asyncio
import time

import redis.asyncio as aioredis

from app import cache
from app.config import REDIS_URL

TEST_USER = "test_cursor_pagination_user"
PAGE_SIZE = 5
SEP = "—" * 56


async def fetch_page(cursor, scores):
    """Mirrors app.py's get_timeline() pagination logic."""
    fetched = await cache.get_timeline_ids(TEST_USER, cursor=cursor, limit=PAGE_SIZE + 1)
    has_more = len(fetched) > PAGE_SIZE
    page = fetched[:PAGE_SIZE]
    next_cursor = scores[page[-1]] if (page and has_more) else None
    return page, next_cursor


async def main() -> None:
    await cache.init_cache(REDIS_URL)
    cleanup = aioredis.from_url(REDIS_URL, decode_responses=True)
    key = f"timeline:{TEST_USER}"
    await cleanup.delete(key)  # start clean

    print(SEP)
    print(" FanoutFeed — Cursor pagination verification")
    print(SEP)

    # Seed 12 "original" posts, distinct increasing scores, well below "now"
    # so anything inserted later is unambiguously newer.
    base = time.time() - 1000
    scores = {}
    for i in range(12):
        pid, score = f"orig-{i}", base + i
        await cache.push_to_timeline(TEST_USER, pid, score)
        scores[pid] = score

    expected_order = [f"orig-{i}" for i in range(11, -1, -1)]  # newest → oldest
    print(f"\n[1] Seeded 12 original posts. Expected scroll order:\n    {expected_order}")

    collected, injected = [], []
    cursor, page_num = None, 0

    while True:
        page_num += 1
        page, cursor = await fetch_page(cursor, scores)
        print(f"\n[Page {page_num}] {page}  (next_cursor={cursor})")
        collected.extend(page)

        if cursor is None:
            break

        # simulate a live user posting between this page and the next fetch
        new_id, new_score = f"new-{page_num}", time.time()
        await cache.push_to_timeline(TEST_USER, new_id, new_score)
        scores[new_id] = new_score
        injected.append(new_id)
        print(f"    ↳ inserted {new_id!r} between fetches (simulates a concurrent post)")

    print(f"\n{SEP}\n Assertions\n{SEP}")

    assert collected == expected_order, (
        f"Order/completeness mismatch.\n  expected: {expected_order}\n  got:      {collected}"
    )
    print(f"✅  All 12 original posts returned exactly once, in the correct order")

    leaked = [pid for pid in injected if pid in collected]
    assert not leaked, f"Injected posts leaked into the scroll session: {leaked}"
    print(f"✅  None of the {len(injected)} concurrently-inserted posts leaked in: {injected}")

    # Close the loop: the injected posts should still surface via a FRESH
    # top-of-feed fetch (cursor=None) — proving they were genuinely written
    # and would appear via the normal "new posts" banner refresh, just
    # correctly excluded from the already-in-progress pagination above.
    fresh_page, _ = await fetch_page(None, scores)
    assert all(pid in fresh_page for pid in injected), (
        f"Expected {injected} at the top of a fresh fetch, got {fresh_page}"
    )
    print(f"✅  A fresh top-of-feed fetch correctly surfaces the injected posts: {fresh_page}")

    await cleanup.delete(key)
    await cleanup.close()
    await cache.close_cache()
    print(f"\n{SEP}\n Cursor pagination verified — no drift, no duplicates, no leaks.\n{SEP}")


if __name__ == "__main__":
    asyncio.run(main())