"""
inspect_post_cache.py — verify the Milestone 5 post cache directly against Redis.

Usage:
    python inspect_post_cache.py <post_id>          inspect one cached post
    python inspect_post_cache.py --list             list all cached post keys
    python inspect_post_cache.py --evict <post_id>  DEL a key (simulate a cache miss)
"""

import asyncio, json, os, sys
from dotenv import load_dotenv
import redis.asyncio as aioredis

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


async def inspect(post_id: str) -> None:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    key = f"post:{post_id}"
    raw, ttl = await r.get(key), await r.ttl(key)
    print(f"key:   {key}")
    if raw is None:
        print("value: (missing — not cached)")
    else:
        print(f"value: {json.dumps(json.loads(raw), indent=2)}")
        print(f"ttl:   {ttl}s")
    await r.close()


async def list_all() -> None:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    keys = sorted([k async for k in r.scan_iter(match="post:*")])
    print(f"{len(keys)} cached post(s):")
    for k in keys:
        print(f"  {k}  (ttl: {await r.ttl(k)}s)")
    await r.close()


async def evict(post_id: str) -> None:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    deleted = await r.delete(f"post:{post_id}")
    print("Deleted" if deleted else "Key did not exist")
    await r.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    if sys.argv[1] == "--list":
        asyncio.run(list_all())
    elif sys.argv[1] == "--evict" and len(sys.argv) == 3:
        asyncio.run(evict(sys.argv[2]))
    else:
        asyncio.run(inspect(sys.argv[1]))
