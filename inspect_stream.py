import asyncio, os
from dotenv import load_dotenv
import redis.asyncio as aioredis

load_dotenv()


async def main():
    r = aioredis.from_url(os.getenv("REDIS_URL"), decode_responses=True)
    print("XLEN:", await r.xlen("ff:stream:PostCreated"))
    print("GROUPS:", await r.xinfo_groups("ff:stream:PostCreated"))
    print("PENDING:", await r.xpending_range("ff:stream:PostCreated", "ff_consumers", min="-", max="+", count=10))
    await r.close()


asyncio.run(main())
