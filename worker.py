"""
worker.py — standalone event-processing process.

Milestone 4: fanout_consumer and realtime_consumer no longer run inside
the HTTP server process. This process's only job is to consume
PostCreated events from Redis Streams and run the consumers against them.

Independent of the HTTP process's lifecycle — can be started, stopped, or
scaled (more instances) without touching the HTTP server at all.

Run:
  uv run worker.py
"""

import asyncio

from app import db, cache
from app.config import DATABASE_URL, REDIS_URL
from app.consumers import fanout_consumer, realtime_consumer
from app.event_bus import bus
from app.ws_manager import manager


async def main() -> None:
    await db.init_db(DATABASE_URL)
    await cache.init_cache(REDIS_URL)
    await bus.init(REDIS_URL)

    # realtime_consumer calls manager.send(), which needs a Redis client to
    # publish to ws:notify:{user_id}. Reusing ConnectionManager.init() here
    # is the simplest option since it already owns that connection logic.
    # This process never accepts a browser WebSocket, so the local
    # connections dict and the _listen() forwarding loop it starts
    # are unused overhead — not a correctness issue, just a seam worth
    # splitting into a slimmer "publisher-only client" later if it matters.
    await manager.init(REDIS_URL)

    bus.subscribe("PostCreated", fanout_consumer)
    bus.subscribe("PostCreated", realtime_consumer)

    print("✅  Worker ready — consuming PostCreated events")

    try:
        await bus.listen()
    finally:
        await manager.close()
        await bus.close()
        await cache.close_cache()
        await db.close_db()


if __name__ == "__main__":
    asyncio.run(main())
