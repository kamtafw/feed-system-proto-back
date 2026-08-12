"""
worker.py — standalone event-processing process.

Milestone 4: fanout_consumer and realtime_consumer no longer run inside
the HTTP server process. This process's only job is to consume
PostCreated events from Redis Streams and run the consumers against them.

Milestone 7.5: this process now shares the same PubSubRouter used by
main.py, imported from app.ws_router. It previously called
manager.init()/close() directly, which the docstring here used to note
left "unused overhead" — a local connections dict and a _listen()
forwarding loop this process never needed, since it never accepts a
browser WebSocket connection. That's now fully resolved by the router's
lazy listener startup (ADR-3, docs/milestone-7.5-cross-process-events.md):
this process's PubSubRouter instance never calls register() (nothing
here ever calls manager.connect()/system.connect()), so the listener
never starts — genuinely zero overhead, not just "present but unused."

realtime_consumer calls manager.send(), which only needs router.publish()
— a method that works identically whether or not this process has ever
registered a single local subscriber.

Milestone 8: adds on_post_created and on_follow_created, the Notification
subsystem's write-side consumers (app/notifications.py). They're
registered here, alongside fanout_consumer/realtime_consumer, rather than
folded into consumers.py — timeline fanout and notification creation are
different business capabilities that happen to react to the same events;
see app/notifications.py's module docstring for the full reasoning.
on_post_created has no ordering dependency on fanout_consumer or
realtime_consumer (unlike fanout->realtime's timeline-before-push
guarantee from M0.5) — it's registered last here purely for readability,
not because order matters to it.

Independent of the HTTP process's lifecycle — can be started, stopped, or
scaled (more instances) without touching the HTTP server at all.

Run:
    uv run worker.py
"""

import asyncio

from app import db, cache
from app.config import DATABASE_URL, REDIS_URL
from app.consumers import fanout_consumer, realtime_consumer
from app.notifications import on_post_created, on_follow_created
from app.event_bus import bus
from app.ws_router import router


async def main() -> None:
    await db.init_db(DATABASE_URL)
    await cache.init_cache(REDIS_URL)
    await bus.init(REDIS_URL)
    await router.init(REDIS_URL)

    bus.subscribe("PostCreated", fanout_consumer)
    bus.subscribe("PostCreated", realtime_consumer)
    bus.subscribe("PostCreated", on_post_created)
    bus.subscribe("FollowCreated", on_follow_created)

    print("✅  Worker ready — consuming PostCreated events")

    try:
        await bus.listen()
    finally:
        await router.close()
        await bus.close()
        await cache.close_cache()
        await db.close_db()


if __name__ == "__main__":
    asyncio.run(main())
