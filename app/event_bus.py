"""
event_bus.py — Redis Pub/Sub event bus.

Replaces InMemoryEventBus from the prototype.

Two Redis connections are required by design:
    _pub  → used for publishing (normal Redis commands)
    _sub  → dedicated to pub/sub mode (once subscribed, a connection
            can only issue subscribe/unsubscribe/ping commands)

The listen() coroutine runs as a background asyncio task started in
main.py's lifespan. It blocks on the Redis subscription stream and
dispatches each incoming message to the registered handlers in order —
matching the sequential guarantee from the prototype (fanout before realtime).

Why not asyncio.gather() across handlers?
    Same reason as the prototype: fanout must complete before realtime fires.
    The sorted set write has to exist before the WebSocket push tells the
    client to fetch the timeline. Sequential await preserves this guarantee.
"""

import asyncio
import json
import redis.asyncio as aioredis
from typing import Awaitable, Callable, Dict, List, Optional


class RedisPubSubEventBus:

    def __init__(self) -> None:
        self._handlers: Dict[str, List[Callable[..., Awaitable]]] = {}
        self._pub:    Optional[aioredis.Redis]            = None
        self._sub:    Optional[aioredis.Redis]            = None
        self._pubsub: Optional[aioredis.client.PubSub]   = None

    async def init(self, url: str) -> None:
        self._pub    = aioredis.from_url(url, decode_responses=True)
        self._sub    = aioredis.from_url(url, decode_responses=True)
        self._pubsub = self._sub.pubsub()
        print("✅  Redis event bus ready")

    def subscribe(self, event_type: str, handler: Callable[..., Awaitable]) -> None:
        self._handlers.setdefault(event_type, []).append(handler)

    async def publish(self, event_type: str, payload: dict) -> None:
        """
        Publish an event.  Returns as soon as Redis acknowledges receipt —
        handlers run asynchronously in the listen() loop.

        This means POST /posts returns to the client immediately; fanout
        and realtime happen in the background.  That's the correct behaviour
        for a real system (unlike the prototype where they were awaited inline).
        """
        await self._pub.publish(event_type, json.dumps(payload))

    async def listen(self) -> None:
        """
        Background coroutine.  Subscribe to every registered channel and
        dispatch messages to handlers sequentially.

        Error handling per handler: one broken handler does not kill the loop
        or skip subsequent handlers in the chain.
        """
        if not self._handlers:
            return

        await self._pubsub.subscribe(*self._handlers.keys())
        print(f"[EventBus] Listening on channels: {list(self._handlers.keys())}")

        async for message in self._pubsub.listen():
            if message["type"] != "message":
                # "subscribe" confirmation messages have type "subscribe" — skip
                continue

            event_type: str = message["channel"]
            try:
                payload = json.loads(message["data"])
            except json.JSONDecodeError as e:
                print(f"[EventBus] Malformed payload on {event_type!r}: {e}")
                continue

            for handler in self._handlers.get(event_type, []):
                try:
                    await handler(payload)
                except Exception as exc:
                    print(f"[EventBus] Handler error on {event_type!r}: {exc!r}")

    async def close(self) -> None:
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.aclose()
        if self._pub:
            await self._pub.aclose()
        if self._sub:
            await self._sub.aclose()


bus = RedisPubSubEventBus()