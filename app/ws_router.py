"""
ws_router.py — Generic cross-process WebSocket event delivery (Milestone 7.5).


Problem this solves: two separate ad-hoc mechanisms previously existed
for "deliver a message to WebSocket clients regardless of which process
produced it" — ConnectionManager's Redis Pub/Sub routing (Milestone 3,
working correctly) and SystemBroadcaster's plain in-memory client list
(Milestone 3, silently broken across processes since Milestone 4 split
fanout/realtime consumers into worker.py). Rather than patch
SystemBroadcaster to duplicate ConnectionManager's logic, both are now
thin facades over this one shared router.

Scope boundary (ADR-6, architecture-review.md):
  PubSubRouter is scoped to WebSocket event delivery ONLY — not a
  general-purpose cross-process messaging primitive. It assumes:
    - the sink is always a WebSocket (_listen() calls ws.send_text()
      directly, no other consumer type is supported)
    - fire-and-forget delivery — no durability, no replay, no ACKs
  Anything needing durability/replay/ACKs belongs in event_bus.py
  (Redis Streams), which already exists for exactly that need.

Core invariant this class exists to maintain (ADR-4):
  A channel is subscribed at the Redis level IFF it has at least one
  locally-registered WebSocket in THIS process. register()/unregister()
  are the only two places that invariant is touched, atomically.
"""

import asyncio
import json
from typing import Dict, Optional, Set

import redis.asyncio as aioredis
from fastapi import WebSocket

_KEEPALIVE_CH = "ws:_keepalive"


class PubSubRouter:
    """
    Generic cross-process event delivery: any process publishes to a
    channel; whichever process(es) hold locally-registered WebSockets
    for that channel forward the message. Producers never need to know
    where subscribers live — that's the whole point.
    """

    def __init__(self) -> None:
        self._redis: Optional[aioredis.Redis] = None
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._listener: Optional[asyncio.Task] = None
        self._channels: Dict[str, Set[WebSocket]] = {}

    # Lifecycle

    async def init(self, redis_url) -> None:
        """
        Only creates a publish-capable client. The subscribe connection,
        keepalive channel, and _listen() task are deferred to the first
        register() call (ADR-3) — a process that only ever publishes
        (worker.py) never pays for a listener it doesn't need.
        """
        self._redis = aioredis.from_url(redis_url, decode_responses=True)
        await self._redis.ping()
        print("✅  PubSubRouter ready (publish-capable; listener starts on first subscriber)")

    async def close(self) -> None:
        if self._listener:
            self._listener.cancel()
            try:
                await self._listener
            except asyncio.CancelledError:
                pass
        if self._pubsub:
            await self._pubsub.close()
        if self._redis:
            await self._redis.close()

    async def _ensure_listening(self) -> None:
        """Lazily starts the subscribe connection + listener task exactly once."""
        if self._pubsub is not None:
            return
        self._pubsub = self._redis.pubsub()
        await self._pubsub.subscribe(_KEEPALIVE_CH)  # listen() needs ≥1 subscription
        self._listener = asyncio.create_task(self._listen())
        print("✅  PubSubRouter listener started (first local subscriber registered)")

    # Registration — used by ConnectionManager / SystemBroadcaster only

    async def register(self, channel: str, ws: WebSocket) -> None:
        await self._ensure_listening()
        subscribers = self._channels.setdefault(channel, set())
        if not subscribers:
            await self._pubsub.subscribe(channel)
        subscribers.add(ws)

    async def unregister(self, channel: str, ws: WebSocket) -> None:
        subscribers = self._channels.get(channel)
        if subscribers is None:
            return
        subscribers.discard(ws)  # unambiguous — no identity-guard needed (ADR-2)
        if not subscribers:
            del self._channels[channel]
            if self._pubsub is not None:
                await self._pubsub.unsubscribe(channel)

    # Publish — the ONLY method a pure producer (e.g. consumers.py) needs.
    # Works with zero local registration; a process that never calls
    # register() can still publish freely.

    async def publish(self, channel: str, data: dict) -> None:
        await self._redis.publish(channel, json.dumps(data))

    # Internal forwarding loop

    async def _listen(self) -> None:
        async for message in self._pubsub.listen():
            if message["type"] != "message":
                continue
            channel = message["channel"]
            for ws in list(self._channels.get(channel, ())):
                try:
                    await ws.send_text(message["data"])
                except Exception:
                    self._channels.get(channel, set()).discard(ws)

# Module-level singleton — imported by ws_manager.py so ConnectionManager
# and SystemBroadcaster share exactly one router instance per process.
router = PubSubRouter()