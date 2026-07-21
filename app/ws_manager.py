"""
ws_manager.py — WebSocket connection management with Redis Pub/Sub routing.

Milestone 3 upgrade: notifications no longer go directly from consumer
to WebSocket. They travel through Redis so any worker can reach any user.

The routing flow:
    User connects to Worker A
        → WebSocket stored in Worker A's local dict
        → Worker A subscribes to "ws:notify:{user_id}" on Redis

    realtime_consumer (on any worker) wants to notify that user
        → calls manager.send(user_id, data)
        → publishes to "ws:notify:{user_id}" on Redis

    Worker A's _listen() task receives the pub/sub message
        → finds the WebSocket in its local dict
        → forwards the message

Single-process behaviour is identical to before — the message travels
through Redis as an intermediary but arrives at the same WebSocket.
Multi-process behaviour allows any worker to reach any user.

Why a keepalive channel?
    redis-py's pubsub.listen() is a generator that blocks until a message
    arrives. It must be subscribed to at least one channel before entering
    the loop, or it may hang. We subscribe to "ws:_keepalive" at startup
    so the listener is live before any users connect. Real user channels
    are added and removed dynamically as users arrive and leave.
"""

import asyncio
import json
from typing import Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import WebSocket

_NOTIFY_PREFIX = "ws:notify:"  # ws:notify:{user_id}
_KEEPALIVE_CH = "ws:_keepalive"  # dummy channel to keep listener alive


class ConnectionManager:
    """
    Tracks WebSocket connections for THIS worker process only.

    send() publishes to Redis instead of calling ws.send_json() directly.
    This makes the notification path worker-agnostic: the message goes into
    Redis, and whichever worker holds the user's connection receives it and
    forwards it locally.
    """

    def __init__(self) -> None:
        self._connections: Dict[str, WebSocket] = {}
        self._redis: Optional[aioredis.Redis] = None
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._listener: Optional[asyncio.Task] = None

    # Lifecycle

    async def init(self, redis_url: str) -> None:
        """
        Call once at startup from the lifespan context.
        Creates the Redis client, subscribes to the keepalive channel,
        and starts the background listener task.
        """
        self._redis = aioredis.from_url(redis_url, decode_responses=True)
        self._pubsub = self._redis.pubsub()

        # Must subscribe before starting listen() — otherwise the generator
        # has nothing to block on and may not process later subscriptions.
        await self._pubsub.subscribe(_KEEPALIVE_CH)

        self._listener = asyncio.create_task(self._listen())
        print("✅  WebSocket manager ready (Redis Pub/Sub routing)")

    async def close(self) -> None:
        if self._listener:
            self._listener.cancel()
            try:
                await self._listener
            except asyncio.CancelledError:
                pass
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.aclose()
        if self._redis:
            await self._redis.aclose()

    # Connection lifecycle

    async def connect(self, user_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self._connections[user_id] = ws
        # Subscribe so this worker receives notifications for this user
        await self._pubsub.subscribe(f"{_NOTIFY_PREFIX}{user_id}")
        print(f"[WS] {user_id} connected  " f"(pid={__import__('os').getpid()}, " f"{len(self._connections)} local connections)")

    def disconnect(self, user_id: str) -> None:
        self._connections.pop(user_id, None)
        # Unsubscribe asynchronously — disconnect() is called from exception
        # handlers where we can't await, so fire-and-forget is correct here.
        asyncio.create_task(self._pubsub.unsubscribe(f"{_NOTIFY_PREFIX}{user_id}"))
        print(f"[WS] {user_id} disconnected  " f"(pid={__import__('os').getpid()}, " f"{len(self._connections)} local connections)")

    def is_online(self, user_id: str) -> bool:
        """
        True if THIS worker holds a connection for user_id.
        In multi-worker deployments this is local-only — a user connected
        to a different worker will return False here. Use only for debug
        context (e.g. the EventLog panel), not to gate notification delivery.
        """
        return user_id in self._connections

    def online_users(self) -> List[str]:
        """Returns user IDs connected to this worker (local only)."""
        return list(self._connections.keys())

    # Sending

    async def send(self, user_id: str, data: dict) -> None:
        """
        Route a notification to user_id via Redis Pub/Sub.

        The message is published to ws:notify:{user_id}. The worker that
        subscribed to that channel (i.e. the worker holding this user's
        WebSocket) will receive it via _listen() and forward it.

        If no worker is subscribed to the channel — user is offline on
        all workers — Redis discards the message silently. No error,
        no retry needed. Offline users see the post on next timeline fetch.
        """
        await self._redis.publish(
            f"{_NOTIFY_PREFIX}{user_id}",
            json.dumps(data),
        )

    # Background listener

    async def _listen(self) -> None:
        """
        Receives Redis Pub/Sub messages for locally-connected users and
        forwards them to the right WebSocket.

        Runs for the lifetime of the worker process. Channels are subscribed
        and unsubscribed dynamically as users connect and disconnect.
        The keepalive channel ensures this loop stays alive even when no
        users are connected.
        """
        async for message in self._pubsub.listen():
            if message["type"] != "message":
                # Subscription confirmations have type "subscribe" — skip them
                continue

            channel: str = message["channel"]
            if not channel.startswith(_NOTIFY_PREFIX):
                # Ignore the keepalive channel and any unexpected channels
                continue

            user_id = channel.removeprefix(_NOTIFY_PREFIX)
            ws = self._connections.get(user_id)

            if ws is None:
                # User disconnected between the publish and this delivery
                continue

            try:
                await ws.send_text(message["data"])
            except Exception:
                # WebSocket is stale — clean up and unsubscribe
                self.disconnect(user_id)


class SystemBroadcaster:
    """
    Broadcasts architecture events to every connected debug client.
    Unchanged from previous milestones — debug panel is per-worker,
    which is acceptable (each worker's panel shows its own events).
    """

    def __init__(self) -> None:
        self._clients: List[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        if ws in self._clients:
            self._clients.remove(ws)

    async def broadcast(self, data: dict) -> None:
        dead = []
        for ws in self._clients:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._clients.remove(ws)


manager = ConnectionManager()
system = SystemBroadcaster()
