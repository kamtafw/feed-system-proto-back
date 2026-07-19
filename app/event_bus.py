"""
event_bus.py — Redis Streams event bus

Replaces the Redis Pub/Sub implementation from Milestone 1

-----------------------------------------------------------
Why Redis Streams over Pub/Sub
-----------------------------------------------------------
Pub/Sub is a live broadcast. If the listener task isn't running when publish()
is called — server restart, hot reload, asyncio task crash — the message is gone.
The post exists in PostgreSQL but fanout never runs. With real authenticated users
this is a silent data loss bug, not a minor inconvenience.

Redis Streams is a persistent, ordered log:

    XADD        append a message to the stream (replaces PUBLISH)
    XREADGROUP  deliver messages to a consumer group (replaces SUBSCRIBE)
    XACK        confirm a message was fully processed (no equivalent in Pub/Sub)
    XAUTOCLAIM  reclaim and retry unACKed messages after a timeout

The message is not removed from the stream until the consumer ACKs it. If the
consumer crashes between receiving and processing, the message stays in the pending
list. On the next startup XAUTOCLAIM finds it and re-delivers it.

-----------------------------------------------------------
Delivery guarantee
-----------------------------------------------------------
At-least-once: a message is retried until it is ACKed. Consumers must be
idempotent — writing the same post_id to a Redis sorted set twice is harmless
(ZADD is idempotent), and sending the same NEW_POST WebSocket message twice to
an already-notified client is visible but acceptable at this scale.

Exactly-once would require distributed transactions across Redis and PostgreSQL,
which is out of scope for this milestone.

-----------------------------------------------------------
Sequential handler execution (fanout before realtime)
-----------------------------------------------------------
Handlers run in registration order, serially, within_process(). We only ACK
after all handlers succeed. This preserves the guarantee that the timeline is
written before the WebSocket push fires — the same contract as the in-memory bus.

-----------------------------------------------------------
Public interface — unchanged from Pub/Sub version
-----------------------------------------------------------
    bus.init(url)
    bus.subscribe(event_type, handler)
    bus.publish(event_type, payload)    ← now XADD instead of PUBLISH
    await bus.listen()                  ← now XREADGROUP loop instead of SUBSCRIBE
    await bus.close()
"""

import asyncio
import json
import os
from typing import Awaitable, Callable, Dict, List, Optional

import redis.asyncio as aioredis

from app.config import STREAM_MAX_LEN, STREAM_RECLAIM_MS

# Constants
_STREAM_PREFIX = "ff:stream:"  # ff:stream:PostCreated
_GROUP_NAME = "ff_consumers"  # one group — all workers share a cursor
_CONSUMER_NAME = f"worker-{os.getpid()}"  # unique per process; safe for multi-worker
_BLOCK_MS = 5_000  # block on XREADGROUP for 5 s before looping


class RedisStreamsEventBus:

    def __init__(self) -> None:
        self._handlers: Dict[str, List[Callable[..., Awaitable]]] = {}
        self._client: Optional[aioredis.Redis] = None

    # lifecycle

    async def init(self, url: str) -> None:
        self._client = aioredis.from_url(url, decode_responses=True)
        await self._client.ping()
        print("✅  Redis Streams event bus ready")

    async def close(self) -> None:
        if self._client:
            await self._client.close()

    # registration

    def subscribe(self, event_type: str, handler: Callable[..., Awaitable]) -> None:
        """Register a handler. Call before listen() is started."""
        self._handlers.setdefault(event_type, []).append(handler)

    # publish

    async def publish(self, event_type: str, payload: dict) -> None:
        """
        Append a message to the stream. Returns as soon as Redis confirms the write.

        The message is durable from this point: even if the listener task is not
        running, it will be delivered when listen() next starts. This is the core
        durability upgrade over Pub/Sub.

        maxlen + approximate=True keeps the stream bounded without an expensive
        exact-trim on every write. Redis trims to the internal node
        boundary, which is fast and keeps memory predictable.
        """
        await self._client.xadd(f"{_STREAM_PREFIX}{event_type}", {"data": json.dumps(payload)}, maxlen=STREAM_MAX_LEN, approximate=True)

    # consume

    async def listen(self) -> None:
        """
        Background coroutine. Must be started as an asyncio Task

        Startup sequence:
            1. Create consumer groups for each registered event type
            2. Enter the main loop:
                a. Reclaim stale pending messages (retry crashed work)
                b. Block-read new messages with XREADGROUP
                c. Process each message, ACK on success
        """
        await self._create_consumer_groups()

        # ">" means "give me messages not yet delivered into this group"
        streams = {f"{_STREAM_PREFIX}{et}": ">" for et in self._handlers}

        print(f"[EventBus] Listening on streams: {list(self._handlers.keys())} " f"(consumer: {_CONSUMER_NAME})")

        while True:
            try:
                # step 1: retry any previously failed messages
                await self._reclaim_pending()

                # step 2: read new messages, block up to _BLOCK_MS ms
                results = await self._client.xreadgroup(_GROUP_NAME, _CONSUMER_NAME, streams, count=10, block=_BLOCK_MS)

                if not results:
                    # timeout — no new messages; loop and try again
                    continue

                # step 3: process each message
                for stream_key, messages in results:
                    event_type = stream_key.removeprefix(_STREAM_PREFIX)
                    for msg_id, fields in messages:
                        await self._process(event_type, stream_key, msg_id, fields)

            except asyncio.CancelledError:
                # clean shutdown — let the task exit
                break
            except Exception as e:
                # unexpected error in the loop itself (not in a handler)
                # log and retry after a short pause rather than crashing
                print(f"[EventBus] Listener error: {e!r} — retrying in 1 s")
                await asyncio.sleep(1)

    # internal

    async def _create_consumer_groups(self) -> None:
        """
        Create consumer groups for each event type we handle. Idempotent —
        safe to call on every startup.

        id="$"      —   start from the tail of the stream (new messages only).
                        On first startup this is what we want: don't replay old events.
        mkstream    —   create the stream key if it doesn't exist yet.
        BUSYGROUP   —   the group already exists from a previous run. The group's
                        cursor is preserved, so unACKed messages from before the
                        restart will still be delivered via XAUTOCLAIM.
        """
        for event_type in self._handlers:
            stream_key = f"{_STREAM_PREFIX}{event_type}"
            try:
                await self._client.xgroup_create(stream_key, _GROUP_NAME, id="$", mkstream=True)
                print(f"[EventBus] Created consumer group for '{event_type}")
            except aioredis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    print(f"[EventBus] Consumer group for '{event_type}' already exists — resuming")
                else:
                    raise

    async def _process(self, event_type: str, stream_key: str, msg_id: str, fields: dict) -> None:
        """
        Deserialise the payload, run all handlers in order, then ACK.

        If a handler raises, we return without ACKing. The message stays in
        the pending list. _reclaim_pending() will retry it after STREAM_RECLAIM_MS.

        Malformed messages (bad JSON) are ACKed immediately — retrying them
        forever will accomplish nothing.
        """
        try:
            payload = json.loads(fields.get("data", "{}"))
        except json.JSONDecodeError as e:
            print(f"[EventBus] Malformed message {msg_id} — discarding: {e}")
            await self._client.xack(stream_key, _GROUP_NAME, msg_id)
            return

        for handler in self._handlers.get(event_type, []):
            try:
                await handler(payload)
            except Exception as e:
                print(
                    f"[EventBus] Handler error on '{event_type}' "
                    f'msg={msg_id}: {e!r}\n'
                    f"      Will retry after {STREAM_RECLAIM_MS//1000} s"
                )
                return # not ACKed — left in pending list for retry

        # all handlers completed successfully
        await self._client.xack(stream_key, _GROUP_NAME, msg_id)

    async def _reclaim_pending(self)->None:
        """
        Find messages that have been sitting in the pending list longer than
        STREAM_RECLAIM_MS and reassign them to this consumer for retry.

        This is the recovery path for the scenario:
            1. Consumer receives message, starts fanout_consumer
            2. Process crashes at step 50 of 200 followers
            3. On restart: XAUTOCLAIM finds the unACKed message, re-delivers it
            4. fanout_consumer runs again from the beginning

        Because ZADD is idempotent, re-running fanout for the same post_id
        to followers who already received it has not effect on their timelines.
        """
        for event_type in self._handlers:
            stream_key = f"{_STREAM_PREFIX}{event_type}"
            try:
                result = await self._client.xautoclaim(
                    stream_key,
                    _GROUP_NAME,
                    _CONSUMER_NAME,
                    min_idle_time=STREAM_RECLAIM_MS,
                    start_id="0-0",
                    count=10,
                )
                # redis-py >= 4.3.4 returns (next_start_id, messages, deleted_ids)
                if not result or not result[1]:
                    continue

                _, messages, _ = result
                for msg_id, fields in messages:
                    print(f"[EventBus] Reclaiming stale message {msg_id} on '{event_type}'")
                    await self._process(event_type, stream_key, msg_id, fields)

            except Exception as e:
                # non-fatal — log and continue; new messages still process normally
                print(f"[EventBus] Reclaim error on '{event_type}': {e!r}")


bus = RedisStreamsEventBus()
