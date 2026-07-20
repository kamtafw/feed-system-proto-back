# Milestone 2 — Event Bus Durability

## FanoutFeed · `milestone-2-redis-streams`

---

## Goal

Replace the Redis Pub/Sub event bus with Redis Streams, giving the `PostCreated`
pipeline at-least-once delivery guarantees. A message published to the event bus
must survive the consumer going offline and be delivered when it comes back.

---

## Previous architecture and its limitation

At the end of Milestone 1, the event bus used Redis Pub/Sub:

```
bus.publish("PostCreated", payload)
    │
    └─► redis.publish("PostCreated", json)
                │
                ▼
         listen() task
         (SUBSCRIBE loop)
              │
         ┌────┴─────┐
         ▼          ▼
   fanout_consumer  realtime_consumer
```

Pub/Sub is a live broadcast. `PUBLISH` sends the message to whoever is currently
subscribed. If the `listen()` background task was not running at the moment
`PUBLISH` fired — server restart, `uvicorn --reload` hot reload, unhandled
exception in the asyncio task — the message was gone. The post existed in
PostgreSQL, but fanout never ran. No follower received the post.

At the prototype stage this was acceptable: users would see the post on next
refresh, the failure was rare, and there were no real users to notice. With
authenticated users on the system, this is a silent correctness bug. It becomes
a reliability problem before adding the separate worker process in Milestone 4,
where the HTTP server and the consumer run in different processes with an
intentional startup gap between them.

---

## Why Redis Streams

Redis Streams is an append-only log — a data structure that Redis was explicitly
designed for event sourcing at this scale.

```
XADD        → append a message to the log (replaces PUBLISH)
XREADGROUP  → deliver messages to a named consumer group (replaces SUBSCRIBE)
XACK        → confirm a message was fully processed (no equivalent in Pub/Sub)
XAUTOCLAIM  → reclaim and retry unACKed messages after a timeout
```

The key difference: the message is not removed from the stream until the consumer
explicitly ACKs it. If the consumer crashes between receiving and processing, the
message stays in the **pending list** — a per-group record of delivered-but-not-ACKed
messages. On restart, `XAUTOCLAIM` finds these messages and re-delivers them.

Think of Pub/Sub as a live radio broadcast — tune in or miss it. Streams is a
podcast — you can listen when you're ready, and if you pause mid-episode, you
resume from exactly where you left off.

---

## Delivery guarantee

**At-least-once.** A message is retried until it is successfully ACKed. Consumers
must tolerate duplicate delivery.

`fanout_consumer` is safe to retry because its writes use Redis `ZADD`. Writing
a post ID that already exists in a user's sorted set is a no-op — no duplicate
timeline entries, no side effects.

`realtime_consumer` sends WebSocket messages on retry, which a client might
receive twice. At this scale, an occasional duplicate "you have a new post"
notification is acceptable.

**Exactly-once** would require distributed transactions across Redis and PostgreSQL.
That is out of scope for this architecture.

---

## Design decisions

### Sequential handler execution is preserved

In Milestone 1, `fanout_consumer` and `realtime_consumer` ran sequentially within
the `listen()` loop — fanout first, realtime second — guaranteeing that the
timeline was written before the WebSocket push fired. This contract is preserved
in `_process()`:

```python
for handler in self._handlers.get(event_type, []):
    await handler(payload)  # fanout runs, then realtime, in order

await self._client.xack(...)  # only after both succeed
```

If any handler raises, we return without ACKing. The message is retried with the
same handler order. A user who immediately calls `GET /timeline` after receiving
`NEW_POST` will always find the post there.

### ACK only on full success, not per-handler

We could ACK per-handler — fanout ACKs after it completes, realtime ACKs after it
completes. This would allow partial retries. We chose not to for two reasons:

1. We use a single stream entry per event. A partial ACK would require splitting
   the event into two separate stream entries, one per consumer — adding
   complexity for a problem that rarely occurs at this scale.

2. Fanout is idempotent. Retrying the full message (both handlers) has no
   correctness cost and keeps the code simple.

### XAUTOCLAIM runs before XREADGROUP on every loop iteration

```python
while True:
    await self._reclaim_pending()   # ← retry stale work first
    results = await self._client.xreadgroup(...)  # ← then read new work
```

This ordering ensures that recovery work is never starved by a stream of new
messages. If the consumer restarts with a backlog of 50 unACKed messages, it
processes those before taking on new ones.

### Malformed messages are ACKed immediately

If a message cannot be parsed (bad JSON), retrying it forever accomplishes
nothing. We ACK and discard it immediately:

```python
except json.JSONDecodeError:
    await self._client.xack(stream_key, _GROUP_NAME, msg_id)
    return  # discard — do not retry
```

In a more mature system these would be routed to a dead-letter queue for manual
inspection. That is deferred to a later milestone.

### Stream length is bounded with approximate trimming

```python
await r.xadd(stream_key, {"data": json.dumps(payload)},
             maxlen=STREAM_MAX_LEN, approximate=True)
```

`approximate=True` (the `~` prefix in Redis CLI notation) means Redis trims at
the nearest internal node boundary rather than an exact count. This avoids
rewriting internal nodes on every `XADD`, making writes significantly faster.
The actual stream length may temporarily exceed `STREAM_MAX_LEN` by a small
margin — acceptable for this use case.

A `STREAM_MAX_LEN` of 10 000 events is enough to cover a multi-hour outage at
typical write rates for 800–1000 users.

---

## What was built

### New files

```
test_streams.py     — standalone XACK/XAUTOCLAIM lifecycle verification
                    (does not require the server to be running)
```

### Modified files

```
backend/
  event_bus.py      — full replacement (XADD/XREADGROUP/XACK/XAUTOCLAIM)
  config.py         — STREAM_MAX_LEN, STREAM_RECLAIM_MS
```

### Unchanged files

Everything else. The public interface of `RedisStreamsEventBus` is identical to
`RedisPubSubEventBus`:

```python
await bus.init(REDIS_URL)
bus.subscribe("PostCreated", fanout_consumer)
bus.subscribe("PostCreated", realtime_consumer)
listener = asyncio.create_task(bus.listen())
# ... server runs ...
await bus.close()
```

`app.py`, `consumers.py`, `db.py`, `cache.py`, `auth.py`, and all frontend
files require zero changes.

---

## Stream structure

```
Stream key:   ff:stream:PostCreated
Group name:   ff_consumers
Consumer:     worker-{pid}        ← unique per process

Message format:
  id:    1234567890123-0           (Redis-generated timestamp + sequence)
  data:  {"post_id": "a1b2c3d4",
          "author_id": "alice",
          "author_name": "Alice",
          "created_at": 1720000000.0}
```

One stream per event type. A single consumer group means every message is
delivered to the group exactly once — whichever consumer instance reads it
first. For the current single-process deployment this is equivalent to the
old Pub/Sub behaviour, with the addition of persistence and retry.

---

## Request flow comparison

### Before (Milestone 1 — Redis Pub/Sub)

```
POST /posts
    │
    ├─ db.create_post()         → PostgreSQL ✓ (durable)
    ├─ system.broadcast()       → WebSocket debug panel
    └─ redis.publish()          → Pub/Sub broadcast
                │
                ▼
         ┌──────────────┐
         │  listen()    │  ← if this isn't running, event is LOST
         └──────┬───────┘
                │
           fanout + realtime (if received)
```

### After (Milestone 2 — Redis Streams)

```
POST /posts
    │
    ├─ db.create_post()         → PostgreSQL ✓ (durable)
    ├─ system.broadcast()       → WebSocket debug panel
    └─ redis.xadd()             → Stream ✓ (durable)
                │
         message persists in stream
                │
         ┌──────────────┐
         │  listen()    │     ← if this restarts, XAUTOCLAIM recovers the message
         └──────┬───────┘
                │
           _reclaim_pending() → XAUTOCLAIM
           xreadgroup()       → new messages
                │
           fanout_consumer    → ZADD ✓ (idempotent on retry)
           realtime_consumer  → WebSocket push (best-effort)
                │
           xack()             → message leaves pending list
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `STREAM_MAX_LEN` | `10000` | Max entries per stream (approximate trim) |
| `STREAM_RECLAIM_MS` | `30000` | Idle ms before XAUTOCLAIM reclaims a message |

Lower `STREAM_RECLAIM_MS` during development to speed up retry observations:

```env
STREAM_RECLAIM_MS=5000   # 5 s during testing; restore to 30000 for production
```

---

## Verification

### Standalone (no server required)

```bash
# Optionally set a short reclaim window for faster observation
echo "STREAM_RECLAIM_MS=3000" >> .env

python test_streams.py
```

Expected output:

```
[1] XADD — write message to stream
    msg_id: 1720000000000-0

[2] XREADGROUP — read without ACKing (simulated crash)
    Received: 1720000000000-0
    ↳ NOT calling XACK — message stays in pending list

[3] XPENDING — confirm message is in pending list
    msg_id:       1720000000000-0
    consumer:     test-script
    idle (ms):    12
    deliveries:   1
    ✅  Message is in the pending list — it will NOT be lost

[4] XAUTOCLAIM — reclaim after 3 s idle
    Reclaimed 1 message(s):
    ↺  1720000000000-0 → {"post_id": "test-xack-001" ...
    ✅  XAUTOCLAIM recovered the unACKed message for retry

[5] XACK — acknowledge the message (success path)
    ACKed 1 message(s)

[6] XPENDING — confirm pending list is now clear
    ✅  Pending list is empty — full lifecycle verified
```

### Retry path (with server running)

1. Set `STREAM_RECLAIM_MS=5000` in `.env`
2. Add `raise Exception("test")` at the top of `fanout_consumer`
3. Restart server, make a post
4. Observe in logs:
   ```
   [EventBus] Handler error on 'PostCreated' msg ...: Exception('test')
              Will retry after 5 s
   ```
5. Remove the `raise`, restart server
6. Within 5 seconds:
   ```
   [EventBus] Reclaiming stale message ... on 'PostCreated'
   ```
7. Fanout runs, timelines updated, message ACKed

### Restart path

1. Make a post
2. Stop the server immediately (Ctrl+C)
3. Restart
4. `XAUTOCLAIM` recovers the in-flight message on next reclaim cycle
5. Follower timelines are updated correctly
6. **Users who reconnect see the post — but receive no push notification**
   (see Known Limitations below)

### Redis CLI inspection

```bash
# Connect to your Redis instance
redis-cli -u "$REDIS_URL"

# Messages in the stream
XLEN ff:stream:PostCreated

# Pending messages for the consumer group
XPENDING ff:stream:PostCreated ff_consumers - + 10

# Group summary: pending count, last delivery ID
XINFO GROUPS ff:stream:PostCreated

# Consumer summary: messages in flight per consumer
XINFO CONSUMERS ff:stream:PostCreated ff_consumers
```

---

## Verification checklist

- [ ] `test_streams.py` runs to completion with all steps showing ✅
- [ ] Normal post: `XPENDING` is empty after fanout completes
- [ ] Injected failure: retry loop visible in logs every `STREAM_RECLAIM_MS`
- [ ] Retry recovery: fixing the handler causes message to process and ACK
- [ ] Restart recovery: in-flight message is reclaimed and fanout runs on restart
- [ ] Duplicate safety: posting the same event twice leaves no duplicate in timeline
- [ ] `XINFO GROUPS` shows `0` pending after a clean run

---

## Known limitations

### WebSocket notifications are not recovered after restart

After a server restart, `XAUTOCLAIM` correctly retries `fanout_consumer` — Redis
sorted set writes are idempotent and timelines are accurate. However,
`realtime_consumer` queries `ConnectionManager.is_online()`, which reflects the
state of the current process only. Because all WebSocket connections are held in
in-process memory, a restarted server has an empty `ConnectionManager`. All
followers appear offline. `REALTIME_SKIP` fires for every follower, and no push
notification is sent.

Users who reconnect after a restart see the correct timeline via `GET /timeline`,
but they receive no `NEW_POST` WebSocket event for posts that were in-flight at
restart time. **This is the expected behaviour of a single-process WebSocket
layer**, not a bug introduced by this milestone.

Mitigation: users see the post immediately on reconnect/refresh. The timeline
is the source of truth; WebSocket is a notification convenience.

Resolution: Milestone 3.

### No dead-letter queue for persistently failing messages

A message that always causes a handler exception will be retried indefinitely.
The current mitigation is a log line per retry. A dead-letter stream
(`ff:stream:dead-letter`) that receives messages exceeding a maximum delivery
count (e.g., `times_delivered > 5`) would make persistent failures visible and
actionable. This is deferred to a later operational milestone.

---

## Scaling ceiling unchanged

Redis Streams adds negligible overhead per publish: one `XADD` and one `XACK`
per `PostCreated` event. At 800–1000 users and typical write rates (< 30
posts/second), stream throughput is well within the headroom of any hosted Redis
instance. The scaling limits identified in Milestone 0.5 are unchanged.

---

## Next milestone — Milestone 3: WebSocket cross-worker routing

**Problem:** The restart test made the structural limit explicit. `ConnectionManager`
is a Python dict living in one process's memory. A server restart empties it. Two
uvicorn workers each have their own `ConnectionManager` with no knowledge of
connections held by the other. If worker 1 processes a `PostCreated` event and
Alice's WebSocket is connected to worker 2, Alice receives no notification.

**Solution:** Route WebSocket notifications through Redis Pub/Sub per user:

```
realtime_consumer
    │
    └─► redis.publish("ws:notify:alice", json)
                │
         Worker 1 subscribes to "ws:notify:alice"
         (because alice is connected to worker 1)
                │
         Finds alice in its local ConnectionManager
         Sends NEW_POST over her WebSocket
```

When a user connects, their `user_id` is registered in Redis along with the
worker identity. On disconnect, it is removed. The consumer publishes to the
Redis channel; the correct worker receives it and forwards it to the WebSocket.

**What it unlocks:** Running 2–4 uvicorn workers behind Nginx, notifications
surviving server restarts (reconnected clients are on the new worker's
`ConnectionManager` within milliseconds), and the architectural foundation for
independently scaling the HTTP and WebSocket layers.