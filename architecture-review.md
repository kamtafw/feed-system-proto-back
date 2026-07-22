# Milestone 3 — WebSocket Cross-Worker Routing

## FanoutFeed · `milestone-3-ws-routing`

---

## Goal

Make real-time `NEW_POST` notifications reach a user regardless of which
uvicorn worker holds their WebSocket connection, so the HTTP layer can run
more than one worker process.

---

## Previous architecture and its limitation

`ConnectionManager` held a plain Python dict mapping `user_id → WebSocket`.
`realtime_consumer` called `manager.send()`, which looked up the socket in
that dict and called `ws.send_json()` directly.

```
realtime_consumer
│
└─► manager.send(user_id, data)
│
└─► self._connections[user_id].send_json(data)
```

This only works if the consumer and the target's WebSocket live in the
same process. With a single worker this was invisible — everything is in
one process by definition. The moment a second worker is introduced,
whichever worker's event-loop happened to consume the `PostCreated`
message from the Streams consumer group is not necessarily the worker
holding the follower's socket. That follower's local dict has no entry,
`manager.send()` silently no-ops, and the notification is lost.

This was the structural blocker to horizontal scaling of the HTTP layer —
running 2+ workers behind a reverse proxy for more request-handling
capacity was not possible without this fix.

---

## Why Redis Pub/Sub for routing

The same pattern already exists between `bus.publish()` and worker
processes for `PostCreated` — Redis as the shared intermediary that every
process can reach. Pub/Sub (not Streams) is the right primitive here
specifically *because* durability is not required: a missed notification
just means the user sees the post on their next reconnect or refresh,
same as any offline user under Fanout-on-Write. There's no need to retry
or persist a "user wasn't listening" event.

```
realtime_consumer (on any worker)
│
└─► redis.publish("ws:notify:alice", json)
│
Worker A subscribes to "ws:notify:alice"
(because alice is connected to Worker A)
│
Finds alice in its local connection dict
Sends NEW_POST over her WebSocket
```

Think of it as a hotel intercom: each floor (worker) manages its own
rooms (connections), but every floor shares the same switchboard (Redis)
to route a call to the right room regardless of which floor placed it.

---

## Design decisions

### Per-user channels, not a single broadcast channel

Publishing to `ws:notify:{user_id}` rather than one shared channel means
each worker only subscribes to channels for users it actually holds —
Worker A doesn't receive (and discard) every notification in the system,
only the ones addressed to its own connected users. This keeps the
Pub/Sub traffic proportional to local connection count, not global post
volume.

### The `ws:_keepalive` channel

`redis-py`'s `pubsub.listen()` is a generator that must have at least one
active subscription to avoid stalling. If every locally-connected user
disconnects, the subscription set could briefly become empty before a new
user connects and re-subscribes. Subscribing to a dummy `ws:_keepalive`
channel at startup — and never unsubscribing — guarantees the listener
loop stays alive for the life of the process, independent of how many
real users are connected at any moment.

### `is_online()` is now explicitly local-only

Previously (in the single-worker world) `is_online()` implicitly answered
"is this user reachable" — true because there was only one worker to check.
That's no longer true. Rather than pay for a global presence check (a
Redis lookup per follower on every post, e.g. `GET user:online:{id}`),
`realtime_consumer` publishes to every follower's channel unconditionally.
If nobody is subscribed — the user is offline everywhere — Redis discards
the message with no error and no retry needed. `is_online()` is kept only
for the debug panel, to show which users are connected to the specific
worker rendering that panel — it must not be used to gate delivery
elsewhere.

### `REALTIME_SKIP` removed

This debug event existed to show "follower X is offline, notification
skipped." Since delivery is no longer conditional on a known online
check, there's nothing to report as skipped — a message is either
delivered (a worker was subscribed) or silently dropped by Redis with no
visibility into which case occurred. This is an acceptable loss of debug
granularity for the correctness gain.

---

## What was built

### Modified files
```
backend/
ws_manager.py — ConnectionManager: init()/close() lifecycle, dedicated
Pub/Sub client, _listen() background task, per-user
channel subscribe/unsubscribe on connect/disconnect
consumers.py — realtime_consumer: publishes to all followers
unconditionally; is_online() used for debug context only
app.py — lifespan: manager.init(REDIS_URL) before accepting
connections, manager.close() on shutdown
```

### Unchanged

`event_bus.py`, `db.py`, `cache.py`, `auth.py`, `fanout_consumer`, and all
frontend files require zero changes — this milestone only touches the
notification delivery path, not fanout or the event bus itself.

---

## Request flow comparison

### Before (Milestone 2 — direct send, single-worker only)

```
realtime_consumer
│
└─► manager.send(user_id, data)
│
└─► local dict lookup → ws.send_json()
(works only if user_id's socket is in THIS process)
```

### After (Milestone 3 — Redis Pub/Sub routing)

```
realtime_consumer (any worker)
│
└─► redis.publish("ws:notify:{user_id}", json)
│
every worker's _listen() task receives the message
(all subscribed to ws:notify:* for their locally-held users)
│
only the worker with a local dict entry for user_id
finds the socket and forwards it — others no-op silently
```

---

## Verification

- Single worker: post → author's own timeline updates without a reload,
  all followers receive `NEW_POST` immediately
- Two workers (ports 8001 / 8002), two browser sessions on different
  frontend ports connected to different backends: posting from a user on
  worker A correctly notifies a follower connected to worker B
- `inspect_stream.py` confirms the event bus itself is healthy throughout:
  `XLEN` increments per post, pending list empties after processing, `lag`
  returns to 0 — ruling out event-bus issues as a cause of any earlier
  delivery failures observed during this milestone's development

---

## Known limitations

### `SystemBroadcaster` (debug panel) is still per-worker

`/ws/events` clients only see events broadcast by the worker that
happened to process a given post. In multi-worker setups, a `PostCreated`
event is consumed by exactly one worker (Redis Streams consumer groups
deliver to one consumer per message), so only clients connected to *that*
worker's `/ws/events` endpoint see the corresponding `FANOUT_START`,
`FANOUT_WRITE`, `REALTIME_START`, `REALTIME_SEND` entries. Single-worker
setups appear unaffected only because there's nothing to partition.

This is a debug/observability tool, not part of the user-facing feed
path, so it's documented rather than fixed here. The fix would follow the
exact same pattern as `ConnectionManager` — a `ws:debug-events` Pub/Sub
channel that every worker subscribes to and rebroadcasts locally — but
that work is deferred rather than bundled into this milestone.

---

## Next milestone — Milestone 4: Separate consumer process

**Problem:** `fanout_consumer` and `realtime_consumer` run inside the same
process as the HTTP server, competing with API request handling for the
same asyncio event loop. A large fanout (a user with thousands of
followers) can introduce latency spikes on unrelated concurrent API calls.

**Solution:** Move event consumption into a separate background worker
process. The HTTP process only publishes to Redis Streams; a dedicated
worker process (or several) runs `XREADGROUP` and executes the consumers
independently.

**What it unlocks:** Independent scaling of the API layer and the
processing layer — e.g. 2 HTTP workers and 1 fanout worker, tuned
according to which one is actually the bottleneck. This is the point
where the system starts to resemble a genuine service-oriented
architecture rather than a monolith with background tasks.