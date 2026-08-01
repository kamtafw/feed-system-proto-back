# Milestone 4 — Separate Consumer Process

## FanoutFeed · `milestone-4-worker-process`

---

## Goal

Move event processing (`fanout_consumer`, `realtime_consumer`) out of the
HTTP server process into a standalone worker process, so the two can be
scaled, restarted, and reasoned about independently.

---

## Previous architecture and its limitation

Since Milestone 2, `bus.listen()` ran as an `asyncio.Task` inside the
same process as the FastAPI HTTP server, started from the `lifespan`
context manager. Every HTTP worker process was, incidentally, also a
Redis Streams consumer registered in `ff_consumers`.

This was invisible with a single HTTP worker. It became concrete evidence
during Milestone 3 testing: `XINFO GROUPS` showed the `consumers` count
climbing on every `uvicorn --reload` cycle (23 → 25 → 28 across
successive checks). Each reload spawned a new process, which registered
a new `worker-{pid}` consumer name — proof that request-handling
capacity and event-processing capacity were coupled to the same process
lifecycle, even though nothing about their actual workloads requires
that.

The two have genuinely independent bottlenecks:

- HTTP layer: scales with concurrent request volume
- Consumer layer: scales with fanout size (a user with thousands of
  followers) and event throughput

Coupling them means you can't add HTTP capacity without also adding
consumer capacity, and vice versa — the wrong lever moves every time you
pull it.

---

## Why a standalone process (not ARQ/SAQ)

The project's roadmap originally listed ARQ, SAQ, or a custom asyncio
reader as candidates. ARQ and SAQ are job queues — one job, one worker,
retry semantics scoped to a single unit of work.

`PostCreated` isn't a single job. It requires two independent handlers
(fanout, realtime) to both run in a fixed order, with one ACK gating
both — the exact guarantee `RedisStreamsEventBus` was built for in
Milestone 2, specifically to prevent the read-your-own-writes race
between the timeline write and the WebSocket push. Splitting into a job
queue would mean enqueuing fanout and realtime as separate jobs and
losing that guarantee entirely — a regression, not a sidegrade.

So the correct move is not a new library — it's just moving *where*
`RedisStreamsEventBus.listen()` runs. The consumers, the event bus, and
the sequential-ACK contract are completely unchanged.

---

## Design decisions

### `worker.py` reuses every existing component unmodified

`db`, `cache`, `bus`, `manager`, `fanout_consumer`, `realtime_consumer`
are all the same modules and singletons used by the HTTP process. The
only new code is the process entry point itself — initialize the same
dependencies, subscribe the same handlers, call `listen()`. This keeps
the seam exactly where it should be: at the process boundary, not inside
any component's logic.

### The worker also initializes `ConnectionManager`

`realtime_consumer` calls `manager.send()`, which needs a Redis client to
publish to `ws:notify:{user_id}`. The worker process calls
`manager.init()` to get that client — even though it never accepts a
real browser WebSocket, so the local connections dict and the
`_listen()` forwarding loop `ConnectionManager` also starts are unused
overhead in this process. Not a correctness issue, just a slightly
oversized dependency — a future refactor could split `ConnectionManager`
into a lean publish-only client and a separate subscribe-and-forward
component that only the HTTP process needs. Deferred; not required for
this milestone's goal.

### The HTTP process keeps `bus.init()`

`POST /posts` still calls `bus.publish()` (XADD) directly — publishing is
cheap, synchronous-from-the-caller's-perspective, and has no reason to be
routed through another process. Only *consumption* (`XREADGROUP` /
`listen()`) moved. This mirrors how Kafka producers and consumers are
typically split in production: any process can produce; only designated
consumer processes read.

---

## What was built

### New files

```
worker.py — standalone process: init dependencies, subscribe handlers,
run bus.listen() indefinitely
```

### Modified files

```
backend/
app.py — lifespan: removed bus.subscribe() calls and the
listener asyncio.Task; bus.init() retained for publish()
ws_manager.py — ConnectionManager.close(): fixed PubSub.aclose() call
(doesn't exist on this redis-py version) to close()
```

### Unchanged

`event_bus.py`, `db.py`, `cache.py`, `auth.py`, `consumers.py`, and all
frontend files — this milestone only changes which process runs the
consume loop, not any consumer logic or the event bus contract.

---

## Request flow comparison

### Before (Milestone 3 — consumer loop inside the HTTP process)


```
HTTP process
│
├─ POST /posts → bus.publish() (XADD)
│
└─ asyncio.Task: bus.listen()
│
├─ fanout_consumer
└─ realtime_consumer
```

### After (Milestone 4 — consumer loop in a separate process)

```
HTTP process
│
└─ POST /posts → bus.publish() (XADD)
│
▼
┌───────────────┐
│ Redis │
│ stream, group,│
│ Pub/Sub │
└───────────────┘
│
▼
worker.py (separate process)
│
└─ bus.listen()
│
├─ fanout_consumer
└─ realtime_consumer
```
The only connection between the two processes is Redis itself — the
stream, the consumer group, and the Pub/Sub channels. Neither process
holds a direct reference to the other.

---

## Verification

- **Both processes running:** post → author's timeline updates
  immediately, followers receive `NEW_POST`, `inspect_stream.py` shows
  `lag: 0` and empty `PENDING` after each post — matches the working
  baseline from Milestone 3.
- **Worker stopped, then post:** `POST /posts` still returns `200`.
  `XLEN` grows and `lag` in `XINFO GROUPS` climbs while the worker is
  down — this is the test that specifically proves the decoupling, since
  it could not have passed before this milestone (the old architecture
  had no way to accept a post without the same process also being the
  consumer).
- **Worker restarted after an outage:** backlog drains automatically,
  followers receive delayed `NEW_POST` notifications without
  reconnecting their WebSocket — confirms recovery is automatic and
  requires no client-side action.
- **Extended outage test (~15 min), attempting to probe Pub/Sub
  idle-disconnect risk:** inconclusive. The access token expired first
  (15-minute TTL) and produced an unrelated 401. A shorter-gap retest
  succeeded with no message loss. This risk remains a named, unconfirmed
  concern rather than something proven safe or proven broken — see Known
  Limitations.

---

## Known limitations

### Pub/Sub idle-disconnect risk on the HTTP process (unconfirmed)

`ConnectionManager`'s Pub/Sub subscription — used to route
`ws:notify:{user_id}` messages to the correct locally-held WebSocket —
runs on a long-lived Redis connection in the HTTP process. Hosted Redis
providers commonly enforce idle-connection timeouts. If that connection
were silently dropped server-side during an extended period with no
Pub/Sub traffic, `realtime_consumer`'s `PUBLISH` call would still succeed
(Redis accepts the write regardless of subscriber state) while the
message never reaches the HTTP process, because its subscription no
longer exists. Neither side would log an error — Pub/Sub delivery
failure is silent by design.

This was not reproduced during testing to date; a genuine long-outage
test was compromised by an unrelated access-token expiry. It remains an
open architectural question rather than a confirmed defect. A persistent
notification store (planned for Milestone 8) would remove this risk
entirely by making the WebSocket push a hint that a notification exists,
rather than the sole mechanism for delivering it — the same pattern
already used for the timeline itself, where WebSocket is a convenience
and PostgreSQL/Redis remain the source of truth.

### Stale consumer entries accumulate in `ff_consumers`

On Windows, `Ctrl+C` raises `KeyboardInterrupt` directly inside the
proactor event loop's I/O polling call, without resuming the currently
running task to let it reach a `finally` block. This means `worker.py`'s
cleanup path (`manager.close()`, `bus.close()`, etc.) does not run on a
non-graceful shutdown, and each restart registers a new
`worker-{os.getpid()}` name in the `ff_consumers` group that is never
explicitly removed via `XGROUP DELCONSUMER`.

This is cosmetic, not functional: `XAUTOCLAIM`'s reclaim logic operates
purely on message idle-time, regardless of whether the original consumer
name was ever cleanly deregistered. Pending-message recovery and retry
correctness are unaffected. It does mean `XINFO GROUPS`'s `consumers`
count is not a reliable indicator of currently-live consumer processes
during local development — a fact worth remembering the next time that
count looks alarming during debugging.

---

## Next milestone — Milestone 5: Post cache

**Problem:** Every timeline read fetches post bodies from PostgreSQL with
`WHERE id = ANY(...)`. At current traffic this is fast, but the same
popular post gets re-fetched from Postgres on every timeline it appears
in — redundant reads that scale with view count, not post count.

**Solution:** Serialize the full post object into a Redis hash
(`post:{post_id}`) on creation. Timeline reads check Redis first, falling
back to PostgreSQL only on a cache miss.

**What it unlocks:** PostgreSQL read traffic for timeline fetches drops
sharply, leaving it to handle writes, follow/unfollow, and occasional
cache misses — the standard read-through cache pattern.

**Planned experiment before M5:** run a second `worker.py` instance
alongside the first and observe how Redis Streams' consumer group splits
message delivery between them — direct, hands-on confirmation of the
horizontal scaling this milestone's separation was built to unlock.