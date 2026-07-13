# Milestone 0.5 — Infrastructure Migration (PostgreSQL + Redis)

## Status

Completed

---

## Goal

Replace in-memory persistence and event delivery with durable,
process-independent infrastructure — without changing the fanout
pattern, the consumer contract, or the frontend API.

---

## Previous Architecture

Milestone 0 (Foundation): `InMemoryEventBus` + plain Python dicts
(`USERS`, `POSTS`, `TIMELINES`, `FOLLOWS`) as the entire persistence
and messaging layer. Everything lived in one process's memory.

```
POST /posts
  → POSTS[id] = post              (dict)
  → bus.publish("PostCreated")    (in-process, synchronous)
      → fanout_consumer  → TIMELINES[follower].insert(0, post_id)
      → realtime_consumer → WS push to online followers
```

---

## Limitation

- Server restart wipes every post, follow relationship, and timeline
- `TIMELINES[user]` is a plain Python list — inserts/reads are fine
  at toy scale but don't reflect how a real timeline store behaves
- The event bus lives entirely in one process — no way to decouple
  "something happened" from "something is handling it"
- Nothing here can survive a crash mid-request

None of this blocks *learning the fanout pattern*, but it blocks
everything that comes after — I can't build auth, rate limiting,
or notifications on top of state that vanishes on restart.

---

## Why This Solution?

**PostgreSQL** as the durable source of truth (users, posts, follows) —
survives restarts, gives real query semantics, and is the same role
Postgres plays in every architecture on the roadmap going forward.

**Redis Sorted Sets** for timelines instead of Postgres directly —
timeline reads are the hottest path in a Fanout-on-Write system, and
`ZADD`/`ZREVRANGE` give O(log n) inserts and range queries with
natural newest-first ordering, which a relational table would need an
index + `ORDER BY` + `LIMIT` to approximate at higher cost per read.

**Redis Pub/Sub** replacing `InMemoryEventBus` — same subscribe/publish
mental model I already had, but the publisher and the consumer no
longer have to be the same process to talk to each other. This is a
prerequisite for ever running more than one worker.

**Docker was ruled out** (Windows environment constraints), so
Supabase (Postgres free tier) and a managed Redis free tier stand in
for local containers — same architectural role, zero local infra to
maintain.

**Trade-off accepted knowingly:** Redis Pub/Sub is *not* durable —
a message published while nothing is subscribed is lost forever, no
retry, no audit trail. This is fine at current scale and is explicitly
the problem Milestone 2 (Redis Streams) exists to solve. Don't let
"it's not perfect" block shipping this step — durability without
persistence first would be solving the wrong problem.

---

## Changes Made

- `store.py` deleted entirely
- `db.py` added — PostgreSQL connection pool + queries for
  users/posts/follows (source of truth)
- `cache.py` added — Redis sorted sets for timelines (fast reads,
  O(log n) insert/range, naturally capped)
- `event_bus.py` rewritten: `InMemoryEventBus` → Redis Pub/Sub, using
  **two** Redis connections — one for `publish()`, one dedicated to
  the subscribe loop (a Redis connection in pub/sub mode can only
  issue subscription commands, nothing else)
- `listen()` is now a background `asyncio` task started once in
  `lifespan`, not awaited per-request
- `POST /posts` behavior change: publishes to Redis and returns
  immediately — fanout and realtime consumers now run asynchronously
  off the bus instead of inline before the HTTP response
- `consumers.py` — **logic unchanged**, only import paths changed
  (proof the consumer contract was never coupled to storage backend)
- `ws_manager.py` — unchanged; single-process WS is still sufficient
  at this scale
- Frontend — unchanged; API contract identical

---

## Request Flow

```
POST /posts
  │
  ├── 1. INSERT post          → PostgreSQL   (db.py)
  ├── 2. broadcast POST_CREATED → SystemBroadcaster (debug panel)
  ├── 3. event_bus.publish("PostCreated") → Redis PUBLISH
  └── 4. return {post_id}     ← response returns HERE

──────────────────────────────────────────────────────────
Background task (started once in lifespan):

Redis SUBSCRIBE ("PostCreated")
  │
  ▼  on message
  ├── fanout_consumer   → ZADD post_id into each follower's
  │                        Redis sorted set timeline
  └── realtime_consumer → push NEW_POST to online followers
                           (unchanged logic)
```

---

## New Components

| Component | Purpose |
|-----------|---------|
| `db.py` | PostgreSQL connection pool + queries — durable source of truth |
| `cache.py` | Redis sorted sets — fast timeline reads/writes |
| `event_bus.py` (Redis-backed) | Publish/subscribe decoupled from a single process, via two connections |

---

## Benefits

- Data survives restarts and crashes
- Timeline operations are O(log n) instead of Python-list operations
- `POST /posts` no longer blocks on fanout completion — faster
  perceived response time
- Consumer contract untouched — validates that the Milestone 0
  event-bus abstraction was designed correctly the first time

---

## Remaining Limitations

- Redis Pub/Sub is still fire-and-forget: a `PostCreated` event
  published while `listen()` isn't running (startup race, crash,
  reload) is silently dropped — post exists in Postgres, but fanout
  never happens until the next unrelated event or manual fix
- `author_id` is still a spoofable query parameter — no real identity
- Single process — WebSocket connections are still local to one worker
- No retry or dead-letter mechanism for a fanout that fails partway

---

## Ceiling

Infra-wise this comfortably handles the same range as before
(hundreds of concurrent users on a single VPS) — this milestone didn't
change the scaling ceiling, it fixed *correctness under restart*.
The binding constraint going forward is still the lack of identity,
not throughput.

---

## Next Milestone

**Milestone 1 — Authentication (JWT)**

Problem it solves: `author_id` can be forged by any caller — no route
can trust who's making the request.

Why now: every feature after this (rate limiting, bookmarks, DMs,
notifications) needs a verified `user_id`. Doing it now, on a system
with three routes and two consumers, is far cheaper than retrofitting
it after five more features exist.