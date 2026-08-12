# Architectural Review & Evolution Roadmap

## FanoutFeed — Post-Scaling-Stage-1 Review

---

## 1. Current System Summary

The system is a single-process FastAPI application backed by PostgreSQL and Redis, implementing
Fanout-on-Write for feed delivery and WebSockets for real-time notification.

When a user creates a post, the flow is:

```
POST /posts
  │
  ├── 1. Persist post → PostgreSQL
  ├── 2. Broadcast POST_CREATED → SystemBroadcaster (debug panel)
  └── 3. Publish "PostCreated" → Redis Pub/Sub
                │
                ▼
         listen() background task
                │
          ┌─────┴─────┐
          ▼             ▼
  fanout_consumer    realtime_consumer
  (sequential first)  (runs after fanout)
          │             │
          ▼             ▼
  Writes post ID    Sends NEW_POST
  to each follower's  to online followers
  Redis sorted set    via WebSocket
```

**Timeline reads** are a two-step operation: fetch post IDs from Redis (sorted set, newest-first),
then resolve full post bodies from PostgreSQL in a single `ANY($1::text[])` query.

**Infrastructure**: PostgreSQL (Supabase), Redis (cloud-hosted), single uvicorn process,
React frontend connecting via HTTP and WebSocket.

---

## 2. Strengths of the Current Design

**Correct separation between source of truth and delivery mechanism.**
PostgreSQL is the durable record. Redis is the fast-access read layer. WebSocket is a
notification channel only — not a data pipe. This distinction means that if the WebSocket
push fails or is missed, the user still receives the post on their next timeline fetch.
Most systems that go wrong at scale conflate these responsibilities.

**Fanout before notification — no race condition.**
Because consumers run sequentially (fanout first, realtime second), the timeline sorted set
is written before the WebSocket push fires. A client that immediately calls `GET /timeline`
after receiving `NEW_POST` will always find the post there. This is a subtle but important
correctness guarantee.

**Clean event contract.**
The `PostCreated` event payload is self-contained: `post_id`, `author_id`, `author_name`,
`created_at`. Consumers ask the payload for what they need; they don't re-query Postgres
to reconstruct data they already have. This keeps consumers fast and independent.

**Graceful degradation for offline users.**
Fanout writes to every follower's timeline regardless of online status. Offline users see
the post on reconnect without any special handling. This is the core value proposition of
Fanout-on-Write that a pure push model can't offer.

**Extensibility via the event bus.**
Adding a new consumer (email digests, push notifications, analytics) requires zero changes
to the post creation path. Subscribe a new handler to `PostCreated` and it participates in
the flow. This is the architectural pattern that lets teams work independently on features.

**Read efficiency.**
The two-step timeline read (Redis sorted set → PostgreSQL batch fetch) is effectively
constant-time regardless of how many posts exist. Redis `ZREVRANGE` is O(log N + K) where
K is the number of results. The Postgres query is one indexed scan on primary keys. This
pattern scales well into the hundreds of thousands of posts.

---

## 3. Weaknesses and Acceptable Technical Debt

These are genuine gaps, but they are the *right* gaps to have at this stage. Fixing them
now would be premature and would add complexity that obscures the architectural patterns
you're learning.

**No authentication.**
`author_id` is a query parameter. Any client can post as any user. Every subsequent
feature that is user-specific (bookmarks, DMs, moderation, notifications) requires a
real identity layer. This is the most critical gap before real users touch the system.

**Redis Pub/Sub has no durability.**
Pub/Sub is fire-and-forget. If the `listen()` background task isn't running when
`bus.publish()` is called — a startup race condition, a transient crash, a task
cancellation during reload — the `PostCreated` event is silently dropped. The post exists
in PostgreSQL but fanout never happens. At 800–1000 users this is unlikely and recoverable
(users just don't see the post until they refresh), but it's a correctness gap.

**No fanout failure recovery.**
If `fanout_consumer` crashes after writing to 50 of 200 followers, there's no retry.
The remaining 150 followers miss the post. Without a dead-letter mechanism, this failure
is invisible. Acceptable now because exceptions are unlikely and the consequence is mild.

**Single process — no horizontal scaling.**
All WebSocket connections live in one process's memory. The `ConnectionManager` dict
is local. If you add a second uvicorn worker, it has its own `ConnectionManager` with
no knowledge of connections on worker 1. A user connected to worker 1 will not receive
WebSocket pushes from an event processed by worker 2. This is the primary structural
limit of the current design.

**Offset-based timeline pagination.**
`GET /timeline?offset=50` has a drift problem: if 3 new posts arrive while the user is
reading page 1, page 2's offset will skip 3 posts or show 3 duplicates from page 1.
This is the classic offset pagination inconsistency. Acceptable in dev; broken in production.

**Unbounded fanout in the consumer loop.**
Fanout iterates over followers sequentially with `await` on each Redis write. For a user
with 1,000 followers this is ~1,000 round-trips to Redis. Even at 1ms per trip, that's a
full second of the event loop blocked on one fanout. At the current user count, no
individual will have enough followers to make this visible. At 5,000 followers it becomes
a real problem.

**No timeline backfill on follow.**
When Dave follows Alice today, he does not see any of Alice's posts from last week. New
follows only affect future fanout writes. This is a known Fanout-on-Write limitation.
The fix — backfilling the new follower's timeline on follow action — adds complexity and
is correctly deferred for now.

**No rate limiting.**
A single client can hammer `POST /posts` in a loop. No consequence. Fine for a controlled
prototype; a pre-production concern before any public access.

---

## 4. Realistic Scaling Limits of This Architecture

**Where the architecture holds comfortably:**
- Up to ~1,500 registered users, ~200–300 concurrent users
- Up to ~20 posts/second sustained
- Follower counts below ~2,000 per user (fanout stays under 50–100ms per event)
- Single-region, single-server deployment

**Where it begins to degrade:**
- Above ~300 concurrent WebSocket connections on a budget VPS, memory pressure from the
  single process becomes visible
- Above ~500 concurrent HTTP requests, the single asyncio event loop starts queuing I/O
- Any user with 5,000+ followers will cause fanout to visibly block other events on the bus

**Where it breaks structurally:**
- Adding a second server/process: WebSocket routing fails (connections aren't shared)
- Redis Pub/Sub message drop during startup or worker reload: fanout silently misses
- Sustained post rate above ~50/second: the sequential fanout loop creates a backpressure
  queue in the `listen()` task, making notification latency unpredictable

**The honest ceiling of this architecture without structural changes:**
Around 1,000–2,000 users and a sustained throughput of ~30 posts/second. Beyond that,
the next milestone (described below) becomes necessary, not optional.

---

## 5. The Next Architectural Milestone

**Add authentication (JWT-based identity).**

Every feature from this point forward is user-specific. Rate limiting requires knowing who
is calling. Bookmarks, DMs, moderation, and notifications require a verified identity.
More subtly, `author_id` as a query param makes every route trivially spoofable —
which means any load or security testing you do now is testing against a system that
doesn't represent reality.

Authentication is also architecturally foundational: once you have a verified `user_id`
in a request context, routes become simpler (no `?author_id=` param), consumers can
trust the identity in the event payload, and the WebSocket connection can be tied to a
real authenticated user.

This is not a scaling milestone. It's a correctness milestone. Do it before adding any
more features, because retrofitting auth into an existing feature set is expensive and
error-prone.

After auth, the next *scaling* milestone is migrating the event bus from Redis Pub/Sub
to Redis Streams — which gives you durability, replay, and at-least-once delivery.

---

## 6. Evolution Roadmap

Each milestone identifies the problem it solves, why it becomes necessary at that
specific point, and what it unlocks for the next stage.

---

### Milestone 1 — Authentication (JWT + password hashing)

**Problem:** No verified identity. `author_id` is a URL parameter anyone can forge.

**Why now:** Every feature from here is user-specific. Auth cannot be bolted on later
without rewriting every route, consumer, and WebSocket handler. It's cheaper to add now
than after 5 more features exist.

**What it introduces:**
- `POST /auth/register`, `POST /auth/login` → issues a JWT
- Every route reads `user_id` from the token, not from query params
- WebSocket handshake validates the token before accepting the connection
- Passwords hashed with bcrypt

**What it unlocks:** Rate limiting, DMs, bookmarks, moderation — anything user-specific.

---

### Milestone 2 — Event bus durability (Redis Pub/Sub → Redis Streams)

**Problem:** Pub/Sub is fire-and-forget. A message published during a worker restart
or a `listen()` task crash is silently dropped. There is no retry, no audit trail,
no way to replay missed events.

**Why now:** Before real users experience silent timeline gaps, you want at-least-once
delivery semantics on the event bus. Redis Streams gives you: message persistence,
consumer group acknowledgement (a message isn't removed until the consumer ACKs it),
retry on failure, and the ability to replay from any point in the stream.

**What changes:** `event_bus.py` replaces `PUBLISH/SUBSCRIBE` with `XADD/XREADGROUP`.
The `fanout_consumer` and `realtime_consumer` ACK messages after completion. Failed
messages get retried by the `XAUTOCLAIM` mechanism.

**What it unlocks:** The foundation for reliable background job processing. Also a
prerequisite for Milestone 4, where consumers move to a separate process.

---

### Milestone 3 — WebSocket cross-worker routing (Redis Pub/Sub per-user channels)

**Problem:** The `ConnectionManager` dict is local to a single process. Adding a second
uvicorn worker means half the WebSocket connections become unreachable by the event
consumer. Horizontal scaling of the HTTP layer is impossible without this.

**Why now:** Once you're comfortable with 2+ uvicorn workers (for CPU-bound or
memory-bound headroom), this becomes the structural blocker.

**What changes:** When a user connects, their `user_id` is registered in Redis with the
worker's identity. The `realtime_consumer` publishes to a Redis channel `ws:notify:{user_id}`
instead of calling `manager.send()` directly. Each worker subscribes to the channels of
its locally-connected users and forwards messages to the right WebSocket.

**The analogy:** Like a hotel intercom system — each floor (worker) handles its own rooms
(connections), but they all share the same switchboard (Redis) to route calls across floors.

**What it unlocks:** Running 2–4 uvicorn workers behind Nginx, which multiplies your
request-handling capacity without changing the application logic.

---

### Milestone 4 — Separate consumer process (background worker)

**Problem:** Fanout runs inside the HTTP server process, competing with API requests for
the asyncio event loop. A heavy fanout (user with 2,000 followers) introduces latency
spikes on unrelated API calls made by other users simultaneously.

**Why now:** When POST /posts response times start correlating with fanout size.

**What changes:** The HTTP process only publishes to Redis Streams. A separate
background worker process (using ARQ, SAQ, or a custom asyncio stream reader) consumes
from the stream and runs fanout and realtime consumers independently. The two processes
share only the Redis stream as the interface between them.

**What it unlocks:** Independent scaling of the API layer and the processing layer.
You can run 2 HTTP workers and 1 fanout worker, or 1 HTTP worker and 3 fanout workers,
based on actual bottlenecks. This is the moment the system starts resembling a real
service-oriented architecture.

---

### Milestone 5 — Post cache (Redis hash per post)

**Problem:** Every timeline read fetches post bodies from PostgreSQL with
`WHERE id = ANY(...)`. At low traffic this is fast. As reads scale, the same popular
post gets fetched from Postgres hundreds of times per second across all timelines it
appears in.

**Why now:** When Postgres read IOPS for timeline fetches becomes visible in monitoring.

**What changes:** On post creation, serialize the full post object into a Redis hash
(`post:{post_id}`). The timeline read path checks Redis first; only falls back to
Postgres on a cache miss. TTL on post hashes (e.g., 24–48 hours) keeps memory bounded.

**What it unlocks:** Postgres read traffic for timeline fetches drops dramatically.
Postgres then only handles writes, follow/unfollow, and occasional cache-miss reads.
This is the read-through cache pattern used by every large social platform.

---

### Milestone 6 — Cursor-based timeline pagination

**Problem:** Offset-based pagination (`?offset=50`) is broken in a live feed.
New posts arrive while the user is reading, shifting every subsequent offset by N.
Page 2 will skip N posts or repeat N posts from page 1 depending on direction.

**Why now:** When user retention data shows users abandoning the feed mid-scroll, or
when duplicate posts in the timeline become a reported bug.

**What changes:** The timeline API accepts a `cursor` (the score/timestamp of the last
seen post) instead of an offset. `ZREVRANGEBYSCORE` returns posts with scores less than
the cursor, regardless of new inserts above it. The client stores the cursor after each
page and sends it with the next request.

**What it unlocks:** Stable, consistent feed reads. Also a prerequisite for infinite
scroll UX and mobile clients that paginate aggressively.

---

### Milestone 7 — Hybrid fanout (Fanout-on-Write + Fanout-on-Read)

**Problem:** Fanout-on-Write doesn't scale to high-follower accounts. If a user has
100,000 followers, a single post triggers 100,000 Redis writes. This blocks the consumer
for seconds and creates latency spikes for all other events in the stream.

**Why now:** When your monitoring shows that the top 1% of users by follower count are
responsible for 80%+ of fanout processing time. This is the classic "celebrity problem."

**What changes:** Introduce a follower count threshold (e.g., 10,000). Users above the
threshold become "heavy fanout" accounts. Their posts are NOT written to follower timelines
at write time. Instead, timeline reads check a secondary source ("heavy fanout posts from
accounts I follow") and merge it with the pre-written timeline at read time.
Accounts below the threshold continue to use Fanout-on-Write unchanged.

**The trade-off:** Reads become slightly more expensive for users who follow celebrities.
Writes become dramatically cheaper. This is exactly the model Twitter/X uses — ordinary
accounts fanout on write, verified high-follower accounts fanout on read.

**What it unlocks:** The system can now handle accounts with millions of followers
without structural changes to the write path.

---

### Milestone 8 — Persistent notification store

**Problem:** WebSocket notifications are ephemeral. If a user is offline or their
client misses the push, the notification is gone. Users expect to see "you had 5
notifications while you were away" — but there's currently no record of them.

**Why now:** When users start asking "why didn't I see the notification?" or when you
add notification types beyond feed posts (likes, replies, follows).

**What changes:** Add a `notifications` table in PostgreSQL. Every event that generates
a notification (new post from followed user, new follower, mention) writes a row.
WebSocket push becomes a hint ("you have unread notifications") not the notification
itself. On login, `GET /notifications` returns unread items. Mark-as-read updates the
table.

**What it unlocks:** Notification badges, unread counts, notification preferences
(mute user, mute type), and email/push digests based on the same data.

---

### Milestone 9 — Rate limiting (Redis sliding window)

**Problem:** No limits on post creation, follow actions, or API calls. A single
authenticated user (or a bot) can exhaust Redis write capacity or flood timelines.

**Why now:** Before any public-facing access, or when you first observe automated
abuse patterns in logs.

**What changes:** A Redis-based sliding window counter per `(user_id, action)`.
`POST /posts` checks "has this user posted more than N times in the last 60 seconds?"
If yes, return `429 Too Many Requests`. The counter key has a TTL matching the window.
No external service needed — Redis `INCR` + `EXPIRE` is the standard implementation.

**What it unlocks:** Protection against abuse, foundation for tiered usage (free users
vs. paid users with higher limits), and data for identifying bot accounts.

---

### Milestone 10 — Read replicas (PostgreSQL horizontal read scaling)

**Problem:** A single Postgres instance handles all reads and writes. As users grow,
timeline cache misses, user lookups, and follow graph queries start competing with
post writes for Postgres I/O, CPU, and connection slots.

**Why now:** When Postgres CPU or I/O utilization stays above 60–70% during peak hours,
or when write latency starts increasing due to read pressure.

**What changes:** Provision a Postgres read replica. Route all read queries (timeline
fallback, user lookups, following lists) to the replica. All writes go to the primary.
Connection pool in `db.py` splits into a write pool and a read pool with separate DSNs.

**The trade-off:** Replica lag (typically <100ms) means reads may not immediately
reflect the latest write. For timeline reads this is acceptable. For "did my post save?"
confirmations, read from primary.

**What it unlocks:** Independently scalable read and write capacity. Multiple read
replicas can be added without changing application logic — just add more DSNs to the
read pool.

---

### Milestone 11 — Full-text search (Elasticsearch or Postgres FTS)

**Problem:** Users can only see posts in their feed. There's no way to discover posts
or users outside their follow graph. No hashtag support, no keyword search.

**Why now:** When engagement metrics show users spending all their time in the feed and
no time discovering new content — a sign the product is becoming a closed loop.

**What changes:** Either add `pg_trgm` full-text search indexes to the `posts` table
(sufficient for hundreds of thousands of posts) or introduce Elasticsearch for more
sophisticated ranking and faceted search. A `SearchConsumer` subscribes to `PostCreated`
and indexes the post content. `GET /search?q=...` queries the search layer.

**What it unlocks:** Hashtags, mentions, user discovery, trending topics. Also enables
content moderation tooling (search for prohibited terms).

---

### Milestone 12 — Media support (object storage + CDN)

**Problem:** Posts are text-only. Users want to attach images and video. Storing binary
data in PostgreSQL is expensive and slow; serving it through FastAPI is worse.

**Why now:** When text-only posts become a product limitation rather than a scope decision.

**What changes:** Introduce S3 (or R2/Backblaze B2) for object storage. The post
creation flow changes: client requests a presigned upload URL, uploads directly to S3
(not through FastAPI), then calls `POST /posts` with the media URL. A CDN sits in front
of S3 for delivery. Post schema gains a `media_urls` array.

**What it unlocks:** Rich content. Also forces a decision about content moderation
infrastructure, since user-uploaded media requires scanning.

---

### Milestone 13 — Event bus upgrade (Redis Streams → Kafka)

**Problem:** Redis Streams works well for one stream with a few consumer groups on a
single node. It becomes limiting when: you need cross-datacenter event replication,
you have 10+ distinct consumer types with different throughput requirements, you need
long-term event replay (days, not hours), or Redis memory becomes the bottleneck for
event volume.

**Why now:** When you have more than 3–4 distinct consumer types, need to replay
events for a new service that wasn't running historically, or when Redis Stream memory
usage exceeds 20–30% of your Redis instance.

**What changes:** Replace `XADD/XREADGROUP` with Kafka topics. `PostCreated` becomes
a Kafka topic. Each consumer (fanout, realtime, notification, search index) is an
independent consumer group with its own offset. Kafka retains events for 7–30 days.
New services can replay from the beginning of the log.

**The trade-off:** Kafka is significantly more operationally complex to run than Redis.
Use managed Kafka (Confluent Cloud, Upstash Kafka, MSK) to avoid the operational burden.

**What it unlocks:** True decoupling between producers and consumers. Teams can add new
consumers (analytics, ML feature pipelines, audit logs) without any coordination with
the post service team. This is the architecture that enables independent product teams
to move at different speeds.

---

### Milestone 14 — Service extraction (Post Service, Timeline Service, Notification Service)

**Problem:** A single FastAPI application handles posts, timelines, notifications,
search, and user management. Different parts of the system have different scaling
requirements (timeline reads are 100x more frequent than post writes) but share the
same process and deployment.

**Why now:** When different components need different scaling strategies, when teams
need to deploy independently without risking the whole system, or when a single
component's failure (e.g., search) is taking down unrelated features (e.g., feed).

**What changes:** Extract distinct services behind an API gateway or internal service
mesh. Each service owns its own database tables. They communicate via the event bus,
not via shared database access. This is the point where Kafka's independence across
consumer groups pays off — each service is just another consumer group.

**The trade-off:** Distributed systems complexity. Network calls replace function calls.
Failures become partial. Observability (tracing, structured logging, metrics) becomes
non-optional. Do not do this prematurely — a monolith that knows its boundaries is
easier to operate than premature microservices.

**What it unlocks:** Independent scaling per service, independent deployment, team
autonomy, and the architectural foundation of modern social platforms.

---

### Milestone 15 — Global distribution (multi-region, edge caching)

**Problem:** All infrastructure is in one region. Users in other continents experience
latency on every API call and WebSocket message. A single-region outage takes down the
entire product.

**Why now:** When geographic user distribution shows significant portions of traffic
coming from regions more than 100ms from your origin server, or when uptime SLAs
require 99.99% availability (which a single region cannot deliver).

**What changes:** Multi-region PostgreSQL (primary in one region, read replicas in
others). Redis clusters per region for local timeline reads. CDN for all static assets
and public post data. WebSocket connections terminate at the nearest edge point of
presence. Writes always route to the primary region; reads route to the nearest replica.

**What it unlocks:** Global product viability, sub-50ms read latency for all users,
and the operational resilience of a production social platform.

---

## Recurring Architectural Patterns

Three shapes have now recurred often enough across independent milestones
to be worth naming as this prototype's architectural vocabulary, rather
than re-deriving each time a new subsystem needs one of them.

**1. Durable source of truth, with a best-effort optimization layered on top.**
The optimization is always disposable — safe to lose, rebuildable or
simply absent without corrupting correctness.

```
M2  Redis Streams (durable, at-least-once)   ←→  Redis Pub/Sub (fire-and-forget)
M5  Postgres (source of truth)               ←→  Redis post cache (disposable)
M8  Postgres notification rows (durable)     ←→  WebSocket push (best-effort hint)
```

The test for which side a piece of state belongs on: can it be
reconstructed from something else if lost? A timeline entry can (M0.5's
Postgres/Redis split). A notification's read/unread bit cannot — nothing
else in the system records whether a specific recipient acknowledged a
specific event, which is exactly why M8's notification rows sit on the
durable side and a live push sits on the disposable side.

**2. Producer → bus → consumers.**
The event bus is the sole integration point between subsystems; producers
never know which or how many consumers exist.

```
PostCreated    →  fanout_consumer, realtime_consumer, on_post_created (M8)
FollowCreated  →  on_follow_created (M8)
```

M8 confirmed this pattern's extensibility claim under real use:
`FollowCreated` is a brand-new event type, and `event_bus.py` required
zero code changes to support it — `_create_consumer_groups()` already
iterates whatever handlers are registered.

**3. Mechanism vs. policy.**
Infrastructure provides a domain-agnostic capability; product decisions
sit in a separate layer on top, documented as explicit ADRs rather than
buried in the mechanism.

```
PubSubRouter (M7.5)  →  ConnectionManager, SystemBroadcaster (policy)
db.py (M8)            →  app/notifications.py (policy: identity, refollow semantics)
```

Any future subsystem needing both correctness and speed, or needing to
integrate with the rest of the system without tight coupling, should
reach for one of these three shapes by default rather than inventing a
fourth.

---

## Summary View

```
Current → M1 → M2 → M3 → M4
Single process, in-memory WS  │
no auth, Pub/Sub bus           │
                               │
M1: Authentication (identity layer)
M2: Redis Streams (event durability)
M3: Cross-worker WS routing (horizontal scale)
M4: Separate worker process (decoupled processing)

         M4 → M5 → M6 → M7
                          │
M5: Post cache (read offload)
M6: Cursor pagination (feed correctness)
M7: Hybrid fanout (celebrity problem)

         M7 → M8 → M9 → M10
                           │
M8: Notification store (persistent notifications)
M9: Rate limiting (abuse protection)
M10: Read replicas (DB read scaling)

         M10 → M11 → M12 → M13
                             │
M11: Full-text search (content discovery)
M12: Media / object storage (rich content)
M13: Kafka (event bus at scale)

         M13 → M14 → M15
M14: Service extraction (team autonomy)
M15: Multi-region (global distribution)
```

Each milestone builds on the one before it. None of them require throwing away what
was built previously — they are substitutions or additions at specific seams.
That is the mark of an architecture with the right boundaries from the start.
```