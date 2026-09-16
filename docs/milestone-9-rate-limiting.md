# Milestone 9 — Rate Limiting

## FanoutFeed · `milestone-9-rate-limiting`

**Status: IMPLEMENTED AND VERIFIED.** Design was reviewed and approved
before any code was written, per this project's standing discipline
(design → implementation → automated verification → manual verification
→ closeout). This document has been updated in place with the final
implementation and verification results, the same way `milestone-8.5`
and `milestone-8.6`'s docs were.

**A note on how verification was performed, stated plainly rather than
left implicit:** automated and HTTP-level verification below was run
against a local Postgres + Redis instance provisioned specifically for
this purpose, running the actual project code — not the project's real
Supabase/Redis Cloud instances, which aren't reachable from the
environment implementation was done in. The code, schema, and test
suite are identical to what runs against the real infrastructure; only
the specific Postgres/Redis endpoint differs. Re-running
`test_rate_limit.py` and the manual checks below against the actual dev
environment before merging is still worthwhile as a final confirmation,
but is not expected to surface anything new — the rate limiter has no
dependency on Supabase- or Redis-Cloud-specific behavior.

---

## Goal

Protect `POST /posts`, `POST /me/follow/{id}`, and
`DELETE /me/follow/{id}` from being called at unbounded volume by a
single authenticated user, with correct trailing-window semantics —
not merely "some counter that resets periodically."

---

## Why now

`architecture-review.md` names this gap directly: *"A single client can
hammer* *`POST /posts`* *in a loop. No consequence."* The roadmap frames
M9 as becoming necessary "before any public-facing access, or when
automated abuse patterns are first observed in logs." Nothing about M9
depends on M8/M8.5/M8.6; it is picked up next purely because it's the
next unaddressed item on the roadmap, not because notifications created
any prerequisite.

---

## Reconnaissance summary (what the actual codebase settled before design began)

- `POST /posts` and `POST /me/follow/{id}` / `DELETE /me/follow/{id}`
  are the only realistic candidates: they're the writes behind
  `Depends(get_current_user)`, keyed by the JWT `sub`.
- `GET /timeline/{user_id}` — the highest-traffic route in the system —
  is unauthenticated and has no `user_id`-shaped key to rate-limit
  against. **Deliberately excluded from M9**, not overlooked (see
  Out of Scope).
- `worker.py` never touches HTTP and has already committed writes by
  the time it sees an event — irrelevant to enforcement placement.
- `create_post` / `add_follow` / `remove_follow` are single
  auto-committed statements with no transaction boundary to hook into.
  The rate-limit check has no natural home *inside* the write path — it
  belongs strictly *before* it.
- The app already runs (or is designed to run — M3/M7.5) behind
  multiple Uvicorn workers sharing only Redis and Postgres as common
  state. This is the actual justification for Redis here (see ADR-1).
- Existing Redis usage is already split into three independently-owned
  clients (`cache.py`, `event_bus.py`, `ws_router.py`), each matching a
  distinct responsibility. No unified key-namespace convention exists
  across them (`timeline:*`, `ws:*`, `ff:stream:*`).
- No fake-clock tooling exists anywhere in the test suite; the only
  precedent for time-dependent testing (`test_streams.py`) waits out a
  real interval with `asyncio.sleep()`.

---

## Rate-limit semantics — sliding-window log

**Definition:** for a given `(user_id, action)`, at most `N` requests
may exist with timestamps in the trailing interval `(now − W, now]`.
Each request's contribution to the count decays individually as its own
timestamp ages past `W` seconds — there is no single "window reset"
event, and no averaging or smoothing of throughput within the window.

**Boundary:** the interval is half-open, exclusive on the old end,
inclusive on the new end — `(now − W, now]`. A request exactly `W`
seconds old has fully exited the window.

**Example** (`N=5`, `W=60s`), requests at
`12:00:01, :05, :10, :20, :30`, then `12:00:45`, `12:01:01`, `12:01:02`:

| TimeCount before this requestAllowed? |                       |    |
| ------------------------------------- | --------------------- |  - |
| 12:00:01                              | 0                     | ✅ |
| 12:00:05                              | 1                     | ✅ |
| 12:00:10                              | 2                     | ✅ |
| 12:00:20                              | 3                     | ✅ |
| 12:00:30                              | 4                     | ✅ |
| 12:00:45                              | 5                     | ❌ |
| 12:01:01                              | 4 (12:00:01 aged out) | ✅ |
| 12:01:02                              | 5                     | ❌ |

Capacity refills one entry at a time as each individual timestamp ages
out — never all-at-once. Full burst up to `N` is explicitly allowed;
this is a hard ceiling on trailing-window volume, not a token-bucket
rate-smoothing mechanism, and no smoothing behavior should be
introduced later without a separate, explicit decision.

**What consumes a slot:** every authenticated request that reaches the
check, before any route-specific business validation runs (empty
content, self-follow, nonexistent target, already-following). The
limiter has no knowledge of route semantics — consistent with the
mechanism/policy separation already established by `PubSubRouter` and
`db.py`/`notifications.py`. Requests that fail *authentication* consume
nothing, since no reliable `user_id` exists to key by at that point —
this falls out of running the check after `get_current_user`, not from
a special case in the limiter itself.

---

## Policies

| ActionBucketRoutes |                   |                                                  |
| ------------------ | ----------------- | ------------------------------------------------ |
| `post_create`      | own bucket        | `POST /posts`                                    |
| `follow_action`    | **shared** bucket | `POST /me/follow/{id}`, `DELETE /me/follow/{id}` |

Key identity: `(user_id, action)`.

`follow_action` is intentionally a single shared bucket across follow
*and* unfollow. The abuse surface being controlled is rapid mutation of
the follow graph, not "how many follows" or "how many unfollows"
independently — a follow → unfollow → follow → unfollow loop must
consume the same budget continuously. Separate buckets would grant two
independent allowances and weaken protection against exactly that
pattern. `post_create` stays separate because it is a functionally
unrelated, far more expensive resource (Postgres write, cache warm,
fanout, WS pushes) with a distinct abuse motivation (timeline spam) —
there is no principled reason posting should throttle the ability to
follow someone, or vice versa.

---

## Redis mechanism

**Rejected: `INCR` + `EXPIRE`.** Not rejected because it's a bad Redis
pattern in general — it's rejected because it cannot express trailing-
window semantics at all. It has no concept of individual request
timestamps, only one count tied to one reset clock, which is
structurally fixed-window regardless of how it's wired. Given the
milestone is specifically titled "sliding window," and the correct
alternative isn't meaningfully more expensive to operate, there's no
reason to accept the semantic mismatch.

**Chosen: sorted-set sliding-window log.** Each request is a ZSET
member scored by its Unix timestamp, key `(user_id, action)`. Per
check: prune members with score `< (now − W)`, count remaining members,
and if `< N`, add the new member — else reject. This directly implements
the semantics above: individually-decaying entries, exact trailing-
window counting, full burst allowed. Memory is self-bounding at O(N)
entries per user per action, the same shape as `cache.py`'s existing
`ZREMRANGEBYRANK`-trimmed timeline sorted sets.

**Atomicity:** prune → count → conditional-add must execute as one
atomic unit against Redis, or two concurrent requests from the same
user could both read the same pre-prune count and both be allowed past
the limit. This is done via a small Lua script (`EVAL`), not
`MULTI`/`WATCH` — the "act" (add) is conditional on the "check" (count)
within the same round-trip, which is exactly what a server-side script
gives you atomically and `MULTI`/`WATCH` does not without an extra
retry loop.

**TTL is memory hygiene only**, set generously (e.g. `2×W`) purely so
an abandoned user's key eventually gets garbage-collected. It plays no
role in rate-limit semantics — using key expiry as the "window reset"
mechanism would silently reintroduce fixed-window behavior under a
different name. This distinction (conceptual behavior vs. whatever
cleanup mechanism Redis happens to offer) is the same discipline this
project already applies elsewhere (M2's Streams retention, M5's cache
TTL).

**`Retry-After`** **derivation:** the oldest surviving ZSET entry
(`ZRANGE key 0 0 WITHSCORES`) is exactly the next entry to age out of
the window — `Retry-After = (oldest_score + W) − now`. This falls out
of data the check already reads, for free. It is deliberately *not*
derived from Redis key `TTL`, which would only tell you when the whole
bucket is garbage-collected, not when the next individual slot frees up
— using it would reintroduce fixed-window semantics through the
response contract after explicitly ruling it out in the mechanism
itself.

---

## Failure behavior

- **Fail open** only on a genuine infrastructure failure reaching the
  limiter (connection error, timeout, script error) — never when the
  limiter successfully determines the user is over budget. An "over
  limit" result is a completed, correct decision, not a failure; it
  must always return `429`.
- Every fail-open event is logged. A fail-open path that never logs is
  a rate limiter that quietly stops enforcing without anyone noticing.
- This is acceptable because rate-limit state is disposable enforcement
  state, not durable business state (see ADR-3) — losing it temporarily
  costs a window of under-enforcement, not corrupted data.
- No changes to the existing failure semantics of `cache.py`,
  `event_bus.py`, or `ws_router.py`. Each keeps its own client and its
  own existing failure behavior, unmodified.

---

## HTTP contract

- `429 Too Many Requests`, raised via the same `HTTPException` style
  every other route already uses.
- `Retry-After` header, delta-seconds, computed as above.
- `detail` stays a human-readable string
  (e.g. `"Rate limit exceeded for post_create. Try again in 12 seconds."`),
  consistent with existing `HTTPException` usage elsewhere in the app.
- No `X-RateLimit-Limit` / `-Remaining` / `-Reset` headers. That's
  standard practice for public APIs with external consumers who need to
  self-throttle; this app has one first-party frontend that does
  nothing with rate-limit visibility today. Deferred, not rejected.

---

## Enforcement placement

**FastAPI dependency**, via a small factory: `rate_limit(action: str)`.
Middleware was ruled out structurally — it runs before FastAPI's
dependency tree resolves, has no access to `get_current_user`'s
decoded identity, and would need to reimplement JWT decoding and a
path→action mapping independently, which is exactly the generalized
framework this milestone is avoiding. Dependency vs. inline is a much
closer call — both call the same underlying checker — but a dependency
keeps rate limiting visible at the same layer this codebase already
uses to declare route preconditions (`Depends(get_current_user)` sits
in every protected route's signature today); inline would require
reading into the function body to discover a route is rate-limited at
all. Each protected route opts in explicitly:

```python
@app.post("/posts")
async def create_post(
    body: CreatePostBody,
    current_user: dict = Depends(get_current_user),
    _: None = Depends(rate_limit("post_create")),
):
    ...
```

The factory takes only an action name and stays deliberately unable to
do anything else — no per-call custom limits, no route-matching
config, no generic "framework" surface.

---

## Configuration

Following the existing flat-constant convention in `app/config.py`
exactly (matching `HEAVY_FANOUT_THRESHOLD`'s shape):

```python
RATE_LIMIT_POST_CREATE_MAX              = int(os.getenv("RATE_LIMIT_POST_CREATE_MAX", "5"))
RATE_LIMIT_POST_CREATE_WINDOW_SECONDS   = int(os.getenv("RATE_LIMIT_POST_CREATE_WINDOW_SECONDS", "60"))
RATE_LIMIT_FOLLOW_ACTION_MAX            = int(os.getenv("RATE_LIMIT_FOLLOW_ACTION_MAX", "20"))
RATE_LIMIT_FOLLOW_ACTION_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_FOLLOW_ACTION_WINDOW_SECONDS", "60"))
```

`5` and `20` are development/product-policy placeholders — the same
status as `HEAVY_FANOUT_THRESHOLD=4` — not researched or claimed-correct
limits. `app/rate_limit.py` assembles these into an internal
`action name → (max, window)` lookup; that lookup is mechanism-internal
wiring, not a second config format.

---

## Architecture Decision Records

### ADR-1: Redis is required because of multi-worker horizontal scaling, not because the roadmap names it

**Decision:** rate-limit state lives in Redis, in its own module with
its own client (`app/rate_limit.py`), not as in-process state.

**Reason:** M3/M7.5 already establish that `app.py` is designed to run
behind multiple Uvicorn workers sharing only Redis and Postgres. An
in-process counter (e.g. a Python dict) would be silently wrong the
moment a second worker exists — each process would enforce an
independent limit, multiplying the effective ceiling by worker count
with no visible symptom. Redis is the only state already shared across
every HTTP worker process. This is the actual justification, stated
explicitly here rather than inherited unexamined from the roadmap's
mechanism name.

**Revisit When:** never expected to — this dependency is structural to
how the app is meant to scale, not a temporary convenience.

### ADR-2: The limiter is a precondition gate and runs before the Postgres write

**Decision:** `rate_limit(action)` runs before `db.create_post` /
`db.add_follow` / `db.remove_follow`, not after, and not wrapped around
them in any transactional sense.

**Reason:** the rate-limit decision is logically independent of
whether the write itself succeeds — it's a check on "should this
request be allowed to attempt the action at all," not a property of
the write. Running it first means a rejected request costs one Redis
round-trip and nothing else: no Postgres write, no cache warm, no
`bus.publish`. This also means the limiter never touches or depends on
the event bus, sidestepping its at-least-once/redelivery semantics
entirely — there is no reason to route a synchronous precondition check
through infrastructure built for asynchronous, retryable event
processing.

**Revisit When:** never, by design — this is definitional to what a
precondition gate is.

### ADR-3: Rate-limit state is disposable enforcement state, not durable business state — this is why fail-open is acceptable

**Decision:** losing rate-limit counters (Redis restart, key eviction,
a transient connection failure) is treated as an acceptable, temporary
lapse in enforcement, not a correctness incident.

**Reason:** this deliberately does **not** follow the "durable source
of truth + best-effort optimization" pattern established by M2/M5/M8.
That pattern applies when the *data itself* is either irreplaceable
(M8's notification read-state) or reconstructible from a durable
source (M5's post cache). Rate-limit counters are neither — they aren't
business data at all, they're temporary bookkeeping whose entire
purpose is enforcement during a specific trailing window. If lost,
nothing needs to be reconstructed; the window simply restarts empty.
This is the distinction the M9 handoff explicitly warned against
collapsing (Section 9): don't reflexively apply M8's durability model
here just because Redis is involved in both.

**Revisit When:** the abuse being defended against carries real
per-request cost (e.g., billed API calls) rather than timeline-spam
nuisance — at that point failing closed to protect the business becomes
the correct trade, and this decision must be explicitly reopened, not
silently inherited.

### ADR-4: `follow_action` is a single shared bucket, not independent `follow_create`/`follow_delete` limits

**Decision:** `POST /me/follow/{id}` and `DELETE /me/follow/{id}` draw
from the same `(user_id, "follow_action")` bucket.

**Reason:** the policy being enforced is "rate of follow-graph
mutation," not "how many follows" and "how many unfollows" as two
separate, unrelated resources. Separate buckets would grant two
independent allowances and specifically weaken protection against a
follow → unfollow → follow → unfollow loop — exactly the rapid-mutation
pattern this policy exists to catch. `post_create` stays a separate
bucket from `follow_action` because posting and following are
functionally unrelated actions with no shared abuse motivation; there
is no principled reason one should throttle the other.

**Revisit When:** follow and unfollow are shown to need materially
different limits for a reason unrelated to graph-mutation rate (not
currently anticipated).

### ADR-5: `INCR` + `EXPIRE` is rejected for producing fixed-window semantics, not because it's an inherently flawed Redis pattern

**Decision:** the roadmap's proposed `INCR`+`EXPIRE` implementation is
not used.

**Reason:** `INCR`+`EXPIRE` is a perfectly good pattern for what it
actually implements — a fixed-window counter with a hard reset every
`W` seconds. The rejection is purely semantic: it cannot represent
"trailing `W`-second window" under any wiring, because it has no
concept of individual request timestamps, only a single count on a
single reset clock. This produces the classic boundary problem (up to
`~2N` requests clustered across two adjacent windows), and since the
milestone is specifically titled "sliding window," and the correct
alternative (ZSET) isn't meaningfully more expensive to build or
operate, there's no justification for accepting the semantic gap.

**Revisit When:** never anticipated for this project's stated limits —
if a future limit needed to protect against something where only
coarse, cheap, approximate enforcement mattered (not the case for the
volumes involved here), `INCR`+`EXPIRE` would be worth reconsidering on
its own merits, not as a "simpler" substitute for this design.

### ADR-6: The Lua script is justified specifically by atomicity under concurrent requests from the same user, not by general preference for server-side scripts

**Decision:** prune → count → conditional-add executes as one Lua
script (`EVAL`), not as separate round-trips guarded by `MULTI`/`WATCH`.

**Reason:** two concurrent requests from the same user must not both
observe the same pre-prune count and both be allowed past `N`. The
"act" (adding this request's timestamp) is conditional on the "check"
(the count after pruning) — that conditional relationship has to be
evaluated and committed atomically against Redis. A Lua script gives
this in one round-trip; `MULTI`/`WATCH` would require an explicit
optimistic-retry loop to get the same guarantee, adding real complexity
for no corresponding benefit here.

**Revisit When:** never anticipated — this is the direct, minimal
mechanism for the actual concurrency hazard, not a generic scripting
preference.

### ADR-7: The ZSET's oldest-entry read also produces the `Retry-After` value, for free

**Decision:** `Retry-After` is computed from the oldest surviving ZSET
member's timestamp, not from Redis key `TTL`.

**Reason:** under sliding-window-log semantics, the oldest entry is by
definition the next one to age out of `(now − W, now]` — the exact
moment capacity next becomes available. This value is available from
data the check already reads (`ZRANGE key 0 0 WITHSCORES`), so it costs
nothing extra to compute. `TTL` on the key would only report when the
whole bucket is garbage-collected — an artifact of the memory-hygiene
mechanism (see main text), not of individual request aging — and using
it here would quietly reintroduce fixed-window semantics through the
response contract after the mechanism itself was built specifically to
avoid that.

**Revisit When:** never, by design.

---

## Explicitly out of scope for M9

- IP-based limiting for unauthenticated endpoints, including
  `GET /timeline/{user_id}` — a real, acknowledged resource-protection
  gap (see Reconnaissance), deliberately deferred because it needs a
  different key (IP, not `user_id`) and a different threat model than
  this milestone addresses.
- WebSocket connection limiting.
- Global/API-wide rate limiting.
- Token-bucket or any throughput-smoothing behavior.
- Approximate/probabilistic fixed-window algorithms (e.g.
  Cloudflare-style weighted counters) — solve a memory-scale problem
  this project doesn't have, at the cost of exactness it does want.
- Proactive `X-RateLimit-*` response headers.
- Any change to `src/api.ts`'s error handling.
- Metrics or structured observability beyond logging fail-open events.
- Any event-bus, Outbox, or async-processing changes.
- Any change to the existing failure semantics of `cache.py`,
  `event_bus.py`, or `ws_router.py`.

## Known, accepted consequence (frontend)

`src/api.ts`'s `createPost`, `follow`, and `unfollow` never check
`res.ok` before parsing the response body — only `auth.login`/
`auth.register` do. A `429` response body is `{"detail": ...}`, not the
success shape those callers expect (`{"post_id": ...}` etc.), so
`handlePost()` would optimistically prepend a post with an `undefined`
id on a rate-limited request, silently. This is a pre-existing gap in
`api.ts` that M9 is the first thing to actually trigger — not
introduced by M9, and not fixed by it. Recorded here so it's a known,
deferred consequence rather than a mystery bug discovered later.

---

## Implementation plan

Concrete only — no code written yet.

### 1. `app/config.py` — modify

Add the four flat constants listed above (`RATE_LIMIT_POST_CREATE_MAX`,
`RATE_LIMIT_POST_CREATE_WINDOW_SECONDS`, `RATE_LIMIT_FOLLOW_ACTION_MAX`,
`RATE_LIMIT_FOLLOW_ACTION_WINDOW_SECONDS`). No other change to this
file.

### 2. `app/rate_limit.py` — new

Owns:

- its own Redis client + `init_rate_limiter(url)` / `close_rate_limiter()`
  lifecycle functions, mirroring `cache.py`'s shape exactly.
- an internal `_POLICIES: dict[str, tuple[int, int]]` mapping action
  name → `(max, window_seconds)`, built once from the four config
  constants.
- the Lua script text (prune → count → conditional add → return
  allowed/oldest-timestamp), loaded once at init.
- a low-level `_check(user_id, action) -> (allowed: bool, retry_after: float | None)`
  function that runs the script and computes `Retry-After` from the
  script's returned oldest-timestamp when `allowed` is `False`.
- the public `rate_limit(action: str)` dependency factory: returns an
  async function usable in `Depends(...)`; on infrastructure error,
  catches, logs, and allows (fail-open); on a successful "over limit"
  result, raises `HTTPException(429, ..., headers={"Retry-After": ...})`.

### 3. `app/app.py` — modify

- `lifespan()`: call `init_rate_limiter(REDIS_URL)` alongside the
  existing `db`/`cache`/`bus`/`router` init calls, and
  `close_rate_limiter()` in the teardown sequence, same ordering
  discipline already documented at the top of this file.
- `create_post`: add `Depends(rate_limit("post_create"))`.
- `follow_user`: add `Depends(rate_limit("follow_action"))`.
- `unfollow_user`: add `Depends(rate_limit("follow_action"))`.

No other route changes.

### 4. `test_rate_limit.py` — new

Direct-against-real-Redis verification, matching this project's
existing `test_*.py` convention (no pytest, no mocking framework beyond
targeted monkeypatching, real infrastructure, `assert` + printed
sections). Planned coverage:

- **Allow/deny at threshold** — seed nothing, issue `N` calls to
  `_check`, assert all allowed; issue one more, assert denied.
- **Boundary correctness** — seed a synthetic ZSET entry with a
  timestamp fabricated to sit just outside vs. just inside
  `(now − W, now]`, and assert it is/isn't counted. No `sleep()` needed
  — this is the concrete payoff of choosing a ZSET (Recon item 5).
- **`Retry-After` correctness** — with a known set of seeded
  timestamps, assert the returned value matches `(oldest + W) − now`
  within a small tolerance.
- **Concurrency / atomicity** — fire `N + 5` concurrent `_check` calls
  for the same `(user_id, action)` via `asyncio.gather`, assert exactly
  `N` are allowed and `5` are denied. This is the test that actually
  exercises the Lua script's atomicity guarantee (ADR-6); without it,
  the design's central correctness claim is unverified.
- **Bucket independence** — exhausting `post_create` for a user does
  not affect that user's `follow_action` count, and vice versa.
- **Shared `follow_action` bucket** — alternating follow/unfollow calls
  for the same user consume the same budget (confirms ADR-4 behavior
  at the mechanism level, not just in the design doc).
- **Fail-open** — monkeypatch the rate limiter's Redis client method to
  raise (mirroring `test_notification_hints.py`'s `manager.send`
  monkeypatch technique), call `rate_limit(...)`'s dependency function
  directly, assert it does not raise `HTTPException` and that a log
  line was emitted.
- One short-window, real-time integration check (e.g. `N=2`, `W=2s`)
  that actually waits with `asyncio.sleep()` past the window and
  confirms a previously-denied user is allowed again — a small, cheap
  version of `test_streams.py`'s real-time-wait precedent, kept purely
  as an end-to-end sanity check on top of the seeded-timestamp unit
  tests above, not a replacement for them.

### 5. Manual E2E verification (post-implementation, not automated)

- Hit `POST /posts` 6 times rapidly as a seed user (default limit 5);
  confirm the 6th returns `429` with a `Retry-After` header; wait out
  the window; confirm success resumes.
- Alternate follow/unfollow against the same target rapidly; confirm
  the shared `follow_action` bucket is exhausted by the combination,
  not by either action alone.
- Confirm a `post_create` rejection does not affect the ability to
  follow/unfollow in the same window, and vice versa.

### 6. Documentation

This document, updated in place once implementation and verification
are complete — status line changed from "DESIGN COMPLETE" to
"COMPLETE," with a Verification section appended in the same style as
`milestone-8.5`/`milestone-8.6`.

---

## Open items intentionally left for implementation time, not design time

None. Every semantic, mechanism, failure-mode, contract, placement, and
configuration decision needed to implement M9 correctly has been made
above. Implementation did not require any further architectural choice
beyond ordinary coding judgment — see "Implementation notes" below for
the two judgment calls that did come up, neither of which changes any
approved decision.

---

## What was built

### New files

```text
app/rate_limit.py     — sliding-window-log limiter: Redis client,
                         policy table, Lua script, check_rate_limit(),
                         rate_limit() dependency factory
test_rate_limit.py     — direct verification against real Redis
```

### Modified files

```text
app/config.py   — four new flat constants (RATE_LIMIT_POST_CREATE_MAX/
                  WINDOW_SECONDS, RATE_LIMIT_FOLLOW_ACTION_MAX/
                  WINDOW_SECONDS)
app/app.py      — lifespan now calls init_rate_limiter()/
                  close_rate_limiter() alongside db/cache/bus/router;
                  POST /posts, POST /me/follow/{id}, and
                  DELETE /me/follow/{id} each gained
                  Depends(rate_limit(...))
```

### Unchanged

`worker.py`, `app/db.py`, `app/cache.py`, `app/event_bus.py`,
`app/ws_router.py`, `app/ws_manager.py`, `app/consumers.py`,
`app/notifications.py`, all frontend files — confirms the design's
claim that the limiter is a self-contained precondition gate with no
dependency on the worker process, the event bus, or any existing Redis
consumer's behavior.

## Implementation notes (judgment calls made during implementation, not design changes)

**"Now" is Redis's own clock (`TIME`), not the caller's `time.time()`.**
The design says "score = request timestamp" without specifying which
clock produces it. Since `app.py` is meant to run behind multiple
Uvicorn workers (ADR-1's own justification for using Redis at all),
computing "now" via each worker's local system clock would make the
sliding window sensitive to clock drift between workers. Computing it
once, inside the Lua script, from Redis's own `TIME` command gives every
worker the same authoritative time source. This doesn't change any
approved semantic — it's a stricter, more correct realization of
"timestamp" than the design needed to specify.

**The fail-open catch is scoped to `redis.exceptions.RedisError`
specifically, not a bare `except Exception`.** A bare catch would also
silently swallow a genuine programming error — e.g. a typo passing
`rate_limit("post_creat")` to a route — as if it were an infrastructure
failure, masking a bug as "working as designed, just less strictly."
`check_rate_limit()` raises `ValueError` for an unknown action, and that
exception is deliberately allowed to propagate rather than being caught
by the fail-open path. This keeps "fail open only on genuine
infrastructure failure" (the approved decision) true in practice, not
just in the docstring.

Per your instruction, neither of these was treated as license to reopen
any settled decision — both are implementation-level realizations of
decisions already made, not new ones.

## Automated test results

Run against a real, live local Redis instance (see status note above):

```text
[1] Threshold — exactly max_count allowed, then denied
    ✅  20 allowed, request 21 denied with retry_after=60s
 
[2] Boundary correctness — entries outside the window are pruned, inside survive
    ✅  in-window entry survived pruning, out-of-window entry was removed
    ✅  entry exactly at the window boundary was correctly excluded
 
[3] Retry-After — derived from the oldest surviving entry, not Redis TTL
    ✅  retry_after=50s ≈ expected 50.0s (oldest entry was 10.0s old)
 
[4] Concurrency — N+5 simultaneous requests resolve to exactly N allowed
    ✅  25 concurrent requests -> exactly 20 allowed (atomic, no over-admission)
 
[5] Bucket independence — post_create and follow_action don't interfere
    ✅  exhausting post_create left follow_action fully available for the same user
 
[6] Shared follow_action bucket drains under alternating calls
    ✅  20 alternating follow/unfollow-equivalent calls exhausted the single shared bucket
 
[7] Fail-open — a Redis/script failure is caught, allowed through, and logged
    ✅  rate_limit() dependency swallowed the infrastructure failure without raising
    ✅  fail-open event was logged: [RateLimit] FAIL-OPEN for user=failopen-8ec3adc3 action=post_create — infrastructure error: ConnectionError('simulated Redis outage')
    ✅  an unknown action raises ValueError immediately rather than being fail-opened
 
[8] Short real-time window expiry (small window, real sleep — like test_streams.py)
    ✅  capacity genuinely returned after real wall-clock time passed
 
Exit code: 0. All 8 sections passed.
```

Section [4] is the one that actually matters most: it's the direct,
empirical test of ADR-6's atomicity claim, not just a plausibility
argument. 25 truly concurrent coroutines hitting the same
`(user_id, action)` key resolved to exactly 20 allowed — no
over-admission — which is only possible if the Lua script's
prune→count→conditional-add is genuinely atomic against Redis.

Fail-open's log line was confirmed emitted (via the standard `logging`
module, visible on stderr):

```text
[RateLimit] FAIL-OPEN for user=failopen-3378c5c9 action=post_create — infrastructure error: ConnectionError('simulated Redis outage')
```

## Manual E2E verification

Performed via direct HTTP requests (`httpx`, script:
`e2e_verify_m9.py`) against a real running instance of the app
(`uvicorn app.app:app`, real local Postgres, real local Redis) — this
milestone has no browser-visible UI effect to click through (the
frontend doesn't special-case `429` at all — see "Known, accepted
consequence" above), so hitting the actual endpoints and inspecting
real status codes/headers **is** the meaningful end-to-end check here,
not a stand-in for one. Windows were temporarily shortened via the
existing env vars (`RATE_LIMIT_POST_CREATE_MAX=3` /
`WINDOW_SECONDS=4`, `RATE_LIMIT_FOLLOW_ACTION_MAX=3` /
`WINDOW_SECONDS=4`) purely so the "wait out the window" step was
practical to run live — same code path, same dependency, smaller
numbers. Full server log and script output below are from the actual
run, not reconstructed.

**Test 1 — `POST /posts`, 4 rapid requests as `alice` (limit 3).**
Requests 1–3 returned `200`; request 4 returned `429` with a
`Retry-After` header. Waiting out that real `Retry-After` value (5s)
and retrying succeeded again:

```text
✅  first 3 requests succeed, 4th is 429       — [200, 200, 200, 429]
✅  429 response carries a Retry-After header
      Retry-After: 4s — waiting 5s for the real window to elapse...
✅  posting succeeds again after the real window elapses
```

Server-side log for the same sequence confirms the precondition-gate
design (ADR-2) end-to-end, not just by inspection of the code: exactly
three `💾 [Cache] warmed post:...` lines appear before the `429` —
the rejected 4th request never reached `create_post`'s body at all, no
Postgres write, no cache warm, no event bus publish:

```text
💾  [Cache] warmed post:b4e1826a
INFO: "POST /posts HTTP/1.1" 200 OK
💾  [Cache] warmed post:acd901c9
INFO: "POST /posts HTTP/1.1" 200 OK
💾  [Cache] warmed post:87efbc4b
INFO: "POST /posts HTTP/1.1" 200 OK
INFO: "POST /posts HTTP/1.1" 429 Too Many Requests
```

**Test 2 — shared `follow_action` bucket, live (`bob` → `dave`,
limit 3).** Alternating `POST /me/follow/dave` / `DELETE /me/follow/dave`
three times all returned `200`; the 4th call (a follow) returned `429`
— confirming ADR-4 at the HTTP layer, not just in the unit-level test:

```text
✅  3 alternating follow/unfollow calls succeed, 4th (shared bucket) is 429
      [('follow', 200), ('unfollow', 200), ('follow', 200), ('unfollow', 429)]
```

**Test 3 — bucket independence, live.** With `alice`'s `post_create`
bucket exhausted from Test 1, `POST /me/follow/dave` as `alice`
returned `200` immediately. With `bob`'s `follow_action` bucket
exhausted from Test 2, `POST /posts` as `bob` returned `200`
immediately:

```text
✅  alice can still follow (follow_action) despite post_create being exhausted
✅  bob can still post (post_create) despite follow_action being exhausted
```

Full script result: `✅ All M9 manual E2E checks passed against the
live running app` (exit code 0). No warnings, tracebacks, or unexpected
log lines appeared anywhere in the server log across the whole run.

## Deviations from the approved design

None. Every semantic, mechanism, failure-mode, contract, placement, and
configuration decision was implemented as specified. The two items
under "Implementation notes" above are realizations of already-approved
decisions (which clock produces "now"; which exception type fail-open
catches), not changes to what was approved.

## Issues discovered during implementation

- `app/auth.py` imports `jwt` (PyJWT), but `pyproject.toml`/`uv.lock`
  list no such dependency — a pre-existing gap unrelated to M9, noticed
  only because building a clean environment to test in required
  installing it explicitly. Worth a one-line fix to `pyproject.toml`
  at some point; not part of this milestone's scope and not touched
  here.
- No other issues. The design held up under real concurrent load and
  real wall-clock timing with no adjustments needed.
