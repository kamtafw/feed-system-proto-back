# Milestone 7 — Hybrid Fanout (Fanout-on-Write + Fanout-on-Read)

## FanoutFeed · `milestone-7-hybrid-fanout`

---

## Goal

Fix the "celebrity problem": a single post from a high-follower account
currently triggers O(followers) sequential Redis writes in
`fanout_consumer`, blocking that worker and creating latency spikes for
every other event on the bus. Introduce a follower-count threshold above
which accounts skip write-time fanout entirely and are instead merged
into follower timelines at read time.

Unlike prior milestones, this one changes how the write path *branches*
rather than adding a new component.

---

## Previous architecture and its limitation

`fanout_consumer` loops over every follower unconditionally:

```python
followers = await db.get_followers(author_id)
for follower_id in followers:
  await cache.push_to_timeline(follower_id, post_id, created_at)
```

At 100k followers, that's 100k sequential awaited Redis round-trips in
one consumer invocation. `realtime_consumer` has the identical shape for
the WS push — same root cause, explicitly left unfixed in this
milestone (see Follow-up).

Roadmap-driven, not felt-pain-driven, same as M5/M6 — our test accounts
have single-digit followers. `HEAVY_FANOUT_THRESHOLD` is a small,
env-configurable dev value so the branch is reachable in testing; it is
not representative of the real ~10,000+ figure from the architecture
review.

---

## Architecture Decision Records

### ADR-1: Heavy-account detection uses `len(followers)`, not a `COUNT(*)`

Decision:  
fanout_consumer classifies an account as heavy or light using
len(followers) from the SAME db.get_followers() call it already
makes — no separate COUNT(*) query, no cached/persisted counter.

Status:  
Accepted (Milestone 7)

Reason:  
The follower list is already being fetched for the light-path loop;
reusing it for classification costs nothing extra in the common
(light) case. A real celebrity account would mean fetching a huge
list purely to discard it on the heavy branch — an accepted
inefficiency given we cannot test at real celebrity scale anyway,
and given the project's standing principle of not solving problems
before they're felt.

Tradeoffs:

```text
+ Zero new queries, zero new state to keep in sync with Postgres
+ Classification can never drift from the real follow graph — it's
recomputed from source-of-truth data on every single post
- A genuinely huge follower list is fully materialized in memory
just to take the heavy branch and discard it
```

Revisit When:
Profiling shows the full-follower-list fetch itself (not the old
O(followers) Redis writes it replaced) becoming a measurable cost
for real heavy accounts. At that point, switch to a cheap
COUNT(*)-first check, only fetching the full list when it turns out
to be light.

### ADR-2: No "is this followed account currently heavy" index

Decision:  
GET /timeline/{user_id} always queries authored:{id} for EVERY
account the user follows, rather than maintaining a separate set of
currently-classified-heavy account IDs to pre-filter against.

Status:  
Accepted (Milestone 7)

Reason:  
Following-list sizes are typically small (dozens, not thousands).
An empty ZREVRANGEBYSCORE against a nonexistent authored: key is
cheap, and all such queries for one request are pipelined into a
single Redis round-trip regardless of how many come back empty.
Maintaining a second classification set would mean keeping it in
sync with account promotion/demotion — genuine complexity for a
case the "just query it, empty is cheap" approach already handles.

Tradeoffs:

```text
+ No second piece of state to keep consistent with anything
+ Read cost genuinely scales with how many accounts YOU follow —
not with anyone's follower count. That's the actual trade-off
this milestone makes: celebrity-post writes get cheap, and the
cost moves to every one of their followers' reads instead.
- A user following a very large number of accounts (most of them
never heavy) still pays a pipelined-but-nonzero query per
followed account on every timeline read
```

Revisit When:
Following-list sizes grow large enough that the per-request
fan-out of authored: queries becomes the new bottleneck — at that
point, a maintained "currently heavy" set becomes worth its
synchronization cost.

---

## Design notes (not formal ADRs, but worth documenting)

### Cursor correctness across multiple sources

The same cursor value is applied independently to every source before
merging. This preserves M6's no-drift guarantee for two reasons:

1. New inserts in any source score at/near "now," which is always ≥ any
   cursor derived from already-seen posts — same protection as the
   single-source case, applied symmetrically.
2. Fetching `limit+1` from **each** source (not once globally) is
   provably sufficient: the true global top-`limit` page can contain at
   most `limit` items from any single source (there are only `limit`
   slots total), so `limit+1` per source always gives enough headroom to
   correctly assemble the true merged page and detect `has_more` —
   regardless of source count or how winners are distributed among them.

One accepted inefficiency: a source that keeps losing the interleaving
race across several pages gets re-queried (bounded, pipelined) on each
of those pages before its items finally surface. Not a correctness
issue, just a minor redundant-query cost.

### Merge strategy: flatten-and-sort, not a k-way merge

Total candidate pool per request is `(1 + accounts_followed) × (limit+1)`
— a few thousand items at most. `sorted()` on that is microseconds.
`heapq.merge()` (true k-way merge) is the standard choice when merging
many large or lazily-streamed sorted sources; here the pool size scales
*with* the source count rather than being independent of it, so the
asymptotic advantage doesn't really materialize. Flatten-and-sort is
simpler to write and equally correct at this scale.

### IDs merged before bodies are resolved

Merging happens on cheap `(post_id, score)` pairs, trimmed to `limit`,
**before** M5's cache-aside body resolution runs. This keeps the
Postgres/cache cost of a timeline read identical to a single-source
read regardless of how many accounts are merged in — only the ID-merge
step scales with the number of sources.

---

## What was built

### Modified files

```text
backend/
app/config.py    — HEAVY_FANOUT_THRESHOLD (default 5, dev-scale only)
app/cache.py     — push_to_authored(), get_timeline_candidates()
                   (scored variant of get_timeline_ids), 
                   get_authored_candidates_bulk() (pipelined)
app/consumers.py — fanout_consumer() branches light/heavy on
                   len(followers); realtime_consumer() unchanged
app/app.py       — get_timeline(): merges timeline:{user} +
                   authored:{followed} candidates before resolving
                   bodies via M5's unchanged cache-aside logic

frontend/
src/types.ts             — add FANOUT_HEAVY to SystemEvent
src/components/event-log.tsx — render FANOUT_HEAVY distinctly (👑,
                                follower count only, never the list)
src/App.css               — .log-fanout-heavy styling
```

### New files

```text
backend/
test_hybrid_fanout.py — light-path direct-fanout assertion,
                        heavy-path authored:-only assertion, and a
                        read-time-merge assertion proving a
                        zero-direct-write follower still sees the post
```

### Unchanged

`worker.py`, `event_bus.py`, `db.py`, `ws_manager.py`, `realtime_consumer`,
Milestone 5's cache-aside logic, Milestone 6's `get_timeline_ids()` and
`test_cursor_pagination.py`, all auth/follow routes, both WebSocket
routes, the frontend composer/optimistic-prepend/infinite-scroll logic.

---

## Request flow comparison

### Before (M6, single source)

```
POST /posts → fanout_consumer → loop over ALL followers → push_to_timeline() × N

GET /timeline/{id} → get_timeline_candidates(user) only → resolve bodies
```

### After (M7, hybrid)

```
POST /posts → fanout_consumer
light (≤ threshold): loop over followers → push_to_timeline() × N (unchanged)
heavy (> threshold): push_to_authored(author) × 1 (NEW)

GET /timeline/{id}
→ get_timeline_candidates(user) — own feed
→ get_authored_candidates_bulk(following) — NEW, pipelined
→ merge (id, score) pairs, dedupe, trim to limit — NEW
→ resolve bodies for exactly the returned posts (M5, unchanged)
```

---

## Verification

- **`test_hybrid_fanout.py`** — all three phases passed:
  - Light account (5 followers, at threshold): all 5 received the post
    via direct timeline write; `authored:{author}` confirmed empty.
  - Heavy account (8 followers, over threshold): zero followers received
    a direct write; `authored:{author}` contained exactly the one post.
  - Read-time merge: a heavy account's follower who received *zero*
    direct writes correctly saw the post via the merge logic — proving
    the branch is invisible to the follower, not just correctly taken
    on the write side.
- **Live end-to-end confirmation** — `HEAVY_FANOUT_THRESHOLD` reduced
  and an existing account (bob) followed past it mid-session. Every
  subsequent post from that account correctly and repeatedly took the
  heavy branch (`worker.py` console: `👑 [Fanout/HEAVY] ... writing
  authored:bob only`, three separate posts). A follower's
  `GET /timeline` showed rising cache-hit counts including bob's
  heavy-path posts, despite that follower's own `timeline:` key never
  receiving those post IDs directly — confirming the merge through the
  real HTTP request path, not just the isolated test script.
- **Discovered during this verification, not introduced by this
  milestone:** none of `FANOUT_START`, `FANOUT_WRITE`, `FANOUT_HEAVY`,
  `REALTIME_START`, or `REALTIME_SEND` reach the browser's `EventLog`.
  Root cause and scope are covered under Known Limitations below.

---

## Known limitations

- **Heavy-account classification re-fetches the full follower list**
  every post, purely to take `len()` (ADR-1) — acceptable given we can't
  test at real scale, revisit if profiling ever shows it mattering.
- **Read cost scales with how many accounts you follow**, not
  introduced as a side effect but as the explicit trade-off this
  milestone makes (ADR-2).
- **`realtime_consumer` still has the O(followers) shape** — deliberately
  deferred, see Next Milestone.
- **Consumer-side debug events never reach the browser `EventLog`**
  (generalizing the Milestone 3 `SystemBroadcaster` per-process
  limitation): since Milestone 4 moved `fanout_consumer`/
  `realtime_consumer` into the standalone `worker.py` process — which
  never serves `/ws/events` — every `system.broadcast()` call from a
  consumer reaches zero browser clients. This affects `FANOUT_START`,
  `FANOUT_WRITE`, `FANOUT_HEAVY` (new in this milestone), `REALTIME_START`,
  and `REALTIME_SEND` — all silently invisible since M4, not a
  regression introduced here. `POST_CREATED` is the only event that has
  ever worked, because it's broadcast from `app.py`'s process, which
  *does* serve `/ws/events`. Frontend rendering for `FANOUT_HEAVY` is
  complete and correct; it just has nothing to receive from yet.
  Console/terminal output in `worker.py` remains the reliable way to
  observe consumer-side behavior in the meantime. Fix scoped as
  Milestone 7.5.

---

## Next milestone

- **M7.5** — Fix `SystemBroadcaster`'s cross-process routing: mirror
  `ConnectionManager`'s existing Redis Pub/Sub pattern (publish from any
  process, forward to local `/ws/events` clients from whichever process
  holds them) so consumer-side debug events finally reach the browser.
- Fix `realtime_consumer`'s identical per-follower WS-push loop (same
  root cause as this milestone, deferred here to keep scope focused).
- M8 — Persistent notification store (per the original roadmap).

