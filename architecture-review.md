# Milestone 5 — Post Cache

## FanoutFeed · `milestone-5-post-cache`

---

## Goal

Reduce Postgres read load on `GET /timeline/{user_id}` by caching post
bodies in Redis, converting the timeline read into a cache-aside pattern.

---

## Previous architecture and its limitation

Since the prototype, timeline reads were a two-step operation:

1. `cache.get_timeline_ids()` — Redis `ZREVRANGE`, post IDs newest-first
2. `db.get_posts_by_ids()` — Postgres `WHERE id = ANY($1::text[])`

Step 2 is a single indexed batch query — cheap in isolation. The problem
is frequency, not per-query cost: a popular post gets independently
re-fetched from Postgres once per follower per timeline read. Read
volume scales with `views × avg followers per post`, not with distinct
post count.

This is not a felt bottleneck at current traffic — it's introduced
proactively, per the original roadmap, before it becomes one.

---

## Architecture Decision Records

### ADR-1: Post cache representation — JSON strings, not Redis hashes

Decision:  
Store cached posts as JSON strings (post:{id}) instead of Redis hashes.

Status:  
Accepted (Milestone 5)

Reason:  
Timeline reads are bulk fetches by post ID list. MGET provides
efficient single-round-trip batch retrieval for plain string keys;
there is no equivalent multi-key-multi-hash primitive. Posts are
immutable (no edit/delete endpoints exist) — there is no field that
needs atomic, partial updating, which is the scenario hashes exist
to serve.

Tradeoffs:

```text
+ Simpler implementation
+ Efficient batch reads (single MGET vs. N pipelined HGETALLs)
+ Fewer Redis commands
- Entire JSON must be rewritten if any field changes
- Less efficient for partial-field updates
```

Revisit When:

- Post editing is introduced
- Delete/restore is introduced
- Like/view/comment counters are updated frequently
- Partial-field mutations become common

### ADR-2: Cache writes are best-effort, not part of the durable write path

Decision:  
`cache.set_post()` failures on the `POST /posts` write path are logged
and do not fail the request.

Status:  
Accepted (Milestone 5)

Reason:  
Redis is a performance layer here, not the source of truth —
PostgreSQL already durably persisted the post before the cache
write is attempted. The read-miss path (Postgres fallback +
backfill) fully repopulates the cache on the next read regardless
of whether the write-path warm succeeded. Failing the user-facing
request for a cache-layer problem would trade availability for a
guarantee we don't actually need.

Tradeoffs:

```text
+ POST /posts availability is decoupled from Redis health
+ Consistent with "Redis silently drops on no subscriber" precedent
already established for Pub/Sub delivery (M3)
- First few timeline reads after a warm failure hit Postgres before
the cache self-heals via the miss path
- Failures are only visible via logs, not surfaced to the caller
```

Revisit When:

- Redis takes on a correctness-critical responsibility that cannot
be reconstructed from Postgres (e.g., a write-through store, or
coordination state) — at that point "best-effort" is the wrong
default and this decision should be revisited per-responsibility,
not blanket-reversed.

---

## What was built

### Modified files

```text
backend/
app/cache.py — get_posts() (bulk MGET), set_post(), set_posts()
(pipelined bulk write for backfill)
app/config.py — POST_CACHE_TTL_SECONDS (default 86400s / 24h)
app/app.py — create_post(): warm cache after DB write, before bus.publish(); best-effort, logged on failure
get_timeline(): cache-aside read (MGET → Postgres fallback for misses → pipelined backfill → merge, preserving sorted-set order)
```

### New files

```text
inspect_post_cache.py — direct Redis inspection: view a cached post + TTL, list all cached keys, or evict a key to force a miss on demand (same pattern as inspect_stream.py from Milestone 2)
```

### Unchanged

`worker.py`, `consumers.py`, `event_bus.py`, `db.py`, `ws_manager.py`, all frontend files — this milestone only touches the read/write seam in `app.py`, plus two new cache functions. No event contract changes, no consumer logic changes.

---

## Request flow comparison

### Before (pre-M5)

```
POST /posts → db.create_post() → bus.publish()

GET /timeline/{id}
├─ cache.get_timeline_ids() (Redis ZREVRANGE)
└─ db.get_posts_by_ids() (Postgres, ALWAYS, every read)
```

### After (M5)

```
POST /posts
├─ db.create_post()
├─ cache.set_post() ← NEW, best-effort, before publish()
└─ bus.publish()

GET /timeline/{id}
├─ cache.get_timeline_ids() (Redis ZREVRANGE, unchanged)
├─ cache.get_posts() ← NEW, bulk MGET
├─ db.get_posts_by_ids() ← ONLY for cache misses
└─ cache.set_posts() ← NEW, backfill misses for next read
```

A full cache miss (cold Redis, TTL expiry across the board) degrades to exactly the pre-M5 code path — no new failure mode introduced, just a fast path added on top.

---

## Verification

- **Warm read:** `50 hit(s), 0 miss(es)` logged — zero Postgres queries
  for post bodies on a fully-warm timeline.
- **Write-path warm:** `💾 [Cache] warmed post:{id}` logged synchronously
  inside `create_post()`, before the HTTP response. Confirmed via
  `inspect_post_cache.py <id>` — correct JSON shape, TTL ≈ 86400s.
- **Organic miss:** creating a new post shifted the top-50 timeline
  window to include a pre-existing post that had never been read since
  M5 shipped (and thus was never cached) → `1 miss(es)` logged →
  Postgres fallback fired → backfilled silently. Confirmed present via
  `inspect_post_cache.py` afterward.
- **Manual miss:** `inspect_post_cache.py --evict <id>` → confirmed
  deleted → subsequent `GET /timeline` (via the frontend) → key
  reappears with a fresh ≈86400s TTL — confirms the backfill path
  independent of the organic case above.

---

## Known limitations

- **Cache-write failures are silent to the caller by design (ADR-2)** —
  observable only via logs. Acceptable given Redis's role as a
  performance layer, not source of truth; revisit if that role changes.
- **No cache invalidation mechanism** — acceptable only because posts
  are currently immutable. The moment editing or deletion exists, TTL
  alone is insufficient and this needs a real invalidation strategy
  (see ADR-1's revisit conditions).

---

## Next milestone — Milestone 6: Cursor-based timeline pagination

**Problem:** `GET /timeline?offset=50` drifts under concurrent writes —
new posts arriving while a user pages through shifts every subsequent
offset, causing skipped or duplicated posts on page 2+.

**Solution:** Accept a `cursor` (the score of the last-seen post)
instead of an `offset`. `ZREVRANGEBYSCORE` returns posts with scores
below the cursor regardless of new inserts above it.

**What it unlocks:** Stable feed reads under concurrent write load —
a prerequisite for real infinite-scroll UX.
