# Milestone 6 — Cursor-Based Timeline Pagination

## FanoutFeed · `milestone-6-cursor-pagination`

---

## Goal

Replace offset-based timeline pagination with cursor-based pagination,
eliminating drift (skipped/duplicated posts) when new posts arrive while
a user is scrolling through older pages. Ship the full vertical slice:
backend, frontend infinite scroll, tests.

---

## Previous architecture and its limitation

`GET /timeline?offset=50&limit=50` used `ZREVRANGE(key, offset, offset+limit-1)`
— a *positional* window into the sorted set. If 3 new posts land while a
user is reading page 1, every post's position shifts by 3. Page 2's
`offset=50` now points 3 slots further in than the user's mental model:
3 posts get skipped, or 3 get duplicated, depending on scroll direction.

This is not specific to Redis or this project — it's the standard failure
mode of offset pagination under any concurrent-write workload.

---

## Architecture Decision Records

### ADR-1: Cursor is a score-only value; score-tie collisions are an accepted risk

Decision:  
Timeline pagination cursor is the created_at score alone —
not a compound (score, post_id) cursor.

Status:  
Accepted (Milestone 6)

Reason:  
A score collision requires two posts to share the same created_at
float, which in practice needs near-simultaneous writes across
concurrent processes. At current traffic this is rare enough that
the failure mode — one post skipped or duplicated at a single page
boundary — costs less than the complexity a compound cursor adds
(a Lua script, or an extra round-trip plus client-side tie-break
filtering).

Tradeoffs:

```text
+ Cursor is a single float — no encoding, no extra Redis round-trips
- A tie at the exact page boundary can skip or duplicate one post
- Collision probability rises with write throughput
```

Revisit When:

- Post creation throughput makes same-timestamp collisions
observable in practice
- Exact-once pagination becomes a hard requirement

### ADR-2: `next_cursor` is an opaque string, not a typed number

Decision:  
next_cursor is typed and treated as an opaque string token by both
the API contract and the frontend, even though the current
implementation is just a stringified created_at float.

Status:  
Accepted (Milestone 6)

Reason:  
Cursor-based APIs (Stripe, GitHub, Relay/GraphQL) universally treat
cursors as tokens the client passes back verbatim, never parses or
constructs. Committing to that now costs nothing — the string just
happens to be a float today — but means a future migration to a
compound (score, post_id) cursor changes only the encoding inside
get_timeline()/get_timeline_ids(), with zero frontend changes.

Tradeoffs:

```text
+ Future cursor-format migration requires no frontend changes
+ Matches established convention for cursor pagination
- Marginally less debuggable in dev tools than a raw number —
negligible today since the string IS just the float
```

Revisit When:

Not revisited on its own — this is the default going forward.
Only the internal encoding changes if ADR-1's revisit conditions
(score collisions becoming observable) are triggered.

---

## API contract change

```text
Before: GET /timeline/{user_id}?limit=50&offset=0 → Post[]
After: GET /timeline/{user_id}?limit=50&cursor=<opaque string>
→ { posts: Post[], next_cursor: string | null }
```

This intentionally replaces the old offset-based API; no backward
compatibility is maintained because the frontend is updated in the same
milestone.

`cursor` omitted = first page. `next_cursor: null` = no older posts left
— either genuinely exhausted, or the `TIMELINE_MAX` (500) cap has been
hit and Redis has trimmed anything older (a pre-existing limitation,
not introduced by this milestone).

---

## What was built

### Modified files

```text
backend/
app/cache.py — get_timeline_ids() rewritten: offset → cursor
               (ZREVRANGE → ZREVRANGEBYSCORE, exclusive upper bound)
app/app.py   — get_timeline(): opaque cursor parsing, "fetch limit+1"
               has_more detection, { posts, next_cursor } response shape

frontend/
src/types.ts — add TimelinePage
src/api.ts   — getTimeline() returns TimelinePage, takes optional cursor
src/App.tsx  — pagination state (nextCursor, hasMore, loadingMore),
               loadMore(), sentinel + end-of-feed markup
src/App.css  — .scroll-sentinel, .feed-end
```

### New files

```text
backend/
test_cursor_pagination.py — seeds a timeline, pages through it while
                             injecting new posts between fetches, asserts
                             exactly-once/in-order/no-leak across the
                             whole session

frontend/
src/hooks/use-infinite-scroll.ts — IntersectionObserver hook; fires a
                                    callback when a sentinel element
                                    scrolls into view
```

### Unchanged

`worker.py`, `consumers.py`, `event_bus.py`, `db.py`, `ws_manager.py`,
Milestone 5's cache-aside read logic (MGET → Postgres fallback →
backfill), all auth/follow routes, both WebSocket routes, the composer
and optimistic-prepend logic in `App.tsx`.

---

## Request flow comparison

### Before (offset)

```
GET /timeline?offset=50&limit=50
→ ZREVRANGE(key, 50, 99) — a POSITION, shifts under concurrent writes
```

### After (cursor)

```
GET /timeline?cursor=<created_at>&limit=50
→ ZREVRANGEBYSCORE(key, "(<cursor>", "-inf", start=0, num=51)
— a VALUE boundary; inserts above it don't move anything below it
→ has_more = fetched 51 items back
→ next_cursor = created_at of the 50th item (as an opaque string)
```

---

## Verification

- **`test_cursor_pagination.py`**: 12 seeded posts, paged through while a
  new post is injected between every single page fetch. Asserts: all 12
  original posts returned exactly once, in newest-first order; none of
  the injected posts leaked into the in-progress scroll session; all
  injected posts correctly appear on a subsequent fresh (cursor=None)
  fetch. This is the concrete proof offset pagination could not offer —
  a page boundary that concurrent writes cannot disturb.
- **Manual browser check**: scrolled through 2–3 pages via infinite
  scroll while a second logged-in tab posted; already-loaded posts and
  scroll position were undisturbed, confirming the same guarantee
  end-to-end through the UI.

---

## Known limitations

- **Score-tie collisions accepted (ADR-1)** — extremely rare at current
  write rates; one post could theoretically be skipped/duplicated at a
  page boundary if two posts share an identical `created_at` float.
- **"N new posts" banner resets pagination to page 1** — pre-existing
  behavior, not introduced or fixed here. If you've scrolled several
  pages deep and click the banner, those loaded older pages are
  discarded in favor of a fresh top-of-feed view.
- **`TIMELINE_MAX` (500) cap still applies** — cursor pagination can only
  page as far back as Redis has retained; beyond that, `next_cursor`
  simply becomes `null` even though older posts still exist in Postgres.

---

## Next milestone — Milestone 7: Hybrid fanout (Fanout-on-Write + Fanout-on-Read)

**Problem:** Fanout-on-Write doesn't scale to high-follower accounts — a
single post from a 100k-follower account triggers 100k Redis writes,
blocking the consumer and creating latency spikes for every other event
on the bus (the "celebrity problem").

**Solution:** Introduce a follower-count threshold. Accounts above it
skip write-time fanout; their posts are merged into follower timelines
at read time instead. Accounts below the threshold keep today's
Fanout-on-Write path unchanged.

**What it unlocks:** The system can support accounts with millions of
followers without a structural rewrite of the write path.
