# Milestone 0 — Foundation

## Status

Completed (baseline)

---

## Goal

Establish a working Fanout-on-Write feed system with zero infrastructure
complexity, to learn the *pattern* before adding production concerns.

---

## Previous Architecture

None — starting point.

---

## Limitation

N/A — this is the baseline every future milestone is measured against.

---

## Why This Solution?

In-memory event bus + in-memory store isolates the fanout mechanics
(publish → fanout_consumer → realtime_consumer) from infra concerns
(persistence, networking, auth). Cheapest way to validate the pattern
is correct before paying for anything else.

---

## Changes Made

- `InMemoryEventBus`: synchronous pub/sub, handlers awaited in order
- In-memory `USERS` / `POSTS` / `TIMELINES` / `FOLLOWS` dicts
- `fanout_consumer`: writes post_id into follower timelines
- `realtime_consumer`: pushes NEW_POST to online followers only
- `ConnectionManager`: per-user WebSocket registry
- `SystemBroadcaster`: debug Event Stream panel feed
- React frontend with live Event Stream visualization

---

## Request Flow

```
POST /posts?author_id=X
  → persist to POSTS dict
  → bus.publish("PostCreated")
      → fanout_consumer   (writes TIMELINES)
      → realtime_consumer (pushes to online WS)
```

---

## New Components

| Component | Purpose |
|-----------|---------|
| InMemoryEventBus | Sequential pub/sub, guarantees fanout completes before realtime push |
| ConnectionManager | Tracks personal WS per user_id |
| SystemBroadcaster | Streams internal events to debug panel |

---

## Benefits

- Fanout-before-notify ordering is correct by construction
- Clean event contract (self-contained payload)
- Observable — every step visible in the Event Stream panel

---

## Remaining Limitations

- `author_id` is a spoofable query param — no real identity
- Nothing persists across restart (in-memory only)
- Event bus has no durability, and lives inside a single process
- Single process — WS state is local

---

## Ceiling

Local dev / demo only. Not meaningful to quote a user ceiling — the
store doesn't survive a restart, let alone concurrent load.

---

## Next Milestone

**Milestone 0.5 — Infrastructure Migration (PostgreSQL + Redis)**

Problem it solves: in-memory store and event bus can't survive a
restart or crash — every post, follow, and timeline disappears the
moment the process stops.

Why now: every feature past this point needs data that outlives a
single process. Building auth or any user-facing feature on top of
storage that resets to empty on restart just means rebuilding it
again once persistence lands — cheaper to fix this first.