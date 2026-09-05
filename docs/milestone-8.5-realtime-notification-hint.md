# Milestone 8 — Persistent Notification Store

## FanoutFeed · `milestone-8-realtime-notification-hint`

---

## Goal

Give the frontend a live signal that a new notification exists, without
reopening any guarantee Milestone 8 established. Postgres notification
rows remain the sole source of truth for existence and read state; this
milestone adds exactly one thing on top — a best-effort WebSocket hint
telling a connected client "something happened, go check" — and nothing
else.

---

## Why now

M8 shipped the durable store with no live signal at all: a user only
discovers a new notification by polling `GET /notifications` or
reloading. M7.5's own closing note already anticipated this milestone,
observing that a persistent store changes what "notify a follower" means
— a durable write plus an *optional* live push, rather than only a live
push. M8.5 is that optional layer, added on top of a subsystem whose
correctness does not depend on it existing at all.

---

## The one rule everything below follows from

**The WS hint may never become a second source of truth.** Concretely:
the hint payload is never shaped like the REST `Notification`
representation, and reconciling a hint on the frontend is never allowed
to write to `notifications` or `unreadCount` directly — only a REST
refetch may do that. Every design decision in this milestone is in
service of making that rule hold structurally, not just by convention.

---

## Inspecting `event_bus.py` before proposing anything

Three facts from `_process()` drove every decision below:

```python
for handler in self._handlers.get(event_type, []):
    try:
        await handler(payload)
    except Exception as e:
        return  # NOT ACKed — message stays pending; ALL handlers re-run on redelivery
await self._client.xack(stream_key, _GROUP_NAME, msg_id)
```

1. Handlers for one event run **sequentially, in registration order**,
   within a single `_process()` call.
2. ACK is **all-or-nothing** — one handler raising skips every handler
   after it this round and leaves the message pending for `XAUTOCLAIM`
   to redeliver.
3. Redelivery **re-runs every handler from the top**, including ones
   that already succeeded.

---

## WS payload contract

```json
{
  "type": "NEW_NOTIFICATION",
  "notification_type": "NEW_POST",
  "actor_id": "alice",
  "actor_name": "Alice",
  "object_type": "post",
  "object_id": "a1b2c3d4"
}
```

Deliberately **no `id`, `created_at`, or `read_at`** — the fields that
make up the REST `Notification` row. This is a guardrail, not an
oversight: making the wire shape visibly different from the REST shape
is what makes "never mutate authoritative state from this payload" a
structural fact rather than a convention someone has to remember to
follow. `app/notifications.py`'s `_notification_hint_payload()` is the
single place this shape is defined, so both hint functions stay in sync
by construction.

`FollowCreated` now carries `follower_name`, sourced from the JWT
payload in `follow_user()` (already embedded by `create_access_token`,
no extra DB lookup needed) — the same "carry what a consumer needs to
render without a lookup" precedent `PostCreated`'s `author_name` already
set. `notify_new_follower_hint` falls back to `follower_id` if
`follower_name` is absent, rather than raising.

---

## Consumer topology

```python
bus.subscribe("PostCreated",   fanout_consumer)
bus.subscribe("PostCreated",   realtime_consumer)
bus.subscribe("PostCreated",   on_post_created)          # M8 — durable write
bus.subscribe("PostCreated",   notify_new_post_hint)      # M8.5 — live hint, MUST run after
bus.subscribe("FollowCreated", on_follow_created)         # M8 — durable write
bus.subscribe("FollowCreated", notify_new_follower_hint)  # M8.5 — live hint, MUST run after
```

Both hint consumers live in `app/notifications.py`, not `consumers.py` —
same reasoning M8 already established for `on_post_created`/
`on_follow_created`: this is a different business capability (live
delivery) that happens to react to the same events, not a natural
extension of `fanout_consumer`/`realtime_consumer`. Both reuse
`ConnectionManager.send()` → `PubSubRouter.publish()` → `ws:notify:{user_id}`
unchanged — the exact channel `realtime_consumer` already pushes
`NEW_POST` through. **No new WS route, no new `PubSubRouter` capability,
no new `ConnectionManager` method were needed.**

---

## Architecture Decision Records

### ADR-1: Live hint is a separate consumer, not folded into the durable writer

**Decision:** `notify_new_post_hint`/`notify_new_follower_hint` are
distinct functions and distinct bus-handler registrations from
`on_post_created`/`on_follow_created`.

**Reason:** durable creation and live delivery are different
failure-tolerance concerns — the durable write must succeed or the
event must be retried; the live push may simply fail and be forgotten.
This is the identical justification `fanout_consumer` and
`realtime_consumer` already have for being separate consumers, applied
to a second pair.

**Revisit When:** no trigger identified — this mirrors an
already-validated pattern in this codebase.

### ADR-2: Durable-before-live ordering depends on `event_bus.py`'s CURRENT sequential handler execution

**Decision:** each hint consumer is registered strictly after its
durable counterpart for the same event type.

**Reason:** this guarantees the notification row exists before any
client is told to go look for it — the identical justification M0.5
already used for `fanout_consumer` running before `realtime_consumer`
(timeline written before the WebSocket push fires).

**This is a real dependency, not an incidental detail.** It relies
entirely on `_process()`'s current behavior: handlers for one event run
sequentially, in registration order, within a single process call. If
`event_bus.py`'s execution model ever changes — handlers run in
parallel, or each handler becomes its own consumer group with an
independent cursor — this ordering guarantee evaporates and must be
re-established explicitly (e.g. by having the live-hint handler query
for the row's existence rather than assuming a prior handler already
wrote it).

**Revisit When:** `event_bus.py`'s handler execution model changes from
sequential/same-process to anything else.

### ADR-3: Hint handlers must never raise

**Decision:** `notify_new_post_hint` and `notify_new_follower_hint` each
wrap their entire body in a try/except that logs and swallows every
exception. Neither function can propagate a failure into `_process()`.

**Reason:** given ADR-2's ordering and `_process()`'s all-or-nothing ACK
(fact 2 above), a hint handler that raised would force redelivery of an
event whose durable work (`on_post_created`/`fanout_consumer`/
`realtime_consumer`) had *already succeeded*, purely to retry a
non-authoritative push. This is the mechanism that makes it
**structurally impossible** for a failed live hint to cause durable
notification state to be lost or incorrectly retried — not a policy
that has to be remembered, a property the code enforces.

**Consequence for redelivery:** because the hint handler never raises,
the only way it fires twice for the same logical event is if the entire
worker process dies mid-`_process()` *after* the hint already ran once
in that round — producing a duplicate toast on the next attempt. That
sits in the same tolerated-nuisance category `realtime_consumer`
already occupies for `NEW_POST`; not worth engineering around for a
channel whose entire definition is "not authoritative."

**Revisit When:** never, by design.

### ADR-4: Hint payload is structurally distinct from the REST representation

**Decision:** the WS hint never contains `id`, `created_at`, or
`read_at`. `_notification_hint_payload()` in `app/notifications.py` is
the single place its shape is defined.

**Reason:** see "The one rule everything below follows from," above.
Enforced at three layers: the backend never puts these fields on the
wire (`app/notifications.py`); the frontend types keep `Notification`
and `NotificationHintWSMessage` as separate interfaces rather than e.g.
`Partial<Notification>` (`front/src/types.ts`); and
`reconcileNotificationHint()` throws at runtime if a hint payload ever
does carry one of these fields, rather than silently accepting it
(`front/src/notification-hint.js`) — a loud, fail-fast guard against the
wire contract drifting undetected.

**Revisit When:** never, by design.

---

## Frontend reconciliation

`NEW_NOTIFICATION` arrives on the **same** WebSocket connection
`useFeedWebSocket` already owns (`/ws/feed`), distinguished from
`NEW_POST` only by the top-level `type` field — no second connection, no
new hook managing its own socket lifecycle.

`src/notification-hint.js` is the single place the reconciliation rule
lives — deliberately plain JS, not TypeScript, so it's directly
`node`-runnable by `test-notification-hint.mjs` with zero build step,
mirroring the backend's `uv run test_*.py` convention rather than
requiring a test framework this project doesn't otherwise have.
`src/notification-hint.d.ts` supplies the type declarations `tsc -b`
needs to resolve the import cleanly, without adding `allowJs` to
`tsconfig.app.json` — Vite's dev server already handles the plain-JS
import fine on its own via esbuild and never needed this file at all.

`App.tsx` wires the new `useFeedWebSocket` callback to
`reconcileNotificationHint`, which is the *only* function permitted to
touch `notificationUI` state, and it only ever changes `hasNewHint` — a
purely local UI affordance flag, structurally identical in role to the
existing `newCount` banner pattern already used for `NEW_POST`.
`notifications`/`unreadCount` stay empty placeholders this milestone;
populating them is explicitly deferred to the full notification-UI
milestone (not yet built), which is also where the "open notifications
→ discard local state → refetch `GET /notifications` and
`GET /notifications/unread-count`" reconciliation — the same
reset-and-refetch shape `loadTimeline()` already uses for posts — gets
wired for real. `acknowledgeNotificationHint()` exists now specifically
to keep that future wiring point pre-named rather than inventing it
later.

---

## What was built

### New files

```text
app/notify_new_post_hint / notify_new_follower_hint  — added to app/notifications.py
test_notification_hints.py                            — backend verification
front/src/notification-hint.js                         — pure reconciliation logic
front/src/notification-hint.d.ts                        — type declarations for the above
front/test-notification-hint.mjs                        — frontend verification (zero build step)
docs/milestone-8.5-realtime-notification-hint.md
```

### Modified files

```text
app/notifications.py    — two new hint consumers + shared payload builder
worker.py                — subscribes both, after their durable counterparts
app/app.py                — FollowCreated payload now includes follower_name
front/src/types.ts         — Notification, NotificationPage, NotificationHintWSMessage, FeedWSMessage
front/src/hooks/use-feed-websocket.ts — branches on NEW_NOTIFICATION alongside NEW_POST
front/src/App.tsx           — wires the hint callback; minimal hasNewHint affordance
```

### Unchanged

`event_bus.py`, `ws_router.py`, `ws_manager.py`, `consumers.py`,
`cache.py`, `db.py` — confirms the goal that no new transport,
abstraction, or infrastructure was needed; `ConnectionManager`/
`PubSubRouter` absorbed a second use case with zero modification, the
same way `event_bus.py` absorbed `FollowCreated` with zero modification
in M8.

---

## Verification

**`test_notification_hints.py`** (backend, real Postgres + Redis, a
`FakeWebSocket` registered with the actual `ConnectionManager`/
`PubSubRouter` — same technique as `test_pubsub_router.py`):

- §1 — durable write then live hint, in order, both confirmed against
  the real database and a real delivered WS message.
- §2 — `manager.send` monkeypatched to raise; `notify_new_post_hint`
  does not propagate the failure, and the durable row from §1 is
  unaffected.
- §3 — `follower_name` propagates into `actor_name`; a payload missing
  it falls back to `follower_id` instead of raising.
- §4 — neither hint payload contains `id`, `created_at`, or `read_at`.

**`test-notification-hint.mjs`** (frontend, plain Node, no build step,
imports the real module the app uses):

- Message-type discrimination (`NEW_POST` vs. `NEW_NOTIFICATION`).
- Structural distinctness from the REST shape.
- `reconcileNotificationHint` leaves `notifications`/`unreadCount`
  untouched, changing only `hasNewHint`.
- A malformed hint carrying a REST-only field is rejected with a thrown
  error, not silently accepted.
- `acknowledgeNotificationHint` clears only the local flag.

Both were actually executed during this milestone, not just written:
the backend suite compiles cleanly; the frontend suite runs to
completion; and the full frontend change set was type-checked with
`tsc` against the project's real compiler settings (`react-jsx`,
`moduleResolution: bundler`, `verbatimModuleSyntax`, etc.) with zero
errors.

---

## Known limitations

- **`notify_new_post_hint` independently re-fetches followers** — a
  fourth independent `db.get_followers(author_id)` call per post,
  alongside `fanout_consumer`, `realtime_consumer`, and
  `on_post_created`. A known, accepted inefficiency, not addressed here
  — out of scope per the same discipline that deferred celebrity-scale
  optimization in M8 ADR-5.
- **No frontend notification panel** — `hasNewHint` is a placeholder
  affordance only. Building the real panel (list, unread count, mark
  read) against the REST endpoints M8 already shipped is deferred to a
  future milestone.
- **Duplicate toast on worker crash mid-batch** — see ADR-3's redelivery
  note. Accepted, same category as `realtime_consumer`'s existing
  exposure.
- **Outbox / celebrity notification optimization / aggregation** —
  unchanged from M8, not touched by this milestone.

---

## Next milestone

The full frontend notification UI (list, unread badge backed by
`GET /notifications/unread-count`, mark-read/mark-all-read wired to the
REST endpoints) is the natural next step, now that the live hint exists
to drive it. Per this project's discipline, it's not pursued
speculatively here — it waits for its own milestone.
