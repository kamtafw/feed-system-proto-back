# Milestone 8.5 — Realtime Notification Hint

## FanoutFeed · `milestone-8.5-realtime-notification-hint`

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
front/src/App.css            — .notification-hint-btn (see note below)
```

**Bell affordance styling was corrected during manual verification, and
the correction is part of M8.5's scope** (it's a bug in this
milestone's own new UI element, not a pre-existing one). The bell
initially reused `.follow-btn`'s styling — built for text labels
("Following"/"Follow"), never setting an explicit `line-height`. A bare
emoji glyph (🔔) renders with taller intrinsic ascent/descent metrics
than Latin text at the same declared font-size, and since `.header` is
`display: flex; align-items: center` with no fixed height, the untamed
emoji-only child grew `.header`'s rendered height past the 57px the
rest of the stylesheet hardcodes (`.layout`, `.splash` both assume
`calc(100vh - 57px)`) — and only when the bell was actually present,
i.e. only when `hasNewHint` flipped true, which is why it looked like
layout breakage tied to "activity." Fixed with a dedicated
`.notification-hint-btn` class: a fixed 28×28 circle with centered
content and `line-height: 1`, the same fixed-dimension pattern already
used by `.header-avatar`/`.compose-avatar`/`.person-avatar` specifically
to keep glyph metrics from ever affecting layout.

**A second, unrelated layout bug was found and fixed during the same
manual verification pass, but it is explicitly OUT OF M8.5's scope**:
`EventLog`'s pre-existing `scrollIntoView()` call (in
`src/components/event-log.tsx`, untouched by any of this milestone's
own changes) could escape to page-level scroll, because its ancestors
(`.event-log`, `.layout`) are `overflow: hidden` and therefore skipped
by `scrollIntoView`'s ancestor walk, landing on `body`/`html` instead
and dragging the whole page — feed included — along with it. This bug
predates M8.5 and has nothing to do with the notification hint
architecture; it became newly *visible* during this milestone's testing
because `system.broadcast()`'s events fire on every post, and testing
the notification hint meant posting repeatedly and watching closely.
Fixed by scrolling the panel's own `scrollTop` directly instead of
`scrollIntoView()`-ing a sentinel element, which cannot bubble to any
ancestor. Recorded here for completeness and because it touched a
production file during this milestone's work, not because it belongs to
M8.5's architecture.

### Unchanged

`event_bus.py`, `ws_router.py`, `ws_manager.py`, `consumers.py`,
`cache.py`, `db.py` — confirms the goal that no new transport,
abstraction, or infrastructure was needed; `ConnectionManager`/
`PubSubRouter` absorbed a second use case with zero modification, the
same way `event_bus.py` absorbed `FollowCreated` with zero modification
in M8.

---

## Verification

Two layers of verification were performed: automated (both suites
actually executed against live infrastructure, not just written) and
manual end-to-end (five scenarios run against the real prototype in a
browser). Both are recorded here in full, including the adjustments
made along the way.

### Automated

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

Ran to completion, all four sections passing, against a live backend.
One test-harness defect was found and fixed along the way, not a defect
in the implementation under test: the original `FakeWebSocket` only
implemented `send_text()`, matching `test_pubsub_router.py`'s double —
but that test calls `router.register()` directly, bypassing
`ConnectionManager`, while this test deliberately goes through
`manager.connect()` to exercise the same production path
`notify_new_post_hint`/`notify_new_follower_hint` actually use via
`manager.send()`. `ConnectionManager.connect()` calls `await ws.accept()`
before registering, which the original fake didn't implement. Fixed by
adding a no-op `accept()` to the fake — confirmed by grepping every
`ws.<method>(` call across `ws_manager.py` and `ws_router.py`, which
shows `accept()` and `send_text()` are the *only* two methods ever
invoked on a WebSocket-like object anywhere in this path. No production
code changed to make the test pass.

**`test-notification-hint.mjs`** (frontend, plain Node, no build step,
imports the real module the app uses):

- Message-type discrimination (`NEW_POST` vs. `NEW_NOTIFICATION`).
- Structural distinctness from the REST shape.
- `reconcileNotificationHint` leaves `notifications`/`unreadCount`
  untouched, changing only `hasNewHint`.
- A malformed hint carrying a REST-only field is rejected with a thrown
  error, not silently accepted.
- `acknowledgeNotificationHint` clears only the local flag.

Ran to completion, all five assertions passing. The full frontend change
set was also type-checked with `tsc` against the project's real compiler
settings (`react-jsx`, `moduleResolution: bundler`,
`verbatimModuleSyntax`, etc.) with zero errors.

### Manual end-to-end

Five scenarios were run against the real prototype (`main.py` +
`worker.py` both running, two accounts in separate browser contexts) to
confirm the automated suites' guarantees hold in the actual running
system, not just in isolated function calls.

**Test 1 — `NEW_POST` live hint.** A follows B; B posts while A is
connected. Confirmed in A's DevTools WS Messages: two distinct frames
arrive — the pre-existing `NEW_POST` message (drives the old
"N new posts" banner) and a `NEW_NOTIFICATION` frame matching the
contract exactly (`notification_type: "NEW_POST"`, `actor_id`,
`actor_name`, `object_type: "post"`, `object_id`, and critically no
`id`/`created_at`/`read_at`). The bell affordance appeared in A's
header; `worker.py`'s console stayed silent for the notification path
specifically (`on_post_created`/`notify_new_post_hint` have no
success-path log lines, by design — confirmed, not a bug); the
`/ws/events` EventLog showed only the pre-existing fanout/realtime debug
events, nothing notification-related, since neither new consumer is
wired to `system.broadcast()`.

**Test 2 — `NEW_FOLLOWER` live hint.** A follows B while B is connected.
Confirmed in B's DevTools: exactly one frame (unlike Test 1's pair —
`FollowCreated` has no legacy WS sibling the way `PostCreated` does),
`actor_name` correctly sourced from A's JWT payload via `follow_user()`
(no extra DB lookup), `object_type: "user"` / `object_id` equal to B's
own id (the self-referential object convention from M8, carried through
to the hint unchanged). Bell appeared on B's side; EventLog showed
nothing at all for this event type, before or after — follows have never
been wired to the debug broadcaster.

**Test 3 — durable state vs. live delivery vs. frontend display are
independent.** After Test 1's post, `GET /notifications` and
`GET /notifications/unread-count` were called directly from the browser
console (bearer token pulled from `localStorage`) at three checkpoints:
immediately after the bell appeared, immediately after dismissing the
bell, and after a full page reload. **The response was identical at all
three checkpoints** — dismissing the bell only ever calls
`acknowledgeNotificationHint()`, which touches `hasNewHint` and nothing
else (verified directly in `notification-hint.js`); no route in this
codebase currently calls `POST /notifications/{id}/read` from the
frontend at all. The database had no way to know the hint had ever been
seen or dismissed, because nothing ever told it.

**Test 4 — missed hint / reconnect.** B's tab was closed entirely
(a real `WebSocketDisconnect`, confirmed via `main.py`'s
`[WS] {user_id} disconnected` log line — not a reload, which
`useFeedWebSocket` has no reconnect logic to distinguish from a fresh
mount anyway). While B was disconnected, A posted. B reconnected
(fresh `[WS] {user_id} connected` line — a brand-new registration, not
a resumed one) to **no bell and no banner** — both WS publishes
(`realtime_consumer`'s and `notify_new_post_hint`'s) went out on
channels with zero subscribers at that instant and were dropped, exactly
as `test_pubsub_router.py`'s existing no-subscriber case already proved
in isolation. B's timeline showed A's post immediately on reload with no
extra effort (`GET /timeline` already runs unconditionally on mount and
reads from `timeline:B`, which `fanout_consumer` had written to
regardless of B's connection state). The same manual `fetch()` calls
from Test 3, run after reconnecting, showed the missed post's
notification present, `read_at: null` — the row was never at risk;
only the live signal was.

The missed-hint behavior this confirms, precisely:

- If the recipient is disconnected, the live hint can be missed.
- The durable PostgreSQL notification is still present.
- M8.5 does not attempt to replay missed hints.
- Recovery through the durable notification REST path belongs to the
  future notification UI milestone, not this one.

**Test 5 — duplicate/retry behavior.** No practical manual trigger
exists for this: redelivery requires a handler to raise or the worker
process to die inside a handler-chain window measured in low single-digit
milliseconds at this follower count — not something reproducible on
demand by hand. This property is a timing/concurrency guarantee, not a
UI-observable behavior, and is already covered precisely by
`test_notifications.py` §2 (identity-based dedup under simulated
redelivery), `test_notification_hints.py` §2 (a hint failure cannot
propagate into `_process()`), and `test_streams.py` (the underlying
`XACK`/`XPENDING`/`XAUTOCLAIM` mechanics). No manual test was forced
here — deliberately.

### What manual verification confirmed, as an architectural conclusion

M8.5 verification confirmed that durable notification state, live
WebSocket delivery, and frontend reaction are independent layers. The
WebSocket notification hint is intentionally lossy and must never be
treated as notification state. PostgreSQL remains authoritative; the
hint merely provides low-latency awareness when the recipient is
connected.

```text
Durable notification exists
        ≠
WebSocket hint was delivered
        ≠
Frontend displayed notification state
```

These are three independent layers, each with its own failure mode:
the row can exist with no hint ever sent (recipient offline at publish
time — Test 4); a hint can be sent and received with the frontend
still choosing to display nothing lasting from it (`hasNewHint` is
transient, in-memory, reset on every mount — Test 3); and a hint's
receipt or dismissal never writes back to the row that caused it. No
layer here can be inferred from another. This was a design intent going
in (see "The one rule everything below follows from," above) and is now
a directly observed, verified property of the running system, not just
a documented rule.

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
