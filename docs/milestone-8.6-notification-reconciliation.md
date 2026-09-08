# Milestone 8.6 — Frontend Notification Reconciliation

## FanoutFeed · `milestone-8.6-notification-reconciliation`

**Status: COMPLETE.** Implemented, type-checked against the project's
real compiler settings with zero errors, automated `isNotificationHint()`
coverage passing, and manual E2E verification confirmed against the
real running prototype. Updated in place at closeout, the same way
`milestone-8.5-realtime-notification-hint.md` was.

---

## Goal

Make the notification data M8 made durable and M8.5 made observable
*usable* — a real list, a real unread count, real mark-read/mark-all
actions, and a live hint that actually causes the frontend to catch up
with what's true in Postgres, rather than toggling a dead-end boolean.

---

## Why now, not M9

Covered in full in the preceding sequencing discussion: M8.5's own
"Next milestone" section already named this exact scope as the natural
next step; M9 (Rate Limiting) has no technical dependency on or from
this work in either direction; and this gap was a deliberate, explicitly
documented deferral in both M8 and M8.5, not an oversight. **M9 remains
Rate Limiting, unrenumbered, untouched by this milestone.**

---

## The architectural principle this milestone exists to enforce

```text
WebSocket hint = change signal
REST = authoritative state
PostgreSQL = durable source of truth
Frontend state = representation of REST-fetched state
```

Every decision below — the retirement of M8.5's reconciliation
scaffolding, the rejection of a toast, the shape of the reconciliation
flow — follows directly from keeping these four things in their own
lane. The hint's entire job is to say "go check REST." Nothing else.

---

## No new domain model

Unlike M8 and M8.5, this milestone introduces **zero new backend
concepts and zero new backend code**. Every endpoint it needs already
shipped in M8: `GET /notifications`, `GET /notifications/unread-count`,
`POST /notifications/{id}/read`, `POST /notifications/read-all`. There
is no new identity question, no new durability question, no new ADR
about ordering or failure isolation to make — those were all settled
two milestones ago. This is a pure frontend consumption milestone, which
is exactly why it can be small.

---

## Why Option A (pure trigger) over a toast

This was the one genuinely open question, and it deserves the actual
reasoning, not just the recorded outcome.

**A toast necessarily treats the hint as a content source, not a change
signal.** The moment `actor_name`/`notification_type`/`object_type`/
`object_id` get rendered directly from the hint payload, the hint has
stopped being "something changed, go look" and become "here is what
changed" — which is precisely the distinction M8.5 was built to prevent
collapsing. That M8.5 already enforced this for *state* (`unreadCount`,
`notifications`) doesn't automatically extend it to *rendering* — a
toast could render hint content while still correctly leaving
`unreadCount`/`notifications` alone. Choosing Option A closes that gap
before it opens: no code path in this app ever displays notification
content that didn't come from a REST response.

**A toast can go stale relative to REST between the moment it's built
and the moment it's shown.** The durable row is guaranteed to exist by
the time the hint fires (M8.5 ADR-2's ordering), but nothing guarantees
the row's *current* state still matches what the hint would render.
Concretely: if the same notification is marked read from another tab or
device in the interval between the hint arriving and the toast
rendering, a toast built from the hint payload has no way to reflect
that — it would show unread-styled content for something already
handled. A REST-sourced render can't have this problem, because it
always reflects whatever is true *at fetch time*, not at
event-publish time.

**Two toasts can disagree with the list they're supposedly summarizing.**
If two hints arrive close together (a small light-fanout burst, or a
follow immediately followed by a post), a toast-per-hint UI has to
decide how to stack, replace, or queue them — and whatever it decides,
that sequence is a second, independently-arrived-at account of "what
happened," sitting next to the list a subsequent refetch renders. If
those two accounts ever disagree — which requires no bug, just ordinary
timing — that's a second source of "the UI told me two different
things" bugs, in addition to the durable/live/frontend divergence M8.5
already had to reason carefully about. Zero toasts means zero chance of
this.

**Option A also has a quiet extensibility benefit worth naming.** Because
nothing in the frontend ever parses the hint payload beyond its
top-level `type` field, the shape of `notification_type`/`actor_name`/
`object_type`/`object_id` can change freely in the future — new
notification types (likes, mentions, reposts) can be added on the
backend with **zero** frontend hint-handling changes, since the trigger
logic never depended on those fields meaning anything specific. A toast
implementation would need updating every time a new notification type
needed its own rendering treatment.

**The honest cost, stated plainly:** Option A is less immediately
lively. A user doesn't get an instant "Alice followed you" pop-up the
moment it happens — they see a badge count change, or nothing until they
open the panel. That's a real, accepted trade against the reasoning
above, not something to pretend away.

---

## What gets retired, not just added

`front/src/notification-hint.js` currently exports `isNotificationHint`,
`reconcileNotificationHint`, `acknowledgeNotificationHint`, and the
`NotificationUIState` type. The latter three existed for exactly one
reason: to hold the line ("`hasNewHint` may change, `notifications`/
`unreadCount` may not") until real REST-backed state existed to enforce
that line for real. That reason ends here.

- **Retired**: `reconcileNotificationHint`, `acknowledgeNotificationHint`,
  `NotificationUIState`, and the `notification-hint-btn` placeholder
  bell's role as a standalone dead-end affordance.
- **Kept, unchanged**: `isNotificationHint()` — WebSocket message
  discrimination on `/ws/feed` is still needed regardless of what
  happens after a hint is identified as one.
- **Consequence, named explicitly rather than left implicit**:
  `notification-hint.d.ts` shrinks to declare only `isNotificationHint`.
  `test-notification-hint.mjs` loses the assertions for the three
  retired functions. This is a *named supersession* of M8.5's own
  scaffolding — the same shape as M7.5 explicitly retiring M4's
  identity-guard once the data structure it existed for changed — not a
  silent deletion.

---

## Reconciliation flow

```text
mount/login            → loadNotifications(); refreshUnreadCount()
NEW_NOTIFICATION hint   → refreshUnreadCount();
                            and loadNotifications() IF the panel is
                            currently open — never renders hint content
                            itself, only triggers these two REST calls
panel opens             → loadNotifications()   — always a fresh page-1
                            fetch, the same reset-and-refetch shape
                            loadTimeline() already uses; closes any
                            staleness gap regardless of whether every
                            intervening hint actually arrived
mark one read           → POST /notifications/{id}/read
                            → loadNotifications() + refreshUnreadCount()
mark all read           → POST /notifications/read-all
                            → loadNotifications() + refreshUnreadCount()
```

### Fetch concurrency — checked against the actual codebase, not assumed

Inspected `App.tsx`'s two real precedents before answering this, rather
than reasoning abstractly:

- **`loadTimeline()`** (the direct analog of `loadNotifications()` —
  both are "fresh page-1, replace everything" operations) has **no
  concurrency guard at all**. Its `.then()` unconditionally calls
  `setTimeline`/`setNextCursor`/`setHasMore` whenever the promise
  resolves, and its only trigger — the banner's `onClick={loadTimeline}`
  — has no `disabled` state during the fetch either. If it were double-
  invoked and the two responses resolved out of order, the older one
  would silently overwrite the newer one. This is a real, pre-existing,
  unaddressed micro-race — not a convention this design can point to as
  "already solved," and not something M8.6 is scoped to fix in
  `loadTimeline()` itself.
- **`loadMore()`** (the direct analog of `loadMoreNotifications()` —
  both are "append the next page" operations) *does* guard against
  this, but structurally rather than by detecting staleness after the
  fact: `if (!currentUser || !hasMore || loadingMore) return` makes it
  impossible for a second request to even start while one is in flight.
  There is never a moment with two in-flight `loadMore()` calls to race
  against each other.

**Why `loadTimeline()`'s lack of a guard doesn't transfer to
`loadNotifications()`, even though they're structurally identical
operations:** the risk isn't in the code shape, it's in the trigger
cadence. `loadTimeline()` is only ever invoked by a human clicking a
button — true overlapping in-flight requests essentially never occur in
practice, even though nothing stops them. `loadNotifications()` gains a
trigger source `loadTimeline()` doesn't have: `NEW_NOTIFICATION` hints,
fired by independent backend events with no human reaction-time buffer
between them. Two hints landing within a network round-trip of each
other is plausible even at this project's current scale — exactly the
back-to-back pattern already produced manually during M8.5's Test 1/
Test 2, or simply two followed accounts posting within the same second.
Copying `loadTimeline()`'s unguarded shape here would carry over a race
that's harmless *there* into a context where it's actually reachable.
See ADR-6.

### Reset vs. paginate — made fully explicit

```text
loadNotifications()          — fresh/reconciliation load
    cursor = null
    fetch page 1
    REPLACE notifications, notifCursor, notifHasMore
    (never merges; matches loadTimeline()'s existing setTimeline(page.posts)
     replace-not-append behavior exactly — verified against the real code)

loadMoreNotifications()      — user-requested pagination
    cursor = current notifCursor
    fetch next page
    APPEND to notifications; update notifCursor/notifHasMore
```

**The scenario that needs to be explicit**: a user has scrolled through
pages 1 and 2 of their notification panel. A `NEW_NOTIFICATION` hint
arrives while the panel is still open.

```text
pages 1 + 2 loaded  →  hint arrives  →  loadNotifications() (NOT loadMoreNotifications())
                                     →  fresh page-1 REST fetch
                                     →  REPLACES notifications wholesale — page 2's
                                        already-loaded rows are discarded
                                     →  notifCursor now represents the fresh page 1
                                     →  user can loadMoreNotifications() again if they
                                        want to scroll back down
```

This is not a compromise specific to notifications — it's the exact
same trade-off `loadTimeline()` already makes for posts, and the
existing code already documents it as intentional:

> `// NOTE: this intentionally resets pagination back to page 1. If
> you've scrolled several pages into history and then click the banner,
> those older loaded pages are discarded in favor of a fresh top-of-feed
> view. That's existing behavior carried over from before this
> milestone — not something M6 introduces or fixes.`

`loadNotifications()` inherits this identically: a hint (or opening the
panel) always means "go get the current truth," never "splice something
in." Explicitly ruled out, per the guiding rule: deriving a notification
from the hint payload and appending it, or attempting to surgically
merge a new row into an already-paginated list. The hint carries no
content that could be merged in the first place under Option A — it
only ever triggers a full replace.

State lives directly in `App.tsx` — `notifications`, `notifCursor`,
`notifHasMore`, `unreadCount`, `panelOpen` — as plain `useState`, plain
async functions. Confirmed against actual precedent before proposing
this: hooks in this codebase (`useFeedWebSocket`, `useSystemEvents`,
`useInfiniteScroll`) exist specifically to own a side-effect *lifecycle*
(a WebSocket connection, an `IntersectionObserver`). Notification
list/count is plain REST-fetched data with no lifecycle to manage —
structurally identical to `timeline`/`nextCursor`/`hasMore`, which
already live as plain `App.tsx` state with `loadTimeline()`/`loadMore()`
as plain callbacks, not a hook. No `useNotifications` hook is proposed,
on that basis.

---

## Planned API surface (signatures only — not implemented yet)

```ts
// api.ts additions, mirroring getTimeline/getFollowing's existing shape
getNotifications(cursor?: string): Promise<NotificationPage>
getUnreadCount(): Promise<{ count: number }>
markNotificationRead(id: number): Promise<{ ok: boolean }>
markAllNotificationsRead(): Promise<{ marked_read: number }>
```

All four are thin wrappers over `authedFetch`, identical in shape to
every existing authenticated call in `api.ts`. `Notification` and
`NotificationPage` types already exist in `types.ts` from M8 — no type
changes needed there either.

---

## UI shape (brief — not a full spec)

The existing bell (`.notification-hint-btn`) stops being a placeholder
and becomes the real affordance: it displays `unreadCount` (a number,
not just presence/absence), and clicking it toggles a dropdown panel
listing `notifications`, each item showing actor, a verb derived from
`type`/`object_type`, and a visual read/unread distinction. Clicking an
item calls `markNotificationRead`; a header action calls
`markAllNotificationsRead`. No new route or page — this app has no
router at all, and a toggleable panel needs none.

---

## Architecture Decision Records

### ADR-1: The live hint is a pure trigger — it never renders its own payload

**Decision:** no code path in this milestone renders `actor_name`,
`notification_type`, `object_type`, or `object_id` directly from a
`NEW_NOTIFICATION` message. The hint's only effect is to call
`refreshUnreadCount()` and, conditionally, `loadNotifications()`:

```text
NEW_NOTIFICATION
    └──► triggers REST reconciliation
          └──► never becomes notification state itself
```

**Reason:** see "Why Option A over a toast," above, in full.

**Revisit When:** a future product decision explicitly wants
immediate, pre-REST-confirmation feedback badly enough to accept the
staleness/consistency costs named above — at which point it should be
scoped as its own explicit rendering path, not retrofitted onto the
existing trigger.

### ADR-2: No independent polling of unread count

**Decision:** `unreadCount` refreshes only on mount, on hint arrival, and
when the panel opens. No interval timer.

**Reason:** a hint can be silently lost without a full disconnect (a
transient Redis Pub/Sub hiccup), leaving the badge briefly stale until
the next of those three triggers — accepted, since opening the panel
always ends up correct regardless.

**Revisit When:** staleness is actually observed as a problem in
practice, not preemptively engineered around.

### ADR-3: Mark-all-read refetches rather than optimistically patching loaded rows

**Decision:** after `POST /notifications/read-all` succeeds, refetch
page 1 and the unread count rather than locally flipping every loaded
item's `read_at`.

**Reason:** matches the reset-and-refetch discipline already used
throughout this project (M6's `loadTimeline()`); the mutation is cheap
enough that the extra round trip isn't worth optimistic-update
bookkeeping.

**Revisit When:** list size makes the refetch itself a bottleneck.

### ADR-4: Mark-one-read is pessimistic

**Decision:** wait for `POST /notifications/{id}/read`'s response before
updating the item's visual state; no optimistic flip on click.

**Reason:** this app has exactly one existing optimistic update
(`handlePost`'s prepend), and it's for the user's own action with
near-certain success. Marking read doesn't share that asymmetry, and
staying pessimistic avoids needing a revert path for this milestone.

**Revisit When:** the round-trip latency is actually felt as sluggish.

### ADR-5: M8.5's reconciliation scaffolding is retired, not left dormant

**Decision:** `reconcileNotificationHint`, `acknowledgeNotificationHint`,
and `NotificationUIState` are deleted, not deprecated-in-place.

**Reason:** dead code that used to be load-bearing is worse than no
code — a future reader shouldn't have to work out whether it's still
part of the contract. `isNotificationHint` is kept because it's still
genuinely used; nothing else from that module survives this milestone.

**Revisit When:** never, by design — this isn't a decision expected to
need reopening.

### ADR-6: Hint-triggered reset fetches use a minimal sequence-counter guard against out-of-order responses

**Decision:** `loadNotifications()` and `refreshUnreadCount()` each
capture a per-function monotonically increasing counter (a `useRef`,
incremented at call time) and check it against the current value before
applying the response to state:

```ts
const notifFetchSeq = useRef(0)

const loadNotifications = useCallback(() => {
 if (!currentUser) return
 const seq = ++notifFetchSeq.current
 api.getNotifications().then((page) => {
  if (seq !== notifFetchSeq.current) return // a newer call already started — discard
  setNotifications(page.notifications)
  setNotifCursor(page.next_cursor)
  setNotifHasMore(page.next_cursor !== null)
 })
}, [currentUser])
```

The identical three-line pattern applies independently to
`refreshUnreadCount()`, with its own separate counter — the two are
unrelated requests with unrelated staleness domains, and coupling them
under one counter would be incorrect, not simpler.
`loadMoreNotifications()` does **not** get this treatment; it mirrors
`loadMore()`'s existing `loadingMore`-boolean structural guard instead,
since its trigger (user-paced pagination) matches `loadMore()`'s trigger
shape, not the hint's.

**Reason:** see "Fetch concurrency," above. `loadTimeline()`'s
unguarded shape is safe only because of its human-click trigger cadence
— a cadence `loadNotifications()` does not share, since hints fire at
machine speed from independent backend events. This is the smallest
correct fix for the specific, narrow failure mode in question
(discarding a stale reset-fetch response), not a general-purpose
solution: no `AbortController` (the underlying request isn't cancelled,
just ignored — cancellation isn't needed to get correctness here), no
request-management library, no new hook. Three lines, reused twice,
using only what the component already has (`useRef`).

**Revisit When:** never anticipated — if a genuinely more complex
request-lifecycle need ever arises elsewhere in the app (cancellation,
retries, shared caching), that's the point to consider a real
abstraction, and it should be evaluated on its own merits then, not
backported speculatively into this fix.

---

## Scope

### In scope

- Notification list UI, paginated via the existing `(created_at, id)`
  cursor.
- Unread badge backed by `GET /notifications/unread-count`.
- Mark-one-read wired to `POST /notifications/{id}/read`.
- Mark-all-read wired to `POST /notifications/read-all`.
- Hint-triggered reconciliation (refetch, never render-from-hint).
- Retirement of M8.5's placeholder reconciliation scaffolding.

### Out of scope

- Any rendering of hint payload content (toasts or otherwise) — see
  ADR-1.
- Polling — see ADR-2.
- Optimistic mutations — see ADR-3, ADR-4.
- Click-through navigation from a notification to the post/user it
  references, or any notification detail route — not requested, not
  assumed. This app has no router; none is introduced here.
- Notification preferences (mute user, mute type) — named in the
  original roadmap as something M8 *unlocks*, not something M8.6
  implements.
- Grouping/aggregation of notifications — M8 ADR-5's territory, still
  deferred.
- Animations or complex notification UX beyond what's needed for a
  clean, functional list — this is a reconciliation milestone, not a
  UI-design exercise.
- Push notifications (browser/OS-level) — a different delivery
  mechanism entirely, never discussed, not in scope.
- Celebrity-scale notification write volume, Outbox-pattern reliability,
  `realtime_consumer`'s O(followers) loop — all unchanged, all
  previously deferred, none of this milestone's concern.
- Any backend code or new backend concepts whatsoever.
- Speculative abstractions of any kind — see ADR-6's explicit rejection
  of `AbortController`, a request-management library, or a new hook for
  a problem three lines already solve.
- M9 — untouched, unrenumbered, not designed here.

---

## Verification

### Automated

`test-notification-hint.mjs` was reduced to match ADR-5: the three
assertions covering `reconcileNotificationHint`/
`acknowledgeNotificationHint`/`NotificationUIState` were removed along
with the functions themselves, rather than left pointing at dead code.
`isNotificationHint()`'s discrimination behavior — `NEW_POST` excluded,
`NEW_NOTIFICATION` included, malformed input (`null`, `undefined`, a
bare string, an empty object) safely rejected rather than throwing —
was re-verified and passes. The full frontend change set was also
type-checked with `tsc` against the project's real compiler settings
and complete dependency graph, with zero errors.

### Manual E2E

Verified against the real running prototype, covering the two
conditions that together prove the reconciliation path actually closes
the loop M8.5 left open:

- **Recipient connected**: a `NEW_NOTIFICATION` hint arriving correctly
  triggers a REST refetch of both the notification list and the unread
  count — confirming the reconciliation flow designed above (hint →
  `refreshUnreadCount()`, and `loadNotifications()` when the panel is
  open) behaves as specified in the real app, not just in isolated
  function calls.
- **Recipient disconnected during the originating action**: the live
  hint may be missed, exactly as characterized in M8.5's own Test 4 —
  but the notification remains available through durable PostgreSQL
  state, and is recovered by the frontend's REST fetch once the
  recipient reconnects. Nothing was lost; only the low-latency signal
  was.

### What manual verification confirmed, as an architectural conclusion

M8.6 verification confirmed the complete durable-to-frontend
notification path: PostgreSQL provides durable notification state,
WebSocket provides best-effort low-latency change awareness, and REST
reconciliation makes the frontend authoritative-state-consistent. A
missed WebSocket hint does not lose a notification; a later REST fetch
recovers the durable state.

This is the direct, verified completion of the chain M8, M8.5, and
M8.6 were each one link of:

```text
M8   — durable notification    (Postgres, authoritative regardless of who's watching)
M8.5 — live hint                (best-effort, lossy by design, proven independent of the row)
M8.6 — frontend reconciliation  (hint → REST refetch → real, authoritative UI state)
     → usable notification state
```

What was true only from a browser console after M8.5 — that the row
survives independent of the hint — is now true from inside the product
itself: open the panel, and whatever was missed is simply there.
