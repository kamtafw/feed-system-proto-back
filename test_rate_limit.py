"""
test_rate_limit.py — Verify the Milestone 9 sliding-window rate limiter
directly against a real Redis instance, exercising the actual
check_rate_limit() / rate_limit() functions app.py's routes call.

Run against a live Redis:

    uv run test_rate_limit.py

No Postgres, no HTTP layer, no event bus — the rate limiter has no
dependency on any of them (see M9 design doc, "The limiter is a
precondition gate"). This mirrors test_cursor_pagination.py's and
test_streams.py's approach of exercising real infrastructure directly
rather than through the full app.

What it proves, section by section:
  1.  Threshold behavior — exactly max_count requests allowed, the next
      one denied.
  2.  Boundary correctness — an entry seeded outside the trailing
      window is pruned; one seeded inside survives. No sleep() needed —
      this is the direct payoff of choosing a ZSET over INCR+EXPIRE
      (M9 design doc, Recon item 5 / ADR-5).
  3.  Retry-After accuracy — computed from the oldest surviving entry,
      not Redis TTL (ADR-7).
  4.  Concurrency / atomicity — N+5 concurrent requests for the same
      user resolve to exactly N allowed, proving the Lua script's
      atomic prune->count->conditional-add (ADR-6), not a race.
  5.  Bucket independence — exhausting post_create does not affect
      follow_action for the same user, and vice versa.
  6.  Shared follow_action bucket — the single bucket ADR-4 specifies
      drains under repeated calls exactly like any other bucket
      (the sharing itself is enforced by app.py's wiring, calling both
      follow and unfollow through the same action string — verified at
      the app.py level separately, this section verifies the mechanism
      has no special-cased behavior that would prevent sharing).
  7.  Fail-open — a simulated Redis/script failure is caught and
      allowed through, never raised as an HTTPException, and never
      silently absorbed without a log line.
  8.  Short real-time expiry — an actual (small) window observed to
      expire in real time, confirming the seeded-timestamp unit tests
      above match genuine wall-clock behavior end-to-end.
"""

import asyncio
import logging
import time
import uuid

from redis.exceptions import ConnectionError as RedisConnectionError

from app import rate_limit as rl
from app.config import REDIS_URL
from app.rate_limit import check_rate_limit

SEP = "—" * 56


def uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def section(n: int, title: str) -> None:
    print(f"\n[{n}] {title}")


async def cleanup_key(user_id: str, action: str) -> None:
    await rl._client.delete(rl._key(user_id, action))


async def main() -> None:
    await rl.init_rate_limiter(REDIS_URL)

    created_keys: list[tuple[str, str]] = []

    print(SEP)
    print(" FanoutFeed — Rate limiter verification (Milestone 9)")
    print(SEP)

    # [1] Threshold behavior
    await section(1, "Threshold — exactly max_count allowed, then denied")
    user1 = uid("threshold")
    max_count, window = rl._POLICIES["follow_action"]
    created_keys.append((user1, "follow_action"))

    for i in range(max_count):
        allowed, retry_after = await check_rate_limit(user1, "follow_action")
        assert allowed, f"request {i + 1}/{max_count} should be allowed"
        assert retry_after is None, "an allowed request must not carry a retry_after"

    allowed, retry_after = await check_rate_limit(user1, "follow_action")
    assert not allowed, f"request {max_count + 1} should be denied"
    assert retry_after is not None and retry_after > 0
    print(f"    ✅  {max_count} allowed, request {max_count + 1} denied with retry_after={retry_after}s")

    # [2] Boundary correctness — no sleep, direct ZSET seeding
    await section(2, "Boundary correctness — entries outside the window are pruned, inside survive")
    user2 = uid("boundary")
    action = "post_create"
    max_count2, window2 = rl._POLICIES[action]
    key2 = rl._key(user2, action)
    created_keys.append((user2, action))

    now = time.time()
    inside_member = "inside-" + uuid.uuid4().hex
    outside_member = "outside-" + uuid.uuid4().hex
    # comfortable 5s margin either side of the boundary to avoid flakiness
    # from the small clock delta between Python's time.time() (used to
    # seed) and Redis's own TIME() (used inside the script to prune)
    await rl._client.zadd(key2, {inside_member: now - (window2 - 5)})
    await rl._client.zadd(key2, {outside_member: now - (window2 + 5)})

    allowed, _ = await check_rate_limit(user2, action)
    assert allowed, "a single seeded in-window entry should not exhaust the limit"

    surviving = await rl._client.zrange(key2, 0, -1)
    assert inside_member in surviving, "entry inside the trailing window was incorrectly pruned"
    assert outside_member not in surviving, "entry outside the trailing window was NOT pruned — boundary bug"
    print(f"    ✅  in-window entry survived pruning, out-of-window entry was removed")

    # exact-boundary case: an entry scored at precisely (now - window) must
    # be excluded, per the (now-window, now] interval definition (ADR / Q6)
    user2b = uid("boundary-exact")
    key2b = rl._key(user2b, action)
    created_keys.append((user2b, action))
    exact_now = time.time()
    exact_member = "exact-" + uuid.uuid4().hex
    await rl._client.zadd(key2b, {exact_member: exact_now - window2})
    await check_rate_limit(user2b, action)  # triggers a prune pass
    surviving_exact = await rl._client.zrange(key2b, 0, -1)
    assert exact_member not in surviving_exact, "an entry exactly W seconds old must be excluded (exclusive lower bound)"
    print(f"    ✅  entry exactly at the window boundary was correctly excluded")

    # [3] Retry-After accuracy
    await section(3, "Retry-After — derived from the oldest surviving entry, not Redis TTL")
    user3 = uid("retryafter")
    action3 = "post_create"
    max_count3, window3 = rl._POLICIES[action3]
    key3 = rl._key(user3, action3)
    created_keys.append((user3, action3))

    now3 = time.time()
    oldest_age = 10.0
    seed = {f"seed-{i}-{uuid.uuid4().hex}": (now3 - oldest_age if i == 0 else now3 - 1) for i in range(max_count3)}
    await rl._client.zadd(key3, seed)

    allowed, retry_after = await check_rate_limit(user3, action3)
    assert not allowed, "seeded at max_count, the next request must be denied"
    expected = window3 - oldest_age
    assert retry_after is not None
    assert abs(retry_after - expected) <= 2, f"retry_after={retry_after} too far from expected~{expected}"
    print(f"    ✅  retry_after={retry_after}s ≈ expected {expected}s (oldest entry was {oldest_age}s old)")

    # [4] Concurrency / atomicity — the actual point of the Lua script
    await section(4, "Concurrency — N+5 simultaneous requests resolve to exactly N allowed")
    user4 = uid("concurrency")
    action4 = "follow_action"
    max_count4, _ = rl._POLICIES[action4]
    created_keys.append((user4, action4))
    total_attempts = max_count4 + 5

    results = await asyncio.gather(*[check_rate_limit(user4, action4) for _ in range(total_attempts)])
    allowed_count = sum(1 for allowed, _ in results if allowed)
    assert allowed_count == max_count4, f"expected exactly {max_count4} allowed under concurrency, got {allowed_count}"
    print(f"    ✅  {total_attempts} concurrent requests -> exactly {allowed_count} allowed (atomic, no over-admission)")

    # [5] Bucket independence
    await section(5, "Bucket independence — post_create and follow_action don't interfere")
    user5 = uid("independence")
    max_post, _ = rl._POLICIES["post_create"]
    created_keys.append((user5, "post_create"))
    created_keys.append((user5, "follow_action"))

    for _ in range(max_post):
        allowed, _ = await check_rate_limit(user5, "post_create")
        assert allowed
    allowed, _ = await check_rate_limit(user5, "post_create")
    assert not allowed, "post_create should now be exhausted"

    allowed, _ = await check_rate_limit(user5, "follow_action")
    assert allowed, "follow_action must be completely unaffected by post_create being exhausted"
    print(f"    ✅  exhausting post_create left follow_action fully available for the same user")

    # [6] Shared follow_action bucket (ADR-4) — mechanism-level check.
    # The follow-vs-unfollow sharing itself is an app.py wiring decision
    # (both routes call Depends(rate_limit("follow_action"))); this
    # confirms the mechanism drains one bucket under repeated calls with
    # no special-casing that would silently prevent that sharing from
    # working once wired.
    await section(6, "Shared follow_action bucket drains under alternating calls")
    user6 = uid("shared-bucket")
    max_count6, _ = rl._POLICIES["follow_action"]
    created_keys.append((user6, "follow_action"))

    for i in range(max_count6):
        allowed, _ = await check_rate_limit(user6, "follow_action")
        assert allowed, f"call {i + 1} (alternating follow/unfollow in production) should be allowed"
    allowed, _ = await check_rate_limit(user6, "follow_action")
    assert not allowed, "the shared bucket should be exhausted regardless of follow/unfollow intent"
    print(f"    ✅  {max_count6} alternating follow/unfollow-equivalent calls exhausted the single shared bucket")

    # [7] Fail-open
    await section(7, "Fail-open — a Redis/script failure is caught, allowed through, and logged")
    user7 = uid("failopen")
    orig_script = rl._script

    async def failing_script(*args, **kwargs):
        raise RedisConnectionError("simulated Redis outage")

    # Capture what rl.logger actually emits — "fail open" without a log
    # line is exactly the silent-failure mode the design explicitly
    # rejected ("loss of enforcement must be visible").
    captured: list[logging.LogRecord] = []

    class _CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    handler = _CaptureHandler()
    rl.logger.addHandler(handler)
    rl.logger.setLevel(logging.WARNING)

    rl._script = failing_script
    try:
        dependency = rl.rate_limit("post_create")
        # Call the dependency function directly with a fake resolved
        # current_user, the same technique test_notification_hints.py
        # uses to exercise a function's real body without the full
        # FastAPI/HTTP stack.
        await dependency(current_user={"sub": user7, "name": user7})
        print("    ✅  rate_limit() dependency swallowed the infrastructure failure without raising")
    finally:
        rl._script = orig_script
        rl.logger.removeHandler(handler)

    assert any("FAIL-OPEN" in r.getMessage() for r in captured), "fail-open must be logged, not silent"
    print(f"    ✅  fail-open event was logged: {captured[-1].getMessage()}")

    # a genuine programming error (unknown action) must NOT be silently
    # fail-opened — it's a bug, not an infrastructure failure
    try:
        await check_rate_limit(user7, "not_a_real_action")
        raise AssertionError("an unknown action should raise ValueError, not silently succeed")
    except ValueError:
        print("    ✅  an unknown action raises ValueError immediately rather than being fail-opened")

    # [8] Short real-time expiry — genuine wall-clock confirmation
    await section(8, "Short real-time window expiry (small window, real sleep — like test_streams.py)")
    user8 = uid("realtime")
    rl._POLICIES["_test_short"] = (2, 2)  # 2 requests / 2 seconds, test-only policy
    try:
        allowed1, _ = await check_rate_limit(user8, "_test_short")
        allowed2, _ = await check_rate_limit(user8, "_test_short")
        allowed3, retry_after3 = await check_rate_limit(user8, "_test_short")
        assert allowed1 and allowed2, "first two requests within the tiny window should be allowed"
        assert not allowed3, "third request should be denied"
        assert retry_after3 is not None and retry_after3 >= 1

        wait_s = retry_after3 + 0.5
        print(f"    Waiting {wait_s}s for the window to actually expire...", end="", flush=True)
        await asyncio.sleep(wait_s)
        print(" done")

        allowed4, _ = await check_rate_limit(user8, "_test_short")
        assert allowed4, "a request after the real window has elapsed should be allowed again"
        print("    ✅  capacity genuinely returned after real wall-clock time passed")
    finally:
        del rl._POLICIES["_test_short"]
        await cleanup_key(user8, "_test_short")

    # Cleanup
    for u, a in created_keys:
        await cleanup_key(u, a)

    print(f"\n{SEP}")
    print(" Rate limiter verified — threshold, boundary, Retry-After,")
    print(" atomicity under concurrency, bucket isolation, shared-bucket")
    print(" draining, fail-open, and real-time expiry all confirmed.")
    print(SEP)

    await rl.close_rate_limiter()


if __name__ == "__main__":
    asyncio.run(main())