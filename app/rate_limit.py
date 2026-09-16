"""
rate_limit.py — Sliding-window-log rate limiter (Milestone 9).

Own module, own Redis client, own init()/close() lifecycle — the same
one-module-one-Redis-responsibility pattern already used three times
over in this codebase (cache.py, event_bus.py, ws_router.py). See
docs/milestone-9-rate-limiting.md for the full design reasoning; the
module implements that design and does not introduce any semantics
beyond it.

-----------------------------------------------------------
Mechanism
-----------------------------------------------------------
Each (user_id, action) pair maps to a Redis sorted set:

    key     = "ratelimit:{user_id}:{action}"
    member  = a per-request unique token (NOT the timestamp — see below)
    score   = the request's timestamp

On every check, a single Lua scrip atomically:
    1.  Computes "now" from Redis's own clock (TIME), not the caller's
        system clock — this keeps every app.py worker process, no matter
        which host it runs on, checking against one authoritative time
        source rather than N independently-drifting clocks.
    2.  Prunes members with score <= (now - window) — i.e. everything
        that is no longer within the trailing (now-window, now) interval.
    3.  Count what remains.
    4.  If count < max, adds this request's entry and reports "allowed";
        otherwise reports "not allowed" and leaves the set untouched.
    5.  Returns the surviving oldest entry's score, so the caller can
        compute Retry-After from the ACTUAL sliding-window state rather
        than from Redis key TTL (which only reflects the memory-hygiene
        expiry, not individual entry aging — see the design doc's ADR-7).

Steps 2-4 must be one atomic unit: two concurrent requests from the
same user must not both observe the same pre-prune count and both be
allowed past the limit. A Lua script (single round-trip, executed
atomically by Redis) is how that's guaranteed here — see ADR-6.

-----------------------------------------------------------
Why the ZSET member is NOT the timestamp
-----------------------------------------------------------
The score is the timestamp; the member is a separate, per-request
unique token (uuid4 hex, generated in Python and passed into the
script). If the member were the timestamp itself, two requests
arriving in the same wall-clock instant (a real possibility under
concurrent requests, since Redis TIME has microseconds but not
infinite-precision resolution) would collide into the same ZSET
member and be silently counted once instead of twice — undercounting
exactly the burst behavior this limiter exists to cap. Score and
member are deliberately decoupled: the score carries the time
information the pruning logic needs; the member only needs to be
unique enough that ZADD never merges two distinct requests together.,

-----------------------------------------------------------
Fail-open scope
-----------------------------------------------------------
Only genuine Redis/infrastructure errors reaching this module are
fail-open (see rate_limit()'s dependency below, and M9 ADR-3: rate-limit
state is disposable enforcement state, not durable business state — a
lost or unreachable counter costs a temporary under-enforcement window,
not corrupted data). A programming error — e.g. rate_limit() called
with an action name that isn't in _POLICIES — is a bug, not an
infrastructure failure, and must NOT be silently fail-opened; it raises
immediately so it's caught in development rather than masked in
production as "no rate limiting happened and nobody knows why."
"""

import logging
import math
import uuid
from typing import Any, Optional, Tuple

import redis.asyncio as aioredis
from redis.exceptions import RedisError
from fastapi import Depends, HTTPException

from app.auth import get_current_user
from app.config import (
    RATE_LIMIT_FOLLOW_ACTION_MAX,
    RATE_LIMIT_FOLLOW_ACTION_WINDOW_SECONDS,
    RATE_LIMIT_POST_CREATE_MAX,
    RATE_LIMIT_POST_CREATE_WINDOW_SECONDS,
)

logger = logging.getLogger("app.rate_limit")

_client: Optional[aioredis.Redis] = None
_script: Any = None  # registered atomically once init_rate_limiter() runs

# action name -> (max requests, window seconds). Policy lives here, not
# in the Lua script or the dependency factory — the mechanism below is
# generic over any (max, window) pair; only this table encodes product
# decisions. See M9 ADR-4 for why follow_create/follow_delete share one
# entry rather than getting two.
_POLICIES: dict[str, tuple[int, int]] = {
    "post_create": (RATE_LIMIT_POST_CREATE_MAX, RATE_LIMIT_POST_CREATE_WINDOW_SECONDS),
    "follow_action": (RATE_LIMIT_FOLLOW_ACTION_MAX, RATE_LIMIT_FOLLOW_ACTION_WINDOW_SECONDS),
}

# KEYS[1] = ratelimit:{user_id}:{action}
# ARGV[1] = window (seconds)
# ARGV[2] = max_count
# ARGV[3] = unique member token for THIS request
# ARGV[4] = ttl (seconds) for the key — memory hygiene only, never used
#           to decide allow/deny (see module docstring and ADR-7).
#
# Returns [allowed (0/1), now (string, Redis server time), oldest_score
# (string, or Lua 'false' -> Nil if the set was empty)].
_LUA_SLIDING_WINDOW = """
local key = KEYS[1]
local window = tonumber(ARGV[1])
local max_count = tonumber(ARGV[2])
local member = ARGV[3]
local ttl = tonumber(ARGV[4])

local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000)

-- prune everything that has aged out of (now - window, now]
local cutoff = now - window
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

local count = redis.call('ZCARD', key)

local allowed = 0
if count < max_count then
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, ttl)
    allowed = 1
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local oldest_score = false
if #oldest > 0 then
    oldest_score = oldest[2]
end

return {allowed, tostring(now), oldest_score}
"""


async def init_rate_limiter(url: str) -> None:
    global _client, _script
    _client = aioredis.from_url(url, decode_responses=True)
    await _client.ping()
    _script = _client.register_script(_LUA_SLIDING_WINDOW)
    print("✅  Rate limiter ready")


async def close_rate_limiter() -> None:
    if _client:
        await _client.close()


def _key(user_id: str, action: str) -> str:
    return f"ratelimit:{user_id}:{action}"


async def check_rate_limit(user_id: str, action: str) -> Tuple[bool, Optional[float]]:
    """
    Low-level check, independent of fail-open policy. Returns
    (allowed, retry_after_seconds); retry_after_seconds is None whenever
    allowed is True

    Raises ValueError immediately for an unknown action (a programming
    error at the call site, not an infrastructure condition — see module
    docstring). Raises the underlying redis.exceptions.RedisError
    unmodified on any genuine Redis failure; deciding what to do about
    that failure is the caller's (rate_limit()'s) responsibility, kept
    out of this function so it stays pure, directly-testable
    implementation of the sliding-window mechanism.
    """
    if action not in _POLICIES:
        raise ValueError(f"Unknown rate-limit action: {action!r}")

    max_count, window = _POLICIES[action]
    key = _key(user_id, action)
    member = uuid.uuid4().hex  # unique per request — see module docstring
    ttl = window * 2  # memory hygiene only; never used in the allow/deny decision

    allowed_raw, now_raw, oldest_raw = await _script(
        keys=[key],
        args=[window, max_count, member, ttl],
    )

    allowed = bool(int(allowed_raw))
    if allowed or oldest_raw is None:
        return allowed, None

    now = float(now_raw)
    oldest = float(oldest_raw)
    # ceil, not floor/truncate: Retry-After must never tell the client to
    # retry a fraction of a second BEFORE the slot actually frees up.
    retry_after = max(0, math.ceil((oldest + window) - now))
    return allowed, retry_after


def rate_limit(action: str):
    """
    FastAPI dependency factory. Usage in a route signature:

        @app.post("/posts")
        async def create_post(
            body: CreatePostBody,
            current_user: dict = Depends(get_current_user),
            _: None = Depends(rate_limit("post_create")),
        ):
            ...

    Declares its own get_current_user sub-dependency rather than
    requiring the route to pass user_id through some other channel —
    FastAPI resolves shared dependencies once per request and reuses the
    result, so this does not decode the JWT twice. This keeps the
    precondition self-contained and visible in the route's dependency
    list, the same way auth already is (see M9 design doc, "Enforcement
    placement").
    """

    async def _dependency(current_user: dict = Depends(get_current_user)) -> None:
        user_id = current_user["sub"]
        try:
            allowed, retry_after = await check_rate_limit(user_id, action)
        except RedisError as e:
            # Fail OPEN — but loudly. A silent fail-open is a rate
            # limiter that has quietly stopped enforcing. This is the
            # ONLY exception type caught here: a ValueError from an
            # unknown action (a bug, not an infrastructure failure)
            # must propagate and fail loudly instead of being masked as
            # "allowed through."
            logger.warning(
                "[RateLimit] FAIL-OPEN for user=%s action=%s — infrastructure error: %r",
                user_id,
                action,
                e,
            )
            return

        if not allowed:
            headers = {"Retry-After": str(retry_after)} if retry_after is not None else {}
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded for {action!r}. Try again in {retry_after} seconds.",
                headers=headers,
            )

    return _dependency
