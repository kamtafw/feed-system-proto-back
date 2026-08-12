"""
db.py — PostgreSQL layer via asyncpg.

Replaces the in-memory USERS, POSTS, FOLLOWS, FOLLOWERS dicts in store.py.

Connection pool (min=2, max=10) is appropriate for 800-1000 users on a single
VPS. Each request acquires a connection from the pool, uses it, and returns it —
no connection per request overhead.

Milestone 8: adds the `notifications` table and its CRUD functions. This
module remains the ONLY place raw SQL is written — app/notifications.py
(the domain layer) never touches asyncpg directly, same boundary already
kept between app.py and cache.py for Redis.
"""

import asyncpg
import time
import uuid
from typing import List, Optional
from app.config import DB_SSL

pool: Optional[asyncpg.Pool] = None


# Schema

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    password_hash   TEXT          -- NULL for legacy seed users until they register
);

CREATE TABLE IF NOT EXISTS posts (
    id          TEXT             PRIMARY KEY,
    author_id   TEXT             NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    author_name TEXT             NOT NULL,
    content     TEXT             NOT NULL,
    created_at  DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS follows (
    follower_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    followee_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    PRIMARY KEY (follower_id, followee_id)
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id          TEXT             PRIMARY KEY,
    user_id     TEXT             NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash  TEXT             NOT NULL UNIQUE,
    expires_at  DOUBLE PRECISION NOT NULL,
    created_at  DOUBLE PRECISION NOT NULL,
    revoked     BOOLEAN          NOT NULL DEFAULT FALSE
);

-- Milestone 8: persistent notification store.
--
-- Identity is (recipient_id, actor_id, type, object_type, object_id) — 
-- a domain concept, deliberately NOT a Redis Stream message ID (see
-- architecture-review.md ADR-1). This is what makes create_notification()
-- below safe under Streams' at least-once redelivery: a retried event
-- produces the same identity tuple and is silently absorbed by ON
-- CONFLICT DO NOTHING rather than duplicating a row.
--
-- object_type/object_id are NEVER NULL, even for NEW_FOLLOWER (which has
-- no natural "object" distinct from the recipient) — Postgres treats
-- NULL <> NULL inside a UNIQUE constraint, so a nullable object column
-- would silently stop deduplicating for exactly that notification type.
-- NEW_FOLLOWER use a self-referential object instead (object_type='user',
-- object_id=recipient_id) — see app/notifications.py
CREATE TABLE IF NOT EXISTS notifications (
    id            BIGSERIAL        PRIMARY KEY,
    recipient_id  TEXT             NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    actor_id      TEXT             NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type          TEXT             NOT NULL,   -- 'NEW_POST' | 'NEW_FOLLOWER'
    object_type   TEXT             NOT NULL,   -- 'post' | 'user'
    object_id     TEXT             NOT NULL,
    created_at    DOUBLE PRECISION NOT NULL,
    read_at       DOUBLE PRECISION,            -- NULL = unread
    
    UNIQUE (recipient_id, actor_id, type, object_type, object_id)
);

-- Fanout consumer queries followers by followee_id
CREATE INDEX IF NOT EXISTS idx_follows_followee    ON follows(followee_id);
-- Auth lookups by user
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user ON refresh_tokens(user_id);
-- Post author index
CREATE INDEX IF NOT EXISTS idx_posts_author        ON posts(author_id);

-- Notification read paths. Both are ordered (created_at DESC, id DESC) to
-- match the cursor tie-break in get_notifications() below — a batch of
-- notifications from one on_post_created() fanout run shares a single
-- created_at, so created_at alone is not a stable sort key here the way
-- it is for M6's per-post timeline cursor. The partial index covers the
-- hot "unread since last check" query and stays small regardless of how
-- large total notification history grows; the general index covers full
-- paginated history.
CREATE INDEX IF NOT EXISTS idx_notifications_unread
    ON notifications (recipient_id, created_at DESC, id DESC)
    WHERE read_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_notifications_recipient
    ON notifications (recipient_id, created_at DESC, id DESC);
"""


# Seed data (mirrors the prototype's follow graph)

_SEED_USERS = [
    ("alice", "Alice"),
    ("bob", "Bob"),
    ("carol", "Carol"),
    ("dave", "Dave"),
]

_SEED_FOLLOWS = [
    ("alice", "bob"),
    ("alice", "carol"),
    ("bob", "alice"),
    ("carol", "alice"),
    ("carol", "bob"),
]

# Lifecycle


async def init_db(dsn: str) -> None:
    global pool
    # Supabase (and most hosted Postgres) requires SSL
    # asyncpg doesn't read sslmode from the DSN string — it must be passed explicitly
    ssl = "require" if DB_SSL else None
    pool = await asyncpg.create_pool(dsn, ssl=ssl, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
        # ADD COLUMN is idempotent via IF NOT EXISTS — safe on existing DBs
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT")
        await conn.executemany(
            "INSERT INTO users (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            _SEED_USERS,
        )
        await conn.executemany(
            "INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            _SEED_FOLLOWS,
        )
    print("✅  PostgreSQL ready")


async def close_db() -> None:
    if pool:
        await pool.close()


# Users


async def get_all_users() -> List[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name FROM users ORDER BY name")
        return [dict(r) for r in rows]


async def get_user(user_id: str) -> Optional[dict]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, name FROM users WHERE id = $1", user_id)
        return dict(row) if row else None


async def get_user_with_password(user_id: str) -> Optional[dict]:
    """Includes password_hash. Only used by the login endpoint."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, name, password_hash FROM users WHERE id = $1", user_id)
        return dict(row) if row else None


async def create_user(user_id: str, name: str, password_hash: str) -> dict:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO users (id, name, password_hash) VALUES ($1, $2, $3) " "RETURNING id, name",
            user_id,
            name,
            password_hash,
        )
        return dict(row)


async def set_password_hash(user_id: str, password_hash: str) -> None:
    """Set password for a user that doesn't have one yet (e.g. seed users)."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET password_hash = $1 " "WHERE id = $2 AND password_hash IS NULL",
            password_hash,
            user_id,
        )


# Refresh tokens


async def store_refresh_token(user_id: str, token_hash: str, expires_at: float) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at, created_at)
            VALUES ($1, $2, $3, $4, $5)
            """,
            str(uuid.uuid4()),
            user_id,
            token_hash,
            expires_at,
            time.time(),
        )


async def get_refresh_token(token_hash: str) -> Optional[dict]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, user_id, expires_at, revoked " "FROM refresh_tokens WHERE token_hash = $1",
            token_hash,
        )
        return dict(row) if row else None


async def revoke_refresh_token(token_hash: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE refresh_tokens SET revoked = TRUE WHERE token_hash = $1",
            token_hash,
        )


async def revoke_all_user_refresh_tokens(user_id: str) -> None:
    """Revoke every session for a user — used on theft detection."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE refresh_tokens SET revoked = TRUE WHERE user_id = $1",
            user_id,
        )


# Posts


async def create_post(
    post_id: str,
    author_id: str,
    author_name: str,
    content: str,
    created_at: float,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO posts (id, author_id, author_name, content, created_at)
            VALUES ($1, $2, $3, $4, $5)
            """,
            post_id,
            author_id,
            author_name,
            content,
            created_at,
        )


async def get_posts_by_ids(post_ids: List[str]) -> List[dict]:
    """
    Fetch posts for a list of IDs and return them in the SAME ORDER as post_ids.

    The timeline sorted set gives us IDs newest-first. Postgres returns rows in
    arbitrary order. We re-sort here to preserve the timeline's order.
    """
    if not post_ids:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, author_id, author_name, content, created_at " "FROM posts WHERE id = ANY($1::text[])",
            post_ids,
        )
    row_map = {r["id"]: dict(r) for r in rows}
    # Preserve the order Redis gave us
    return [row_map[pid] for pid in post_ids if pid in row_map]


# Follows


async def get_followers(user_id: str) -> List[str]:
    """Return IDs of everyone who follows user_id (used by fanout consumer)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT follower_id FROM follows WHERE followee_id = $1", user_id)
        return [r["follower_id"] for r in rows]


async def get_following(user_id: str) -> List[str]:
    """Return IDs of everyone user_id follows (used by sidebar UI)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT followee_id FROM follows WHERE follower_id = $1", user_id)
        return [r["followee_id"] for r in rows]


async def add_follow(follower_id: str, followee_id: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2) " "ON CONFLICT DO NOTHING",
            follower_id,
            followee_id,
        )


async def remove_follow(follower_id: str, followee_id: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM follows WHERE follower_id = $1 AND followee_id = $2",
            follower_id,
            followee_id,
        )


# Notifications (Milestone 8)


async def create_notification(
    recipient_id: str,
    actor_id: str,
    notif_type: str,
    object_type: str,
    object_id: str,
    created_at: float,
) -> Optional[dict]:
    """
    Idempotent insert — the load-bearing correctness guarantee for
    running under Redis Streams' at-least-once delivery. A retried
    PostCreated/FollowCreated event resolves to the same identity tuple
    and is silently absorbed by ON CONFLICT DO NOTHING rather than
    duplicating a row.

    Returns the inserted row, or None if a notification with this exact
    identity already existed. For NEW_FOLLOWER specifically, "already
    existed" can also mean a genuine unfollow-then-refollow — that's a
    deliberate product decision (M8 ADR-3), not just infrastructure
    dedup: refollowing does not resurrect an already-read notification.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO notifications
                (recipient_id, actor_id, type, object_type, object_id, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (recipient_id, actor_id, type, object_type, object_id)
            DO NOTHING
            RETURNING id, recipient_id, actor_id, type, object_type, object_id, 
                created_at, read_at
            """,
            recipient_id,
            actor_id,
            notif_type,
            object_type,
            object_id,
            created_at,
        )
        return dict(row) if row else None


async def get_notifications(
    recipient_id: str,
    cursor_created_at: Optional[float] = None,
    cursor_id: Optional[int] = None,
    limit: int = 50,
) -> List[dict]:
    """
    Cursor pagination keyed on (created_at, id), not created_at alone.

    This deliberately diverges from M6's timeline cursor (score-only),
    because the two situations differ in exactly the ways that made M6's
    single-value cursor safe: a post's created_at comes from one HTTP
    request, naturally spread out in time, and M6's Redis sorted set had
    no cheap way to add a tie-breaker. Here, on_post_created() captures
    ONE created_at per fanout batch — many notifications for the same
    recipient can legitimately share an identical timestamp — and a
    composite Postgres predicate costs nothing extra. See
    M8 architecture-review.md for the full comparison.
    """
    async with pool.acquire() as conn:
        if cursor_created_at is None:
            rows = await conn.fetch(
                """
                SELECT id, recipient_id, actor_id, type, object_type, object_id,
                    created_at, read_at
                FROM notifications
                WHERE recipient_id = $1
                ORDER BY created_at DESC, id DESC
                LIMIT $2
                """,
                recipient_id,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT id, recipient_id, actor_id, type, object_type, object_id,
                    created_at, read_at
                FROM notifications
                WHERE recipient_id = $1
                    AND (created_at, id) < ($2, $3)
                ORDER BY created_at DESC, id DESC
                LIMIT $4
                """,
                recipient_id,
                cursor_created_at,
                cursor_id,
                limit,
            )
        return [dict(r) for r in rows]


async def mark_notification_read(notification_id: int, recipient_id: str) -> bool:
    """
    Recipient-scoped update — the WHERE clause IS the ownership check.
    A notification_id that exists but belongs to someone else matches
    zero rows, identically to a nonexistent id; the caller can't
    distinguish "not found" from "not yours," and shouldn't (the id
    itself is just an opaque client handle, not a security boundary —
    ownership is).

    COALESCE(read_at, $3) makes this idempotent on the read_at VALUE:
    marking an already-read notification as read again still matches
    (returns True — not "found and updated," just "found and belongs to
    you"), but doesn't bump its read_at timestamp on repeat calls.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE notifications
            SET read_at = COALESCE(read_at, $3)
            WHERE id = $1 AND recipient_id = $2
            RETURNING id
            """,
            notification_id,
            recipient_id,
            time.time(),
        )
        return row is not None


async def mark_all_notifications_read(recipient_id: str) -> int:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE notifications
            SET read_at = $2
            WHERE recipient_id = $1 AND read_at IS NULL
            """,
            recipient_id,
            time.time(),
        )
        # asyncpg's execute() returns a command tag string, e.g. "UPDATE 3"
        return int(result.split()[-1])


async def count_unread_notifications(recipient_id: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND read_at IS NULL",
            recipient_id,
        )
