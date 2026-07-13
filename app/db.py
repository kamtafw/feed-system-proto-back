"""
db.py — PostgreSQL layer via asyncpg.

Replaces the in-memory USERS, POSTS, FOLLOWS, FOLLOWERS dicts in store.py.

Connection pool (min=2, max=10) is appropriate for 800-1000 users on a single
VPS. Each request acquires a connection from the pool, uses it, and returns it —
no connection per request overhead.
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

-- Fanout consumer queries followers by followee_id
CREATE INDEX IF NOT EXISTS idx_follows_followee    ON follows(followee_id);
-- Auth lookups by user
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user ON refresh_tokens(user_id);
-- Post author index
CREATE INDEX IF NOT EXISTS idx_posts_author        ON posts(author_id);
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
