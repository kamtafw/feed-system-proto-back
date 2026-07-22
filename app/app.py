"""
app.py — FastAPI application.

Milestone 3 change: manager.init(REDIS_URL) and manager.close() added to
the lifespan. ConnectionManager now needs Redis to set up the Pub/Sub
listener before any WebSocket connection arrive.

Initialisation order matters:
    1. db       —   PostgreSQL pool (consumers depend on it)
    2. cache    —   Redis timeline ops (consumers depend on it)
    3. bus      —   Redis Streams event bus
    4. manager  —   Redis Pub/Sub WebSocket router (must be ready before
                    the first WebSocket connection, which could arrive the
                    moment uvicorn starts accepting requests)
    5. listener —   starts consuming from Redis Streams

Teardown is the reverse: cancel listener first so no new events are
processed while the connections below it are closing.
"""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app import db
from app import cache
from app.auth import (
    create_access_token,
    generate_refresh_token,
    get_current_user,
    get_ws_user,
    hash_password,
    hash_refresh_token,
    verify_password,
)
from app.config import DATABASE_URL, REDIS_URL, REFRESH_TOKEN_EXPIRE_DAYS
from app.consumers import fanout_consumer, realtime_consumer
from app.event_bus import bus
from app.ws_manager import manager, system

# Lifespan

_SEED_PASSWORD = "password123"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Storage
    await db.init_db(DATABASE_URL)
    await cache.init_cache(REDIS_URL)

    # Event bus (Redis Streams)
    await bus.init(REDIS_URL)
    bus.subscribe("PostCreated", fanout_consumer)
    bus.subscribe("PostCreated", realtime_consumer)

    # WebSocket router (Redis Pub/Sub) — Milestone 3
    await manager.init(REDIS_URL)

    # Seed default passwords for seed users (idempotent)
    hashed = hash_password(_SEED_PASSWORD)
    for uid in ("alice", "bob", "carol", "dave"):
        await db.set_password_hash(uid, hashed)

    # Start event stream listener
    listener = asyncio.create_task(bus.listen())
    print(f"✅  All systems ready  (seed password: {_SEED_PASSWORD!r})")

    yield  # ← server runs here

    # Teardown (reverse order)
    listener.cancel()
    try:
        await listener
    except asyncio.CancelledError:
        pass

    await manager.close()  # stop Pub/Sub listener, disconnect Redis
    await bus.close()
    await cache.close_cache()
    await db.close_db()


app = FastAPI(title="FanoutFeed", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# Auth helpers


def _refresh_expiry() -> float:
    return time.time() + REFRESH_TOKEN_EXPIRE_DAYS * 86_400


async def _issue_tokens(user_id: str, name: str) -> dict:
    """Create an access token and a fresh refresh token, persist the refresh token."""
    access = create_access_token(user_id, name)
    raw_ref = generate_refresh_token()
    await db.store_refresh_token(user_id, hash_refresh_token(raw_ref), _refresh_expiry())
    return {
        "access_token": access,
        "refresh_token": raw_ref,
        "token_type": "bearer",
    }


# Auth routes


class RegisterBody(BaseModel):
    username: str
    password: str
    display_name: str = ""  # optional; defaults to username


class LoginBody(BaseModel):
    username: str
    password: str


class RefreshBody(BaseModel):
    refresh_token: str


@app.post("/auth/register", status_code=201)
async def register(body: RegisterBody):
    user_id = body.username.strip().lower()
    if not user_id:
        raise HTTPException(400, "Username cannot be empty")

    if await db.get_user(user_id):
        raise HTTPException(409, "Username already taken")

    name = body.display_name.strip() or user_id
    user = await db.create_user(user_id, name, hash_password(body.password))
    tokens = await _issue_tokens(user["id"], user["name"])
    return {**tokens, "user": user}


@app.post("/auth/login")
async def login(body: LoginBody):
    user_id = body.username.strip().lower()
    user = await db.get_user_with_password(user_id)

    # constant-time: always call verify_password even when user not found,
    # to prevent timing-based username enumeration
    dummy_hash = "$2b$12$notarealhashjustpadding000000000000000000000000000000"
    stored_hash = user["password_hash"] if (user and user.get("password_hash")) else dummy_hash

    if not verify_password(body.password, stored_hash) or not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )

    tokens = await _issue_tokens(user["id"], user["name"])
    return {**tokens, "user": {"id": user["id"], "name": user["name"]}}


@app.post("/auth/refresh")
async def refresh(body: RefreshBody):
    token_hash = hash_refresh_token(body.refresh_token)
    record = await db.get_refresh_token(token_hash)

    if not record:
        raise HTTPException(401, "Invalid refresh token")

    if record["revoked"]:
        # token already used — possible theft; revoke every session for this user
        await db.revoke_all_user_refresh_tokens(record["user_id"])
        raise HTTPException(
            401,
            "Refresh token already used — all sessions have been revoked." "Please log in again.",
        )

    if record["expires_at"] < time.time():
        raise HTTPException(401, "Refresh token expired")

    # rotate: revoke old token, issue fresh pair
    await db.revoke_refresh_token(token_hash)
    user = await db.get_user(record["user_id"])
    if not user:
        raise HTTPException(401, "Invalid refresh token")
    return await _issue_tokens(user["id"], user["name"])


@app.post("/auth/logout")
async def logout(body: RefreshBody):
    """
    Revoke the refresh token. The access token expires naturally (15 min).
    There's no server-side access token blacklist — that would require a DB
    lookup on every request, eliminating the main advantage of JWTs.
    """
    await db.revoke_refresh_token(hash_refresh_token(body.refresh_token))
    return {"ok": True}


# Users (public)


@app.get("/users")
async def list_users():
    return await db.get_all_users()


@app.get("/users/{user_id}/following")
async def get_following_public(user_id: str):
    return await db.get_following(user_id)


# Me — authenticated user's own actions


@app.get("/me/following")
async def get_my_following(current_user: dict = Depends(get_current_user)):
    return await db.get_following(current_user["sub"])


@app.post("/me/follow/{target_id}")
async def follow_user(
    target_id: str,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["sub"]
    if user_id == target_id:
        raise HTTPException(400, "Cannot follow yourself")
    if not await db.get_user(target_id):
        raise HTTPException(404, "User not found")
    await db.add_follow(user_id, target_id)
    return {"ok": True}


@app.delete("/me/follow/{target_id}")
async def unfollow_user(
    target_id: str,
    current_user: dict = Depends(get_current_user),
):
    await db.remove_follow(current_user["sub"], target_id)
    return {"ok": True}


# Posts


class CreatePostBody(BaseModel):
    content: str


@app.post("/posts")
async def create_post(
    body: CreatePostBody,
    current_user: dict = Depends(get_current_user),  # author_id now from token
):
    author_id = current_user["sub"]
    user = await db.get_user(author_id)
    if not user:
        raise HTTPException(401, "Invalid user")
    if not body.content.strip():
        raise HTTPException(400, "Content cannot be empty")

    post_id = str(uuid.uuid4())[:8]
    created_at = time.time()
    content = body.content.strip()

    await db.create_post(post_id, author_id, user["name"], content, created_at)

    await system.broadcast(
        {
            "event": "POST_CREATED",
            "post_id": post_id,
            "author": user["name"],
            "content": content,
            "ts": created_at,
        }
    )

    await bus.publish(
        "PostCreated",
        {
            "post_id": post_id,
            "author_id": author_id,
            "author_name": user["name"],
            "created_at": created_at,
        },
    )

    return {"post_id": post_id}


# Timeline


@app.get("/timeline/{user_id}")
async def get_timeline(user_id: str, limit: int = 50, offset: int = 0):
    """
    Two-step read:
        1. Get post IDs from Redis sorted set (fast, ordered)
        2. Fetch post bodies from PostgreSQL using ANY($1::text[]) (single query)

    The Redis step is O(log n + limit). The Postgres step is one indexed scan.
    Total cost is effectively constant regardless of timeline length.
    """
    post_ids = await cache.get_timeline_ids(user_id, limit=limit, offset=offset)
    return await db.get_posts_by_ids(post_ids)


# WebSockets
# Note: /ws/events must be defined BEFORE /ws/feed to avoid route shadowing.


@app.websocket("/ws/events")
async def system_events_ws(ws: WebSocket):
    """Public debug event stream. Per-worker — shows events from this process."""
    await system.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        system.disconnect(ws)


@app.websocket("/ws/feed")
async def feed_ws(
    ws: WebSocket,
    current_user: dict = Depends(get_ws_user),  # token from ?token= query param
):
    """
    Authenticated personal feed channel.
    Connect: ws://localhost:8000/ws/feed?token=<access_token>
    Receives: { type: "NEW_POST", post_id, author_id, author_name }

    On connect: this worker subscribes to ws:notify:{user_id} on Redis.
    On disconnect: this worker unsubscribes.
    Notifications arrive via Redis Pub/Sub from any worker.
    """
    user_id = current_user["sub"]
    await manager.connect(user_id, ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(user_id)
