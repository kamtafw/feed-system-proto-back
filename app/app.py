"""
app.py — FastAPI app with updated lifespan and async routes.

Key differences from the prototype:
    - lifespan initialises PostgreSQL pool + Redis + event bus in order
    - bus.listen() runs as a background asyncio task (not inline per request)
    - All routes are async (asyncpg requires it)
    - POST /posts returns before consumers run (fire-and-forget via Redis pub)
    - GET /timeline resolves IDs from Redis then fetches posts from Postgres
"""

import asyncio
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app import db
from app import cache
from app.config import DATABASE_URL, REDIS_URL
from app.event_bus import bus
from app.ws_manager import manager, system
from app.consumers import fanout_consumer, realtime_consumer


# Lifespan

@asynccontextmanager
async def lifespan(app: FastAPI):
    # initialise in dependency order: storage first, bus last
    await db.init_db(DATABASE_URL)
    await cache.init_cache(REDIS_URL)
    await bus.init(REDIS_URL)

    # register consumers (fanout must be first — runs before realtime)
    bus.subscribe("PostCreated", fanout_consumer)
    bus.subscribe("PostCreated", realtime_consumer)

    # start the Redis subscription loop as a background task
    # without this task, published events have no listener
    listener = asyncio.create_task(bus.listen())
    print("✅  All systems ready")

    yield  # ← server runs here

    # graceful shutdown
    listener.cancel()
    try:
        await listener
    except asyncio.CancelledError:
        pass

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


# Users

@app.get("/users")
async def list_users():
    return await db.get_all_users()


@app.get("/users/{user_id}/following")
async def get_following(user_id: str):
    return await db.get_following(user_id)


@app.post("/users/{user_id}/follow/{target_id}")
async def follow_user(user_id: str, target_id: str):
    if user_id == target_id:
        return {"error": "Cannot follow yourself"}
    if not await db.get_user(target_id):
        return {"error": "Target user not found"}
    await db.add_follow(user_id, target_id)
    return {"ok": True}


@app.delete("/users/{user_id}/follow/{target_id}")
async def unfollow_user(user_id: str, target_id: str):
    await db.remove_follow(user_id, target_id)
    return {"ok": True}


# Posts

class CreatePostBody(BaseModel):
    content: str


@app.post("/posts")
async def create_post(author_id: str, body: CreatePostBody):
    user = await db.get_user(author_id)
    if not user:
        return {"error": "User not found"}
    if not body.content.strip():
        return {"error": "Content cannot be empty"}

    post_id = str(uuid.uuid4())[:8]
    created_at = time.time()
    content = body.content.strip()

    # 1. persist post to PostgreSQL
    await db.create_post(post_id, author_id, user["name"], content, created_at)

    # 2. broadcast to debug panel
    await system.broadcast(
        {
            "event": "POST_CREATED",
            "post_id": post_id,
            "author": user["name"],
            "content": content,
            "ts": created_at,
        }
    )

    # 3. publish event — consumers run asynchronously in the listen() task
    #    created_at is included so fanout_consumer can use it as the sorted
    #    set score without an extra DB round-trip.
    await bus.publish(
        "PostCreated",
        {
            "post_id": post_id,
            "author_id": author_id,
            "author_name": user["name"],
            "created_at": created_at,
        },
    )

    # returns immediately — fanout and realtime happen in the background
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

@app.websocket("/ws/events")
async def system_events_ws(ws: WebSocket):
    await system.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        system.disconnect(ws)

@app.websocket("/ws/{user_id}")
async def user_ws(ws: WebSocket, user_id: str):
    if not await db.get_user(user_id):
        await ws.close(code=4004)
        return
    await manager.connect(user_id, ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(user_id)
