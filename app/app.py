import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uuid

from app.schemas import CreatePostBody
from app.event_bus import bus
from app.store import FOLLOWERS, FOLLOWS, POSTS, TIMELINES, USERS, Post
from app.consumers import fanout_consumer, realtime_consumer
from app.ws_manager import manager, system


@asynccontextmanager
async def lifespan(app: FastAPI):
    bus.subscribe("PostCreated", fanout_consumer)
    bus.subscribe("PostCreated", realtime_consumer)
    print("✅  Event bus ready — consumers registered")
    yield


app = FastAPI(title="FanoutFeed API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/users")
def list_users():
    return [{"id": u.id, "name": u.name} for u in USERS.values()]


@app.get("/users/{user_id}/following")
def get_following(user_id: str):
    return list(FOLLOWS.get(user_id, []))


@app.post("/users/{user_id}/follow/{target_id}")
def follow_user(user_id: str, target_id: str):
    if user_id == target_id:
        return {"error": "Cannot follow yourself"}
    if target_id not in USERS:
        return {"error": "Target user not found"}

    FOLLOWS.setdefault(user_id, set()).add(target_id)
    FOLLOWERS.setdefault(target_id, set()).add(user_id)
    return {"ok": True}


@app.delete("/users/{user_id}/follow/{target_id}")
def unfollow_user(user_id: str, target_id: str):
    FOLLOWS.setdefault(user_id, set()).discard(target_id)
    FOLLOWERS.setdefault(target_id, set()).discard(user_id)
    return {"ok": True}


@app.post("/posts")
async def create_post(author_id: str, body: CreatePostBody):
    if author_id not in USERS:
        return {"error": "User not found"}
    if not body.content.strip():
        return {"error": "Content cannot be empty"}

    post = Post(
        id=str(uuid.uuid4())[:8],
        author_id=author_id,
        author_name=USERS[author_id].name,
        content=body.content.strip(),
        created_at=time.time(),
    )
    POSTS[post.id] = post

    print(f"\n📝  POST CREATED  id={post.id!r}  author={author_id!r}")
    print(f"    Content: {post.content!r}")

    # Broadcast POST_CREATED to the event log panel first
    await system.broadcast(
        {
            "event": "POST_CREATED",
            "post_id": post.id,
            "author": post.author_name,
            "content": post.content,
            "ts": post.created_at,
        }
    )

    # fire the event — consumers run synchronously in order:
    # 1. fanout_consumer  (writes timelines)
    # 2. realtime_consumer (pushes to online followers)
    await bus.publish(
        "PostCreated",
        {
            "post_id": post.id,
            "author_id": author_id,
        },
    )

    return {"post_id": post.id}


@app.get("/timeline/{user_id}")
def get_timeline(user_id: str):
    post_ids = TIMELINES.get(user_id, [])
    posts = [POSTS[pid] for pid in post_ids if pid in POSTS]
    return [
        {
            "id": p.id,
            "author_id": p.author_id,
            "author_name": p.author_name,
            "content": p.content,
            "created_at": p.created_at,
        }
        for p in posts
    ]


@app.websocket("/ws/events")
async def system_events_ws(ws: WebSocket):
    """Broadcast channel: streams architecture events to the debug panel."""
    await system.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        system.disconnect(ws)


@app.websocket("/ws/{user_id}")
async def user_ws(ws: WebSocket, user_id: str):
    """Personal channel: receives NEW_POST notifications."""
    await manager.connect(user_id, ws)
    try:
        while True:
            await ws.receive_text()  # keep alive; client sends pings
    except WebSocketDisconnect:
        manager.disconnect(user_id)


@app.get("/health")
def health():
    return {"status": "ok"}
