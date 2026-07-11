import time

from app.store import FOLLOWERS, TIMELINES, USERS
from app.ws_manager import manager, system


async def fanout_consumer(payload: dict) -> None:
    """
    Fanout-on-Write: copy the post ID into every follower's timeline.
    Think of this like a mail carrier delivering copies of a letter
    to every address on a distribution list.
    """
    post_id = payload["post_id"]
    author_id = payload["author_id"]
    followers = list(FOLLOWERS.get(author_id, set()))

    print(f"\n📢  [Fanout] PostCreated → post={post_id!r}  author={author_id!r}")
    print(f"    Followers to fan out to: {followers}")

    await system.broadcast(
        {
            "event": "FANOUT_START",
            "post_id": post_id,
            "author": USERS[author_id].name,
            "followers": followers,
            "ts": time.time(),
        }
    )

    for follower_id in followers:
        TIMELINES[follower_id].insert(0, post_id)
        print(f"    ✅  Inserted into {follower_id}'s timeline")
        await system.broadcast(
            {
                "event": "FANOUT_WRITE",
                "target": follower_id,
                "post_id": post_id,
                "ts": time.time(),
            }
        )

    # Author sees their own post too
    TIMELINES[author_id].insert(0, post_id)
    print(f"    ✅  Inserted into author {author_id}'s own timeline")


async def realtime_consumer(payload: dict) -> None:
    """
    Realtime push: for every follower that currently has an open
    WebSocket, fire a NEW_POST notification. Like a switchboard
    operator routing calls only to phones that are off the hook.
    """
    post_id = payload["post_id"]
    author_id = payload["author_id"]
    author_name = USERS[author_id].name
    followers = list(FOLLOWERS.get(author_id, set()))

    online = [f for f in followers if manager.is_online(f)]
    offline = [f for f in followers if not manager.is_online(f)]

    print(f"\n⚡  [Realtime] Checking online followers of {author_id!r}")
    print(f"    Online:  {online}")
    print(f"    Offline: {offline}")

    await system.broadcast(
        {
            "event": "REALTIME_START",
            "author": author_name,
            "online": online,
            "offline": offline,
            "ts": time.time(),
        }
    )

    for follower_id in online:
        await manager.send(
            follower_id,
            {
                "type": "NEW_POST",
                "post_id": post_id,
                "author_id": author_id,
                "author_name": author_name,
            },
        )
        print(f"    📱  NEW_POST sent → {follower_id}")
        await system.broadcast(
            {
                "event": "REALTIME_SEND",
                "target": follower_id,
                "ts": time.time(),
            }
        )

    for follower_id in offline:
        await system.broadcast(
            {
                "event": "REALTIME_SKIP",
                "target": follower_id,
                "reason": "offline",
                "ts": time.time(),
            }
        )
