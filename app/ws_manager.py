"""
ws_manager.py — Connection-semantics facades over PubSubRouter (Milestone 7.5).

Milestone 3 introduced Redis Pub/Sub routing for ConnectionManager, plus
a purely in-memory SystemBroadcaster that turned out to be process-local
— a known, explicitly-accepted limitation at the time. Milestone 4 then
moved fanout_consumer/realtime_consumer into a separate worker.py
process, which made that limitation concrete: every system.broadcast()
call from inside a consumer was reaching zero browser clients, silently.

Milestone 7.5 replaces both mechanisms with thin facades over one shared
PubSubRouter (app/ws_router.py). The distinction is deliberate:

    PubSubRouter        —   mechanism. Domain-agnostic: doesn't know what a
                            user is, doesn't know the ws:notify: naming
                            convention exists, doesn't know about auth.
    ConnectionManager   —   policy/domain layer. Owns the user_id -> channel
                            mapping, presence tracking (is_online), and is
                            the natural home for any future per-user
                            connection policy (auth scopes, session limits).
                            It carries real local state (_local_users) that
                            would still exist even if the transport changed
                            — that's what makes it more than a wrapper.
    SystemBroadcaster   —   thin semantic facade over the fixed
                            "system:events" channel. After this refactor it
                            carries no state of its own — kept for call-site
                            readability (system.broadcast(...) vs. a
                            repeated magic string) and as a low-cost future
                            extension seam, not because it's a domain object
                            in its own right.

ConnectionManager's old identity-guard on disconnect() (M4) — the
`if self._connections.get(user_id) is not ws: return` check — is GONE.
It existed only because the old design stored one WebSocket per user in
a plain dict, forcing "which connection is canonical" arbitration when a
second tab connected. Now that channels track a Set[WebSocket]
(PubSubRouter's register/unregister), removing exactly the socket that
closed is unambiguous by construction. EXPLICIT BEHAVIOR CHANGE: a user
with two open tabs now receives a push on BOTH, rather than the second
tab silently taking over.
"""

from typing import Dict, List, Set

from fastapi import WebSocket

from app.ws_router import PubSubRouter, router as _shared_router

_NOTIFY_PREFIX = "ws:notify:"


class ConnectionManager:
    """
    Owns user-facing connection-semantics. _local_users is real state
    that has nothing to do with the router — it would still be needed
    even if the underlying transport were swapped out entirely.
    """

    def __init__(self, router: PubSubRouter) -> None:
        self._router = router
        self._local_users: Dict[str, Set[WebSocket]] = {}

    async def connect(self, user_id: str, ws: WebSocket) -> None:
        await ws.accept()
        await self._router.register(f"{_NOTIFY_PREFIX}{user_id}", ws)
        self._local_users.setdefault(user_id, set()).add(ws)
        print(f"[WS] {user_id} connected {len(self._local_users[user_id])} local connections)")

    async def disconnect(self, user_id: str, ws: WebSocket) -> None:
        await self._router.unregister(f"{_NOTIFY_PREFIX}{user_id}", ws)
        subscribers = self._local_users.get(user_id)
        if subscribers is not None:
            subscribers.discard(ws)
            if not subscribers:
                del self._local_users[user_id]
        print(f"[WS] {user_id} disconnected")

    def is_online(self, user_id: str) -> bool:
        """True if THIS worker holds a connection for user_id."""
        return bool(self._local_users.get(user_id))

    def online_users(self) -> List[str]:
        return list(self._local_users.keys())

    async def send(self, user_id: str, data: dict) -> None:
        """
        Publish a notification for user_id. Callable from ANY process —
        including one (like worker.py) that never calls connect() and
        holds no local WebSockets at all.
        """
        await self._router.publish(f"{_NOTIFY_PREFIX}{user_id}", data)


class SystemBroadcaster:
    """
    Thin facade over the fixed "system:events" debug/architecture-event
    channel. Module docstring shares why this is kept rather than
    inlined at each call site, despite carrying no state of its own.
    """

    _CHANNEL = "system:events"

    def __init__(self, router: PubSubRouter) -> None:
        self._router = router

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        await self._router.register(self._CHANNEL, ws)

    async def disconnect(self, ws: WebSocket) -> None:
        await self._router.unregister(self._CHANNEL, ws)

    async def broadcast(self, data: dict) -> None:
        await self._router.publish(self._CHANNEL, data)


# Module-level singletons — both share the one PubSubRouter instance
# imported from ws_router.py. app.py / worker.py only init()/close() the
# router itself; neither facade has its own lifecycle method anymore.
manager = ConnectionManager(_shared_router)
system = SystemBroadcaster(_shared_router)
