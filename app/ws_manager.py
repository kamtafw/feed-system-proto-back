from typing import Dict, List

from fastapi import WebSocket


class ConnectionManager:
    """Tracks one WebSocket per user (personal events: NEW_POST, etc.)"""

    def __init__(self):
        self._connections: Dict[str, WebSocket] = {}

    async def connect(self, user_id: str, ws: WebSocket) -> None:
        await ws.accept()
        self._connections[user_id] = ws
        print(f"[WS] {user_id} connected  ({len(self._connections)} online)")

    def disconnect(self, user_id: str) -> None:
        self._connections.pop(user_id, None)
        print(f"[WS] {user_id} disconnected  ({len(self._connections)} online)")

    def is_online(self, user_id: str) -> bool:
        return user_id in self._connections

    async def send(self, user_id: str, data: dict) -> None:
        ws = self._connections.get(user_id)
        if ws:
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(user_id)  # stale connection




class SystemBroadcaster:
    """
    Broadcasts architecture events to every connected debug client.
    The frontend's EventLog panel subscribes to /ws/events.
    """

    def __init__(self):
        self._clients: List[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        if ws in self._clients:
            self._clients.remove(ws)

    async def broadcast(self, data: dict) -> None:
        dead = []
        for ws in self._clients:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)

        for ws in dead:
            self._clients.remove(ws)


manager = ConnectionManager()
system = SystemBroadcaster()
