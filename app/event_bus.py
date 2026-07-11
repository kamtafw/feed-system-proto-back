from typing import Dict, List, Callable, Awaitable


class InMemoryEventBus:
    """
    Synchronous-style pub/sub: publish() awaits each handler in order.
    This means fanout completes before realtime fires — intentional for
    the prototype so the timeline is always ready before the WS push.
    """

    def __init__(self):
        self._handlers: Dict[str, List[Callable[..., Awaitable]]] = {}

    def subscribe(self, event_type: str, handler) -> None:
        self._handlers.setdefault(event_type, []).append(handler)

    async def publish(self, event_type: str, payload: dict) -> None:
        for handler in self._handlers.get(event_type, []):
            await handler(payload)


bus = InMemoryEventBus()
