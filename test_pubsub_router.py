"""
test_pubsub_router.py — Verify cross-process WebSocket event delivery
(Milestone 7.5).

Run against a live Redis instance:

    uv run test_pubsub_router.py

Two SEPARATE PubSubRouter instances are created here, each with its own
Redis connection — deliberately simulating two different processes
(e.g. main.py and worker.py) rather than testing a single instance
against itself, which would risk asserting against in-process state
instead of proving real Redis Pub/Sub delivery.

What it proves:
  1.  A publish from a router with ZERO local subscribers (simulating
      worker.py, which never calls register()) is delivered to a
      DIFFERENT router instance's locally-registered WebSocket.
  2.  Multiple local WebSockets registered under the same channel all
      receive a single publish (Set-based membership, ADR-2).
  3.  Unregistering one WebSocket stops delivery to it specifically,
      without affecting other subscribers on the same channel.
  4.  Publishing to a channel with zero subscribers anywhere is a safe
      no-op.
"""

import asyncio

from app.config import REDIS_URL
from app.ws_router import PubSubRouter

SEP = "—" * 56


class FakeWebSocket:
    def __init__(self, name: str):
        self.name = name
        self.received: list[str] = []

    async def send_text(self, data: str) -> None:
        self.received.append(data)


async def wait_for_delivery(fake_ws: "FakeWebSocket", timeout: float = 1.0) -> None:
    """Pub/Sub delivery is async over a real connection — poll briefly
    rather than assuming instantaneous delivery."""
    elapsed = 0.0
    interval = 0.02
    while not fake_ws.received and elapsed < timeout:
        await asyncio.sleep(interval)
        elapsed += interval


async def main() -> None:
    print(SEP)
    print(" FanoutFeed — PubSubRouter cross-process verification")
    print(SEP)

    router_worker = PubSubRouter()  # simulates worker.py — never registers
    router_main = PubSubRouter()  # simulates main.py — holds local subscribers
    await router_worker.init(REDIS_URL)
    await router_main.init(REDIS_URL)

    channel = "test:cross-process"

    print("\n[1] Cross-process delivery")
    ws_a = FakeWebSocket("a")
    await router_main.register(channel, ws_a)
    await router_worker.publish(channel, {"event": "TEST", "n": 1})
    await wait_for_delivery(ws_a)
    assert ws_a.received, "Publish from a router with no local subscribers never reached the other process"
    print("    ✅  router_worker (0 local subscribers) → router_main's WS received it")

    print("\n[2] Multi-subscriber delivery")
    ws_b = FakeWebSocket("b")
    await router_main.register(channel, ws_b)
    await router_worker.publish(channel, {"event": "TEST", "n": 2})
    await wait_for_delivery(ws_b)
    assert ws_a.received[-1] == ws_b.received[-1], "Both subscribers should have received the identical message"
    print("    ✅  Both ws_a and ws_b received the same publish")

    print("\n[3] Precise unregistration")
    await router_main.unregister(channel, ws_a)
    count_a_before, count_b_before = len(ws_a.received), len(ws_b.received)
    await router_worker.publish(channel, {"event": "TEST", "n": 3})
    await wait_for_delivery(ws_b)
    assert len(ws_a.received) == count_a_before, "Unregistered ws_a should NOT have received the next publish"
    assert len(ws_b.received) == count_b_before + 1, "Still-registered ws_b SHOULD have received the next publish"
    print("    ✅  ws_a (unregistered) received nothing further; ws_b (still registered) received it")

    print("\n[4] No-subscriber publish is a safe no-op")
    await router_worker.publish("test:nobody-listening", {"event": "TEST", "n": 4})
    await asyncio.sleep(0.2)
    print("    ✅  Publishing to a channel with no subscribers raised no error")

    await router_main.unregister(channel, ws_b)
    await router_worker.close()
    await router_main.close()

    print(f"\n{SEP}\n PubSubRouter verified — cross-process delivery, multi-subscriber")
    print(f" fan-out, and precise unregistration all confirmed.\n{SEP}")


if __name__ == "__main__":
    asyncio.run(main())