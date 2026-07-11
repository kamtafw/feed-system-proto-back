import asyncio
from app.event_bus import bus

received = []


async def handler(payload):
    received.append(payload)


bus.subscribe("TestEvent", handler)


async def main():
    await bus.publish("TestEvent", {"x": 1})
    assert received == [{"x": 1}], "Handler not called"
    print("Bus works")


asyncio.run(main())
