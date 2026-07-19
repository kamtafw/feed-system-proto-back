"""
test_streams.py — Verify XACK and XAUTOCLAIM without touching the server.

Run this independently while the server is STOPPED (so it doesn't consume
the test message before this script does):

  python test_streams.py

What it proves, step by step:
  1. XADD  — message is written to the durable stream
  2. XREADGROUP — consumer group receives the message
  3. XPENDING   — unACKed message appears in the pending list (no silent loss)
  4. XAUTOCLAIM — pending message is reclaimed after idle timeout (retry works)
  5. XACK  — ACKed message is removed from the pending list (success path works)
"""

import asyncio
import json
import time
import os
from dotenv import load_dotenv
import redis.asyncio as aioredis

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
STREAM_KEY = "ff:test:PostCreated"  # separate key — safe to run alongside server
GROUP_NAME = "ff:consumers"
CONSUMER_NAME = "test-script"
RECLAIM_MS = 3_000  # 3 s idle before XAUTOCLAIM reclaims

SEP = "—" * 56


async def section(n: int, title: str) -> None:
    print(f"\n[{n}] {title}")


async def main() -> None:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()

    print(SEP)
    print(" FanoutFeed — Redis Streams verification")
    print(SEP)

    # 1. Write a test event
    await section(1, "XADD — write message to stream")

    payload = {
        "post_id": "test-xack-0001",
        "author_id": "alice",
        "author_name": "Alice",
        "created_at": time.time(),
    }
    msg_id = await r.xadd(STREAM_KEY, {"data": json.dumps(payload)})
    stream_len = await r.xlen(STREAM_KEY)

    print(f"  stream:       {STREAM_KEY}")
    print(f"  msg_id:       {msg_id}")
    print(f"  stream_len:   {stream_len} message(s)")

    # 2. Create a consumer group (idempotent)
    await section(2, "XGROUP CREATE — set up consumer group")

    try:
        await r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
        print(f"  Created group '{GROUP_NAME}' on '{STREAM_KEY}'")
    except aioredis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"  Group '{GROUP_NAME}' already exists — OK")
        else:
            raise

    # 3. Read WITHOUT ACKing — simulate a consumer crash mid-processing
    await section(3, "XREADGROUP — read without ACKing (simulated crash)")

    results = await r.xreadgroup(
        GROUP_NAME,
        CONSUMER_NAME,
        {STREAM_KEY: ">"},  # ">" = only new, undelivered messages
        count=1,
    )

    if not results:
        print(" No new messages — the group cursor may already be past this message.")
        print(" Re-run with a fresh STREAM_KEY, or delete the group and retry.")

    _, messages = results[0]
    read_id, fields = messages[0]
    print(f"  Received:   {read_id}")
    print(f"  Payload:    {fields['data'][:72]}...")
    print(f"  ↳ NOT calling XACK — message stays in pending list")

    # 4. Verify the message appears in XPENDING
    await section(4, "XPENDING — confirm message is in pending list")

    pending = await r.xpending_range(STREAM_KEY, GROUP_NAME, min="-", max="+", count=10)

    if pending:
        for entry in pending:
            print(f"  msg_id:       {entry['message_id']}")
            print(f"  consumer:     {entry['consumer']}")
            print(f"  idle (ms):    {entry['time_since_delivered']}")
            print(f"  deliveries:   {entry['times_delivered']}")
        print(f"\n  ✅  Message is in the pending list — it will NOT be lost")
    else:
        print(" (empty — message may have been ACKed by the running server)")

    # 5. Wait, then XAUTOCLAIM
    await section(5, f"XAUTOCLAIM — reclaim after {RECLAIM_MS // 1000} s idle")

    print(f"  Waiting {RECLAIM_MS // 1000} s …", end="", flush=True)
    await asyncio.sleep(RECLAIM_MS / 1000 + 0.5)
    print(" done")

    result = await r.xautoclaim(
        STREAM_KEY,
        GROUP_NAME,
        CONSUMER_NAME,
        min_idle_time=RECLAIM_MS,
        start_id="0-0",
        count=10,
    )
    # redis-py >= 4.3.4: (next_start_id, [(id, fields), ...], [deleted_ids])
    _, reclaimed, _ = result

    if reclaimed:
        print(f"  Reclaimed {len(reclaimed)} message(s):")
        for rec_id, rec_fields in reclaimed:
            print(f"  ↺  {rec_id} → {rec_fields['data'][:60]}…")
        print(f"\n  ✅  XAUTOCLAIM recovered the unACKed message for retry")
    else:
        print(f"  No messages reclaimed (idle time not yet exceeded or already ACKed)")

    # 6. ACK — success path
    await section(6, "XACK — acknowledge the message (success path)")

    acked = await r.xack(STREAM_KEY, GROUP_NAME, read_id)
    print(f"  ACKed {acked} message(s)")

    # 7. Confirm pending list is now empty
    await section(7, "XPENDING — confirm pending list is now clear")

    pending_after = await r.xpending_range(STREAM_KEY, GROUP_NAME, min="-", max="+", count=10)

    if pending_after:
        print(f"  Still pending: {len(pending_after)} message(s)")
        for entry in pending_after:
            print(f"  ↳ {entry['message_id']} ({entry['consumer']})")
    else:
        print(f"  ✅  Pending list is empty — full lifecycle verified")

    # 8. Summary
    final_len = await r.xlen(STREAM_KEY)
    print(f"\n{SEP}")
    print(f"  Stream length after test: {final_len} message(s)")
    print(f"  (messages stay in stream until MAXLEN trim — that's expected)")
    print(SEP)

    await r.close()


if __name__ == "__main__":
    asyncio.run(main())
