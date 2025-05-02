#----- Version 2
# bus.py - Redis Stream Bus for Agentic1 Framework

import aioredis
import json
import uuid
from envelope import Envelope

STREAM_PREFIX = "user."
REDIS_URL = CONFIG

class RedisBus:
    def __init__(self):
        self.redis = None
        self.seen_streams = set()

    async def connect(self):
        self.redis = await aioredis.from_url(REDIS_URL, decode_responses=True)

    async def publish(self, envelope: Envelope):
        if not self.redis:
            await self.connect()
        await self.redis.xadd(envelope.to[0], envelope.to_dict())

    async def subscribe(self, stream_name, callback, group="pa0", consumer_id=None):
        if not self.redis:
            await self.connect()
        consumer_id = consumer_id or f"{group}-{uuid.uuid4().hex[:6]}"
        try:
            await self.redis.xgroup_create(stream_name, group, id="$", mkstream=True)
        except aioredis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

        async def worker():
            while True:
                entries = await self.redis.xreadgroup(group, consumer_id, streams={stream_name: ">"}, count=10, block=5000)
                for stream, msgs in entries:
                    for msg_id, data in msgs:
                        env = Envelope.from_dict(data)
                        await callback(env)
                        await self.redis.xack(stream, group, msg_id)

        import asyncio
        asyncio.create_task(worker())

    async def subscribe_discovery(self, on_new_stream, pattern=f"{STREAM_PREFIX}*.inbox", poll_delay=5):
        import asyncio
        if not self.redis:
            await self.connect()

        async def scanner():
            while True:
                cursor = "0"
                while cursor:
                    cursor, keys = await self.redis.scan(cursor=cursor, match=pattern)
                    for key in keys:
                        if key not in self.seen_streams:
                            self.seen_streams.add(key)
                            await on_new_stream(key)
                await asyncio.sleep(poll_delay)

        asyncio.create_task(scanner())

bus = RedisBus()

# pa0.py - Personal Assistant entrypoint for Agentic1

import asyncio
from bus import bus
from envelope import Envelope

async def handle_user_inbox(env):
    print(f"[PA0] Inbox Message for {env.to}: {env.content}")
    # Dispatch to PersonalAssistant instance here

async def handle_stream_detected(stream_name):
    print(f"[PA0] Discovered new stream: {stream_name}")
    await bus.subscribe(stream_name, handle_user_inbox, group="pa0")

async def main():
    await bus.connect()
    await bus.subscribe_discovery(handle_stream_detected)

if __name__ == "__main__":
    asyncio.run(main())

# billing.py - Billing agent that subscribes to tx stream

import asyncio
from bus import bus
from envelope import Envelope

async def handle_tx(env):
    print(f"[BILLING] Received TX: {env.content}")
    # Simulate processing and maybe respond

async def main():
    await bus.connect()
    await bus.subscribe("billing.tx.inbox", handle_tx, group="billing")

if __name__ == "__main__":
    asyncio.run(main())

# README (inline)
"""
Agentic1 Redis Stream Bus
=========================

This is a minimal and extensible message bus framework using Redis Streams
for the Agentic1 system. It supports per-user inbox channels, dynamic discovery,
and consumer groups for durable multi-agent communication.

🔧 Setup Instructions (For Devs and Agents):
--------------------------------------------

1. ✅ Ensure Redis server is running 8081! on forge!
2. ✅ Launch the root agent:
    ```bash
    python pa0.py
    ```
3. ✅ Run a message sender (your CLI stays the same)
    ```python
    await bus.publish(Envelope(to=["user.991.inbox"], content={"text": "hello"}))
    ```
4. ✅ (Optional) Run another agent like billing:
    ```bash
    python billing.py
    ```

📦 What Happens:
----------------
- `pa0.py` scans Redis for `user.*.inbox` streams
- When a new user inbox is detected, it subscribes via group `pa0`
- All messages sent to that stream are routed to `handle_user_inbox`
- You can run multiple agents (billing, summarizer, etc.) in parallel

💡 Developer Notes:
-------------------
- Streams are just Redis streams: `user.991.inbox`, `billing.tx.inbox`, etc.
- Groups let multiple logical agents process in isolation (`XREADGROUP`)
- `bus.py` is your installable library — keep it core, use everywhere

📚 Summary of Bus Functions:
----------------------------
- `publish(envelope)` → Send an Envelope to a stream
- `subscribe(stream, callback, group)` → Listen to a stream as part of a group
- `subscribe_discovery(callback)` → Scan Redis for new streams like `user.*.inbox`

📊 Group/Stream Diagram:
────────────────────────────────────────────────────────────
        Redis Streams (via XADD/XREADGROUP)
               │
               ▼
    ┌─────────────────────────────┐
    │   Stream: user.991.inbox    │
    └─────────────────────────────┘
               │  auto-detected by SCAN
               ▼
          [ PA0 Agent ] (group: "pa0")
               │
               ▼
    [ PersonalAssistant instance(s) ]

    ┌─────────────────────────────┐
    │  Stream: billing.tx.inbox   │
    └─────────────────────────────┘
               │
               ▼
        [ Billing Agent ] (group: "billing")

[ cli_bridge.py ]
    │
    ▼  publish() with Envelope
[ Redis Stream Bus ]
    │
    └─▶ user.991.inbox ─────▶ handled by PA0
              ▲
              │ (auto-subscribed via subscribe_discovery)
        bus.py  ←── SCANs Redis every 5s
Channels (Streams):
───────────────────────────────────────────────
- user.991.inbox       → Messages for PersonalAssistant(991)
- user.442.inbox       → For user 442, etc.
- system.discovery     → Watcher emits discovered inbox streams

Groups (Consumers on channels):
───────────────────────────────────────────────
- pa0                  → Main group for handling user inboxes
- billing              → Handles tx/invoice streams like billing.tx.inbox
- mcp_bridge           → Handles mcp_bridge.inbox / outbox routing

"""




#------------------
# VERSION 1.0bus.py - Redis Stream Bus for Agentic1 Framework

import aioredis
import json
import uuid
from envelope import Envelope

STREAM_PREFIX = "user."
REDIS_URL = CONFIG

class RedisBus:
    def __init__(self):
        self.redis = None
        self.seen_streams = set()

    async def connect(self):
        self.redis = await aioredis.from_url(REDIS_URL, decode_responses=True)

    async def publish(self, envelope: Envelope):
        if not self.redis:
            await self.connect()
        await self.redis.xadd(envelope.to[0], envelope.to_dict())

    async def subscribe(self, stream_name, callback, group="pa0", consumer_id=None):
        if not self.redis:
            await self.connect()
        consumer_id = consumer_id or f"{group}-{uuid.uuid4().hex[:6]}"
        try:
            await self.redis.xgroup_create(stream_name, group, id="$", mkstream=True)
        except aioredis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise e

        async def worker():
            while True:
                entries = await self.redis.xreadgroup(group, consumer_id, streams={stream_name: ">"}, count=10, block=5000)
                for stream, msgs in entries:
                    for msg_id, data in msgs:
                        env = Envelope.from_dict(data)
                        await callback(env)
                        await self.redis.xack(stream, group, msg_id)

        import asyncio
        asyncio.create_task(worker())

    async def subscribe_discovery(self, on_new_stream, pattern=f"{STREAM_PREFIX}*.inbox", poll_delay=5):
        import asyncio
        if not self.redis:
            await self.connect()

        async def scanner():
            while True:
                cursor = "0"
                while cursor:
                    cursor, keys = await self.redis.scan(cursor=cursor, match=pattern)
                    for key in keys:
                        if key not in self.seen_streams:
                            self.seen_streams.add(key)
                            await on_new_stream(key)
                await asyncio.sleep(poll_delay)

        asyncio.create_task(scanner())

bus = RedisBus()

# pa0.py - Personal Assistant entrypoint for Agentic1

import asyncio
from bus import bus
from envelope import Envelope

async def handle_user_inbox(env):
    print(f"[PA0] Inbox Message for {env.to}: {env.content}")
    # Dispatch to PersonalAssistant instance here

async def handle_stream_detected(stream_name):
    print(f"[PA0] Discovered new stream: {stream_name}")
    await bus.subscribe(stream_name, handle_user_inbox, group="pa0")

async def main():
    await bus.connect()
    await bus.subscribe_discovery(handle_stream_detected)

if __name__ == "__main__":
    asyncio.run(main())

# billing.py - Billing agent that subscribes to tx stream

import asyncio
from bus import bus
from envelope import Envelope

async def handle_tx(env):
    print(f"[BILLING] Received TX: {env.content}")
    # Simulate processing and maybe respond

async def main():
    await bus.connect()
    await bus.subscribe("billing.tx.inbox", handle_tx, group="billing")

if __name__ == "__main__":
    asyncio.run(main())

# README (inline)
"""
Agentic1 Redis Stream Bus
=========================

This is a minimal and extensible message bus framework using Redis Streams
for the Agentic1 system. It supports per-user inbox channels, dynamic discovery,
and consumer groups for durable multi-agent communication.

🔧 Setup Instructions (For Devs and Agents):
--------------------------------------------

1. ✅ Ensure Redis server is running locally (port 6379)
2. ✅ Launch the root agent:
    ```bash
    python pa0.py
    ```
3. ✅ Run a message sender (your CLI stays the same)
    ```python
    await bus.publish(Envelope(to=["user.991.inbox"], content={"text": "hello"}))
    ```
4. ✅ (Optional) Run another agent like billing:
    ```bash
    python billing.py
    ```

📦 What Happens:
----------------
- `pa0.py` scans Redis for `user.*.inbox` streams
- When a new user inbox is detected, it subscribes via group `pa0`
- All messages sent to that stream are routed to `handle_user_inbox`
- You can run multiple agents (billing, summarizer, etc.) in parallel

💡 Developer Notes:
-------------------
- Streams are just Redis streams: `user.991.inbox`, `billing.tx.inbox`, etc.
- Groups let multiple logical agents process in isolation (`XREADGROUP`)
- `bus.py` is your installable library — keep it core, use everywhere

📚 Summary of Bus Functions:
----------------------------
- `publish(envelope)` → Send an Envelope to a stream
- `subscribe(stream, callback, group)` → Listen to a stream as part of a group
- `subscribe_discovery(callback)` → Scan Redis for new streams like `user.*.inbox`

📊 Group/Stream Diagram:
────────────────────────────────────────────────────────────
        Redis Streams (via XADD/XREADGROUP)
               │
               ▼
    ┌─────────────────────────────┐
    │   Stream: user.991.inbox    │
    └─────────────────────────────┘
               │  auto-detected by SCAN
               ▼
          [ PA0 Agent ] (group: "pa0")
               │
               ▼
    [ PersonalAssistant instance(s) ]

    ┌─────────────────────────────┐
    │  Stream: billing.tx.inbox   │
    └─────────────────────────────┘
               │
               ▼
        [ Billing Agent ] (group: "billing")

[ cli_bridge.py ]
    │
    ▼  publish() with Envelope
[ Redis Stream Bus ]
    │
    └─▶ user.991.inbox ─────▶ handled by PA0
              ▲
              │ (auto-subscribed via subscribe_discovery)
        bus.py  ←── SCANs Redis every 5s
Channels (Streams):
───────────────────────────────────────────────
- user.991.inbox       → Messages for PersonalAssistant(991)
- user.442.inbox       → For user 442, etc.
- system.discovery     → Watcher emits discovered inbox streams

Groups (Consumers on channels):
───────────────────────────────────────────────
- pa0                  → Main group for handling user inboxes
- billing              → Handles tx/invoice streams like billing.tx.inbox
- mcp_bridge           → Handles mcp_bridge.inbox / outbox routing

"""
