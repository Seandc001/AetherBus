# 🧠 AetherBus

This system uses Redis Streams to handle real-time messaging between agents and users. Each user has their own inbox stream (e.g. `user.<user_id>.inbox`), and agents dynamically discover and subscribe to new user streams.

---

## 📝 License

This project is licensed under the **Agentic1 - Agent Licence**. See the [LICENSE](../LICENSE) file for details.

---

## ✅ How It Works

- **Each user** gets a personal inbox: `user.<user_id>.inbox`
- The system scans for new streams matching `user.*.inbox`
- When discovered, it **spawns a dedicated async subscriber**
- Messages are handled by the PersonalAssistant (`pa0.py`) or another agent

---

## 🛰️ Developer Flow (CLI / SDK)

1. No need to manually announce a user anymore ✅
2. Just send a message via `publish_envelope(...)` — this:
   - Creates the stream (if not exists)
   - Triggers discovery automatically
   - Delivers the message to the correct inbox

```python
env = Envelope(
    role="user",
    user_id="myuser123",
    reply_to="user.myuser123.inbox",
    content={"text": "hello"},
    agent_name="cli_bridge",
    envelope_type="message"
)

await publish_envelope(redis, env.reply_to, env)
```

That’s it. The system handles discovery, subscriptions, and routing.

---

## 🔥 Behind the Scenes

- Discovery uses `SCAN` + `asyncio.create_task()` to keep subscriptions non-blocking
- First message in each inbox is always processed (`xreadgroup` uses `'0'` once)
- Dead-letter handling is built-in (after configurable retry count)

---

## 🧩 Key Components

| File             | Purpose                                |
|------------------|----------------------------------------|
| `bus.py`         | Pub/sub engine with discovery          |
| `pa0.py`         | Main entry agent (PersonalAssistant)   |
| `cli_bridge.py`  | CLI interface for user interaction     |
| `envelope.py`    | Message schema + metadata              |

#----------






# core_bus

Message bus SDK for AG1. Defines the Envelope schema and helpers for publish/subscribe on Redis Streams.

## Envelope Schema

All messages on the bus are wrapped in an `Envelope` object, serialized as JSON. This ensures consistency and evolvability across all services.

```
      ┌────────────────────────────────────────────────────────────────────┐
      │                           Envelope Schema                         │
      ├───────────────┬───────────────┬───────────────┬───────────────────┤
      │ id            │ ts            │ channel       │ headers           │
      │ (UUID/ULID)   │ (ISO-8601)    │ (str)         │ {str: str}        │
      ├───────────────┴───────────────┴───────────────┴───────────────────┤
      │ payload         (dict | str)                                     │
      ├───────────────────────────────────────────────────────────────────┤
      │ version (int) │ reply_to (str) │ trace (list[str])               │
      └───────────────────────────────────────────────────────────────────┘
```

| Field      | Type              | Required | Purpose                                      |
|------------|-------------------|----------|----------------------------------------------|
| id         | ULID/UUIDv7/str   | ✔︎        | Globally unique message id (sortable)        |
| ts         | ISO-8601 string   | ✔︎        | Creation timestamp (UTC)                     |
| channel    | str               | ✔︎        | Routing key (e.g., tasks.create)             |
| headers    | dict[str, str]    | ✔︎        | Fast filters & correlation (max 10 keys)     |
| payload    | dict or str       | ✔︎        | Message body (versioned)                     |
| version    | int               | ✔︎        | Schema version of payload                    |
| reply_to   | str (optional)    | —        | Where a consumer should answer               |
| trace      | list[str]         | —        | Hop list for debugging                       |

**Transport encoding:**
- `json.dumps(envelope).encode("utf-8")` → XADD/XREAD (Redis Streams)
- **Size limit:** 128 KB per message (put large blobs in S3/object storage & pass the URI)

## Canonical Channels (examples)

| Channel         | Producer(s)          | Consumer(s)        | Payload v1                          |
|-----------------|---------------------|--------------------|-------------------------------------|
| tasks.create    | Conversation-Agent  | Task-Manager       | { task_id, user_id, agent_hint, args } |
| billing.request | Task-Manager        | Billing-Service    | { task_id, amount, ... }            |
| tasks.result    | Task-Manager        | PA/Orchestrator    | { task_id, status, result }         |

## AG1 Bus Architecture (ASCII)

```
BrainProject3/
└── AG1/
    ├── core_bus/
    │   ├── envelope.py      # Envelope schema (strict contract)
    │   ├── bus.py           # Reliable pub/sub (Redis Streams, consumer groups)
    │   ├── tail.py          # CLI traffic viewer
    │   └── README.md        # Schema, usage, and ASCII docs
    │
    ├── task_manager_service/
    │   └── main.py          # Listens on tasks.create, echoes results
    │
    ├── billing_service/
    │   └── echo.py          # Echoes canned billing responses
    │
    ├── agent_registry_service/
    │   └── main.py          # (Stub for future service wrapper)
    │
    ├── tests/
    │   ├── test_core_bus.py # Unit/integration tests for bus + envelope
    │   └── README.md
    │
    └── README.md            # AG1 overview & migration plan

      ┌──────────────┐         publish/subscribe        ┌──────────────┐
      │ Producer(s)  │ ───────────────────────────────► │ Consumer(s)  │
      └──────────────┘                                  └──────────────┘
             │                                                ▲
             ▼                                                │
     ┌─────────────────────────────┐                 ┌─────────────────────────────┐
     │  core_bus.publish()         │                 │  core_bus.subscribe()       │
     └────────────┬────────────────┘                 └────────────┬────────────────┘
                  │                                              │
                  ▼                                              │
        ┌─────────────────────────────┐           ┌─────────────────────────────┐
        │   Redis Streams (tasks,     │ ◄──────── │   Redis Streams (billing,   │
        │   billing, ...channels)     │           │   tasks.result, ...)        │
        └─────────────────────────────┘           └─────────────────────────────┘
```

## Files
- `envelope.py`: Envelope dataclass and validation
- `bus.py`: Publish/subscribe helpers
- `tail.py`: CLI tool to view bus traffic

## Envelope Publishing Guidelines for MCP Bridge

When publishing an Envelope to the AG1 bus for MCP tool calls, ensure:

- **endpoint**: Always provide the full MCP base URL (e.g., `https://server.smithery.ai/@jmp0x7c00/my-echo-mcp`).  
  Do **not** use `@foo/bar` or add `/ws` or `/mcp/call/ws`—the bridge will handle protocol details.

- **tool**: The MCP tool name to invoke (e.g., `"echo"`, `"get_bot_info"`).

- **args**: Arguments for the tool, as a dict.

- **config**: Any required configuration for the MCP endpoint (e.g., tokens, API keys).

- **session_code**: Always include a unique session code for tracking and downstream correlation.

- **reply_to**: Set to `"mcp_bridge.outbox"` (or your preferred response channel).

**Example:**
```python
Envelope(
    role="user",
    content={
        "endpoint": "https://server.smithery.ai/@jmp0x7c00/my-echo-mcp",
        "tool": "echo",
        "args": {"message": "Hello, world!"},
        "config": {}
    },
    session_code="your-session-id-123",
    reply_to="mcp_bridge.outbox"
)
```

**Best Practices:**
- Use environment variables for secrets (tokens, API keys).
- Always set `session_code` for traceability.
- Handle error envelopes (with `envelope_type='error'`) in your consumers.
- For new endpoints, validate tool names and schemas before publishing.

---
