# ONBOARDING.md — Agentic1 Bus Quick Start Guide

Welcome to the **Agentic1 Redis Bus Layer** — a flexible, pluggable messaging backbone for AI agents, human inputs, and modular services.
This guide gets you live in under 2 minutes.

---

## 🚀 What You’re Working With

- ✅ `bus.py` – the core message layer (installable)
- ✅ `pa0.py` – main root agent that listens to user inboxes
- ✅ `billing.py` – sample agent on another group/stream
- ✅ `cli_bridge.py` – CLI sender (already working, no changes needed)

---

## 🔧 1. Prerequisites

- Python 3.8+
- Redis running forge : 8081)
- `aioredis`, `uuid`, and your own `Envelope` class installed

---

## 🧠 2. How This Works (Visually)

```plaintext
[ cli_bridge.py ]
    │
    ▼  publish() with Envelope
[ Redis Stream Bus ]
    │
    └─▶ user.991.inbox ─────▶ handled by PA0
              ▲
              │ (auto-subscribed via subscribe_discovery)
        bus.py  ←── SCANs Redis every 5s
```

- 📨 Streams like `user.991.inbox`, `billing.tx.inbox` are auto-handled
- 🧠 Groups like `pa0`, `billing` isolate consumer logic

---

## ✅ 3. Step-by-Step Setup

### 1️⃣ Run PA0 (main dynamic agent handler)
```bash
python pa0.py
```

This auto-detects any new `user.*.inbox` stream and subscribes to it.

### 2️⃣ Send a message from CLI
```python
await bus.publish(Envelope(to=["user.991.inbox"], content={"msg": "hi!"}))
```
✅ Your message is delivered. PA0 prints it.

### 3️⃣ Run billing agent (example of static group)
```bash
python billing.py
```
Listens to `billing.tx.inbox` under group `billing`

---

## 🧰 Key Functions

```python
await bus.connect()                       # 🔌 Connects to Redis
await bus.publish(envelope)              # 📤 Sends to any inbox stream
await bus.subscribe(stream, handler)     # 📥 Listens on a stream (group-based)
await bus.subscribe_discovery(cb)        # 🔍 Auto-subscribes to all user.*.inbox
```

---

## 🤖 What Does PA0 Actually Do?
- Connects to Redis
- Scans Redis every 5s for new `user.*.inbox` streams
- Subscribes to them on-demand
- Dispatches envelopes to a per-user assistant instance (or prints/logs them)

---

## 💬 Developer Tips

- ✅ You **don’t need to change CLI tools**
- ✅ You **don’t need a discovery agent**
- ✅ You **can plug any agent** into its own stream + group

```python
await bus.subscribe("summarizer.inbox", handler, group="summarizer")
```

---

## 💥 Done!
Now you're streaming, agentically. Drop in your own stream name, add a group, and your agents will just work.

Let the envelopes flow.
