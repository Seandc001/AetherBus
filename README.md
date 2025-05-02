# 🧠 AetherBus

This system uses Redis Streams to handle real-time messaging between agents and users. Each user has their own inbox stream (e.g. `user.<user_id>.inbox`), and agents dynamically discover and subscribe to new user streams.

---

## 📝 License

This project is licensed under the **Agentic1 - Agent Licence**. See the [LICENSE](./LICENSE) file for details.

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
