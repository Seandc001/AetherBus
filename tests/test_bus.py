import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
import redis.asyncio as redis
import asyncio
from core_bus.bus import publish_envelope
from core_bus.envelope import Envelope

@pytest.mark.asyncio
async def test_publish_envelope():
    # Build Redis URL from environment or use defaults
    REDIS_URL = os.getenv("REDIS_URL") or f"redis://{os.getenv('REDIS_USERNAME', 'agentbus')}:{os.getenv('REDIS_PASSWORD', 'SuperSecret1234')}@{os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6379')}"
    r = redis.from_url(REDIS_URL)

    # Prepare envelope
    env = Envelope(
        role="test",
        user_id="pytestuser",
        reply_to="user.pytestuser.inbox",
        content={"text": "pytest hello!"},
        agent_name="pytest_agent",
        envelope_type="test_event"
    )

    # Publish envelope
    await publish_envelope(r, env.reply_to, env)

    # Check that the message exists in Redis (first message in the stream)
    messages = await r.xrange(env.reply_to, count=1)
    assert messages, "No messages found in the stream!"
    assert b'data' in messages[0][1], "Envelope missing 'data' field in Redis stream!"

    import json
    envelope_data = json.loads(messages[0][1][b'data'].decode())
    assert envelope_data["role"] == "test"
    assert envelope_data["content"]["text"] == "pytest hello!"
    assert envelope_data["agent_name"] == "pytest_agent"
    assert envelope_data["envelope_type"] == "test_event"

    await r.close()
