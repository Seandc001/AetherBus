import asyncio
import uuid
import sys
import os
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if root_dir not in sys.path:
    sys.path.append(root_dir)

from AG1.core_bus.bus import publish_envelope, subscribe, build_redis_url
from AG1.core_bus.envelope import Envelope
import redis.asyncio as aioredis

CHANNEL = lambda user_id: f"user.{user_id}.inbox"

async def handle_response(env):
    print(f"[CLI_BRIDGE][RESPONSE] {env.content if hasattr(env, 'content') else env}")

async def main():
    # Interactive prompt for user_id and session_code
    user_id = input("Enter user_id (default: testuser): ").strip() or "testuser"
    session_code = input("Enter session_code (blank for new session): ").strip()
    channel = CHANNEL(user_id)
    print(f"[CLI_BRIDGE] Connected as user: {user_id}")
    print(f"[CLI_BRIDGE] Type messages to send to {channel}. Ctrl+C to exit.")

    redis = await aioredis.from_url(build_redis_url())

    #await redis.xadd("user.discovery", {"user_id": user_id})

    # Start a background subscriber for responses
    asyncio.create_task(subscribe(redis, channel, handle_response, "pa_group", user_id))

    # Main input loop
    while True:
        try:
            msg = input(">>> ").strip()
            if not msg:
                continue
            env = Envelope(
                role="user",
                content={"text": msg},
                user_id=user_id,
                session_code=session_code if session_code else None,
                agent_name="cli_bridge",
                envelope_type="message",
                reply_to=channel
            )
            async def announce_user(user_id):
                redis = await aioredis.from_url(build_redis_url())
                discovery_env = Envelope(
                    role="user",
                    content={"user_id": user_id},
                    user_id=user_id,
                    agent_name="cli_bridge",
                    envelope_type="discovery"
                )
                await publish_envelope(redis, "user.discovery", discovery_env)
                await redis.aclose()

            # After user_id is set
            # await announce_user(user_id)
            await publish_envelope(redis, env.reply_to, env)
        except (EOFError, KeyboardInterrupt):
            print("\n[CLI_BRIDGE] Exiting.")
            break

    await redis.close()

if __name__ == "__main__":
    asyncio.run(main())
