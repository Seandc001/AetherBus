import sys
import os
from dotenv import load_dotenv
load_dotenv()

import uuid
import json
from .envelope import Envelope
import redis.asyncio as aioredis
import redis as redis_sync
import asyncio
import inspect

# --- Configurable Redis connection ---
REDIS_HOST = os.getenv("REDIS_HOST", "forge.evasworld.net")
REDIS_PORT = int(os.getenv("REDIS_PORT", 8081))
REDIS_USERNAME = os.getenv("REDIS_USERNAME","admin")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "UltraSecretRoot123")



STREAM_MAXLEN = int(os.getenv("BUS_STREAM_MAXLEN", 10000))  # For dev/demo, tune as needed
ENVELOPE_SIZE_LIMIT = 128 * 1024  # 128 KB

# --- Optional: Subscribe to all matching inbox streams dynamically ---
async def subscribe_discovery(redis, pattern="user.*.inbox", callback=None, group="pa0", seen=None, poll_delay=5):
    seen = seen or set()

    async def _subscribe(stream):
        if callback:
            await subscribe(redis, stream, callback, group=group)

    import asyncio
    while True:
        #print(f"[DISCOVERY] SCAN loop start")
        cursor = "0"
        while True:
            #print(f"[DISCOVERY] Scanning with cursor: {cursor}")
            cursor, keys = await redis.scan(cursor=cursor, match=pattern)
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode()
                if key not in seen:
                    #print(f"[DISCOVERY] New stream: {key}")
                    seen.add(key)
                    #await _subscribe(key)
                    asyncio.create_task(_subscribe(key))
            if cursor == "0":
                break
        await asyncio.sleep(poll_delay)
        #print("[DISCOVERY] Waiting before next scan...")

# --- Utility: Ensure consumer group exists robustly ---
async def ensure_group(redis, channel, group):
    try:
        await redis.xgroup_create(channel, group, id='0-0', mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            pass  # Group already exists
        else:
            raise

def extract_user_id_from_channel(ch):
    return ch.split(".")[1] if ch.startswith("user.") else "unknown"
    
# --- Publish ---
async def publish_envelope(redis, channel: str, env: Envelope):
    data = json.dumps(env.to_dict())
    if len(data.encode("utf-8")) > ENVELOPE_SIZE_LIMIT:
        raise ValueError(f"Envelope exceeds {ENVELOPE_SIZE_LIMIT} bytes. Offload large payloads to S3/object storage.")
    #if  u need to disover
    #await redis.xadd(channel, {"data": data}, maxlen=STREAM_MAXLEN)
    #autodisover
    stream_exists = await redis.exists(channel)
    await redis.xadd(channel, {"data": data}, maxlen=STREAM_MAXLEN)
    if not stream_exists:
        # auto-discovery trigger
        discovery = Envelope(
            role="user",
            content={"user_id": extract_user_id_from_channel(channel)},
            user_id=extract_user_id_from_channel(channel),
            agent_name="auto_announce",
            envelope_type="discovery"
        )
        await redis.xadd("user.discovery", {"data": json.dumps(discovery.to_dict())})

# --- Subscribe (Consumer Group, Reliable, Robust) ---
async def subscribe(redis, channel: str, callback, group: str = "corebus", consumer: str = None, block_ms: int = 1000, dead_letter_max_retries: int = 3):
    consumer = consumer or f"{os.getenv('HOSTNAME','host')}-{uuid.uuid4().hex[:4]}"
    await ensure_group(redis, channel, group)
    retry_counts = {}
    first_read = True  # ✅ Only do '0' on first loop
    #print(f"[BUS] Subscribing to {channel} with consumer={consumer}")
    while True:
        try:
            read_from = '0' if first_read else '>'
            #print(f"[BUS] Waiting for messages on {channel} (group={group})")
            resp = await redis.xreadgroup(group, consumer, streams={channel: read_from}, count=10, block=block_ms)
            
            first_read = False  # ✅ Switch to '>' after first pass

            for ch, msgs in resp:
                #print(f"[BUS] Received {len(msgs)} messages on {channel}")
                for msg_id, raw in msgs:
                    data = json.loads(raw[b'data'].decode())
                    env = Envelope.from_dict(data)
                    env.add_hop(consumer)

                    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
                    retry_counts[msg_id_str] = retry_counts.get(msg_id_str, 0) + 1

                    try:
                        if inspect.iscoroutinefunction(callback):
                            reply_env = await callback(env)
                        else:
                            reply_env = callback(env)
                        if reply_env is not None:
                            await publish_envelope(redis, reply_env.reply_to, reply_env)
                    except Exception as e:
                        import sys
                        #print(f"[bus] callback error: {e} (msg_id={msg_id})", file=sys.stderr)
                        if retry_counts[msg_id_str] >= dead_letter_max_retries:
                            print(f"[bus] Moving message {msg_id_str} to dead.{channel}", file=sys.stderr)
                            await redis.xadd(f"dead.{channel}", {"data": json.dumps(env.to_dict())})
                            retry_counts.pop(msg_id_str, None)
                    finally:
                        await redis.xack(channel, group, msg_id)

        except aioredis.ResponseError as e:
            if "NOGROUP" in str(e):
                await ensure_group(redis, channel, group)
            else:
                raise

# --- Helper: Build Redis URL with optional auth ---
def build_redis_url():
    user = REDIS_USERNAME
    pwd = REDIS_PASSWORD
    if user and pwd:
        return f"redis://{user}:{pwd}@{REDIS_HOST}:{REDIS_PORT}"
    elif pwd:
        return f"redis://:{pwd}@{REDIS_HOST}:{REDIS_PORT}"
    else:
        return f"redis://{REDIS_HOST}:{REDIS_PORT}"

async def main():
    print(f'Host : {REDIS_HOST}')
    redis = await aioredis.from_url(build_redis_url())
    # Example usage:
    # await publish_envelope(redis, "my_channel", Envelope())
    # await subscribe(redis, "my_channel", my_callback)
    await redis.aclose()

if __name__ == "__main__":
    asyncio.run(main())
