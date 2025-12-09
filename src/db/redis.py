import os
import json
import redis.asyncio as redis
from dotenv import load_dotenv

# Load variables from .env (for local development)
load_dotenv()

# 1. Read configuration from environment
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_DB = os.getenv("REDIS_DB", "0")

# Construct URL: redis://redis:6379/0
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

print(f"🔌 [Redis] Connecting to: {REDIS_URL}")

# 2. Create connection pool
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

async def get_cache(key: str):
    """Retrieve and deserialize data from Redis."""
    try:
        data = await redis_client.get(key)
        return json.loads(data) if data else None
    except redis.ConnectionError:
        print("❌ [Redis] Connection failed! Check if Redis is running.")
        return None

async def set_cache(key: str, data: dict, expire: int = None):
    """Serialize and save data to Redis with optional expiration."""
    try:
        value = json.dumps(data)
        if expire:
            await redis_client.set(key, value, ex=expire)
        else:
            await redis_client.set(key, value)
    except redis.ConnectionError:
        print("❌ [Redis] Failed to save data (No connection).")
