# bdns_portal/cache/redis_cache.py
import json
from typing import Any, Optional
import redis.asyncio as redis
from ..core.config import settings

class RedisCache:
    def __init__(self):
        self.client = None
    
    async def init(self):
        self.client = await redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            encoding="utf-8"
        )
    
    async def get(self, key: str) -> Optional[Any]:
        if not self.client:
            return None
        data = await self.client.get(key)
        if data:
            return json.loads(data)
        return None
    
    async def set(self, key: str, value: Any, expire: int = 3600) -> None:
        if not self.client:
            return
        await self.client.setex(key, expire, json.dumps(value, default=str))
    
    async def delete(self, key: str) -> None:
        if not self.client:
            return
        await self.client.delete(key)
    
    async def clear_pattern(self, pattern: str) -> None:
        if not self.client:
            return
        keys = await self.client.keys(pattern)
        if keys:
            await self.client.delete(*keys)

redis_cache = RedisCache()