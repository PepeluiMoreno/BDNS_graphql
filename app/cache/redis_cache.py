import json
import pickle
from typing import Any, Optional, TypeVar, Generic, Union
import redis.asyncio as redis
from app.config import get_settings

settings = get_settings()

T = TypeVar('T')

class RedisCache(Generic[T]):
    """Implementación de caché con Redis"""
    
    def __init__(self):
        self.redis = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD or None,
            decode_responses=False,  # Necesario para pickle
        )
    
    async def get(self, key: str) -> Optional[T]:
        """Obtener valor de caché"""
        value = await self.redis.get(key)
        if value is None:
            return None
        try:
            return pickle.loads(value)
        except Exception:
            return None
    
    async def set(self, key: str, value: T, expire: Optional[int] = None) -> bool:
        """Establecer valor en caché"""
        try:
            serialized = pickle.dumps(value)
            if expire:
                return await self.redis.setex(key, expire, serialized)
            else:
                return await self.redis.set(key, serialized)
        except Exception:
            return False
    
    async def delete(self, key: str) -> bool:
        """Eliminar valor de caché"""
        return await self.redis.delete(key) > 0
    
    async def clear_pattern(self, pattern: str) -> int:
        """Eliminar valores que coincidan con un patrón"""
        keys = await self.redis.keys(pattern)
        if not keys:
            return 0
        return await self.redis.delete(*keys)

# Instancia global de caché
redis_cache = RedisCache()
