import aioredis
import json

redis = aioredis.from_url("redis://localhost")

async def cache_query(query: str, result=None):
    """
    Cache SQL query and result. If result is None, retrieve from cache.
    """
    key = f"sql_cache:{hash(query)}"
    if result is None:
        # Try to fetch from cache
        cached_result = await redis.get(key)
        if cached_result:
            return json.loads(cached_result)
        return None
    else:
        # Cache the query result
        await redis.set(key, json.dumps(result), ex=60*5)  # Cache for 5 minutes
        return result
