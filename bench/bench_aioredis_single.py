import asyncio

import uvloop
from redis import asyncio as aioredis

uvloop.install()


async def main():
    pool = aioredis.BlockingConnectionPool.from_url(
        "redis://localhost", max_connections=10
    )
    redis = aioredis.Redis(connection_pool=pool)
    for i in range(1000000):
        await redis.set("bench", "yes")


asyncio.run(main())
