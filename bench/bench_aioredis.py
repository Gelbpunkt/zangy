import asyncio
import aioredis
import uvloop
from tqdm.asyncio import tqdm

uvloop.install()

async def main():
    pool = await aioredis.create_redis_pool("redis://localhost", minsize=10, maxsize=10)
    futures = []
    async for i in tqdm(range(1000000), desc="Bulk spawning..."):
        futures.append(asyncio.create_task(pool.set(f"bench{i}", "yes")))
    await asyncio.gather(*futures)


asyncio.run(main())
