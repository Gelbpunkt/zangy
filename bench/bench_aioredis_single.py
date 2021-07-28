import asyncio
import aioredis
import uvloop
from tqdm.asyncio import tqdm

uvloop.install()

async def main():
    pool = await aioredis.create_redis_pool("redis://localhost", minsize=10, maxsize=10)
    async for i in tqdm(range(1000000), desc="Setting keys..."):
        await pool.set("bench", "yes")


asyncio.run(main())
