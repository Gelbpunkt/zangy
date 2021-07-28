import asyncio
import aioredis
import uvloop
from tqdm.asyncio import tqdm

uvloop.install()

async def main():
    pool = await aioredis.create_redis_pool("redis://localhost", minsize=10, maxsize=10)
    await pool.set("bench", "yes")
    async for i in tqdm(range(1000000), desc="Getting keys..."):
        await pool.get("bench")


asyncio.run(main())
