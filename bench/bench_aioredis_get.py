import asyncio
import aioredis
from tqdm import tqdm


async def main():
    pool = await aioredis.create_redis_pool("redis://localhost", minsize=10, maxsize=10)
    await pool.set("bench", "yes")
    for i in tqdm(range(1000000), desc="Getting keys..."):
        await pool.get("bench")


asyncio.run(main())
