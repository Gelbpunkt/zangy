import asyncio
import aioredis
from tqdm import tqdm


async def main():
    pool = await aioredis.create_redis_pool("redis://localhost", minsize=10, maxsize=10)
    for i in tqdm(range(1000000), desc="Setting keys..."):
        await pool.set("bench", "yes")


asyncio.run(main())
