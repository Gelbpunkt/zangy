import asyncio
import uvloop
import zangy
from tqdm.asyncio import tqdm

uvloop.install()

async def main():
    pool = await zangy.create_pool("redis://localhost", 10, 0)
    async for i in tqdm(range(1000000), desc="Setting keys..."):
        await pool.set("bench", "yes")


asyncio.run(main())
