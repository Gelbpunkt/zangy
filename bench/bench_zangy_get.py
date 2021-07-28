import asyncio
import uvloop
import zangy
from tqdm.asyncio import tqdm

uvloop.install()

async def main():
    pool = await zangy.create_pool("redis://localhost", 10, 0)
    await pool.set("bench", "yes")
    async for i in tqdm(range(1000000), desc="Getting keys..."):
        await pool.get("bench")


asyncio.run(main())
