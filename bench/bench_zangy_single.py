import asyncio
import zangy
from tqdm import tqdm


async def main():
    pool = await zangy.create_pool("redis://localhost", 10)
    for i in tqdm(range(1000000), desc="Setting keys..."):
        await pool.set("bench", "yes")


asyncio.run(main())
