import asyncio
import zangy
from tqdm import tqdm


async def main():
    pool = await zangy.create_pool("redis://localhost", 10, 0)
    futures = []
    for i in tqdm(range(1000000), desc="Bulk spawning..."):
        futures.append(pool.set(f"bench{i}", "yes"))
    await asyncio.gather(*futures)


asyncio.run(main())
