import asyncio

import uvloop

import zangy

uvloop.install()


async def main():
    pool = await zangy.create_pool("redis://localhost", 10, 0)
    futures = []
    for i in range(1000000):
        futures.append(pool.set(f"bench{i}", "yes"))
    await asyncio.gather(*futures)


asyncio.run(main())
