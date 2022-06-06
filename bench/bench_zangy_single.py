import asyncio

import uvloop
import zangy

uvloop.install()


async def main():
    pool = await zangy.create_pool("redis://localhost", 10, 0)
    for i in range(1000000):
        await pool.set("bench", "yes")


asyncio.run(main())
