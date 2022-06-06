import asyncio

import uvloop
import zangy

uvloop.install()


async def main():
    pool = await zangy.create_pool("redis://localhost", 10, 0)
    await pool.set("bench", "yes")
    for i in range(1000000):
        await pool.get("bench")


asyncio.run(main())
