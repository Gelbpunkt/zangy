# zangy

A fast redis library for python written in Rust using PyO3.

## Installation

`pip install --user zangy`

Building from source requires nightly Rust.

## How does it work?

zangy aims to be the fastest python redis library. This is done by using [pyo3](https://pyo3.rs) to generate shared objects in binary form. It is pretty much identical to writing this in C, but less of a pain to compile and identical in speed.

Due to being completely in Rust, zangy can't do lifetime-based connection pooling and instead will create connections on startup and not lazily. All actions are distributed over the pool based on round robin. Internally, [redis-rs](https://github.com/mitsuhiko/redis-rs) is used for the redis operations and [tokio](https://github.com/tokio-rs/tokio) is used to spawn tasks outside the GIL.

Because it uses tokio and rust-level tasks, zangy unleashes maximum performance when used with a _lot_ of concurrent things to do.

## Is it fast?

Yes! It beats similar Python libraries by a fair margin. Tokio, no GIL lock and the speed of Rust especially show when setting 1 million keys _in parallel_.

Benchmark sources can be found in the `bench` directory.

Benchmarks below done with Redis 7.0.8 and Python 3.11.1, redis-py 4.4.2 and the latest zangy master using a pool with 10 connections:

| Task                     | aioredis                       | zangy    |
| ------------------------ | ------------------------------ | -------- |
| 1.000.000 sequential GET | 2min 34s                       | 1min 44s |
| 1.000.000 sequential SET | 2min 45s                       | 1min 52s |
| 1.000.000 parallel SET   | Pool times out after 18min 52s | 17s      |

TLDR: zangy is faster in every regard but crushes in actually concurrent scenarios.

## Usage

The API is subject to change.

```py
import zangy
# Create a pool with 2 connections and 2 pubsub connections
pool = await zangy.create_pool("redis://localhost:6379", 2, 2)
# Generic redis commands (disadvised)
await pool.execute("SET", "a", "b")
# Individual commands
value = await pool.get("a")

# Wait for pubsub messages and echo back
with pool.pubsub() as pubsub:
    await pubsub.subscribe("test1")
    async for (channel, payload) in pubsub:
        print(channel, payload)
        await pool.publish("test2", payload)
```

Aliases for almost all operations exist on pool (`.set`, `.set_ex`, `.zrange`, etc).

## What is not supported?

- Single connections. Just use a pool with 1 member.
