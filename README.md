# zangy

A fast redis library for python written in Rust using PyO3.

## Installation

Until pubsub is implemented, I won't publish zangy on PyPi. To get around the install hazzle, download a wheel from the latest CI run by clicking [here](https://github.com/Gelbpunkt/zangy/actions) and then clicking on a run, then download the wheels.zip. It will include manylinux wheels for most recent python versions.

Building from source requires nightly Rust.

## How does it work?

zangy aims to be the fastest python redis library. This is done by using [pyo3](https://pyo3.rs) to generate shared objects in binary form. It is pretty much identical to writing this in C, but less of a pain to compile and identical in speed.

Due to being completely in Rust, zangy can't do lifetime-based connection pooling and instead will create connections on startup and not lazily. All actions are distributed over the pool based on round robin. Internally, [redis-rs](https://github.com/mitsuhiko/redis-rs) is used for the redis operations and [tokio](https://github.com/tokio-rs/tokio) is used to spawn tasks outside the GIL.

Because it uses tokio and rust-level tasks, zangy unleashes maximum performance when used with a _lot_ of concurrent things to do.

## Is it fast?

Yes! It beats similar Python libraries by a fair margin. Tokio, no GIL lock and the speed of Rust especially show when setting 1 million keys _in parallel_.

Benchmark sources can be found in the `bench` directory.

Benchmarks below done with Redis 6.0.9 and Python 3.9, aioredis 1.3.1 and the latest zangy master:

| Task                                              | aioredis operations/s | aioredis total time | zangy operations/s | zangy total time |
| ------------------------------------------------- | --------------------- | ------------------- | ------------------ | ---------------- |
| Loop 1 million times, set key to value            | 7941.61               | 2m6.054s            | 8485.11            | 1m57.923s        |
| Set 1 million keys at once and wait for finishing | -                     | 0m49.797s           | -                  | 0m25.294s        |

TLDR: zangy is faster in every regard but crushes in actually concurrent scenarios.

## Usage

The API is subject to change.

```py
import zangy
# Create a pool with 2 connections
pool = await zangy.create_pool("redis://localhost:6379", 2)
# Generic redis commands (disadvised)
await pool.execute("SET", "a", "b")
# Individual commands
value = await pool.get("a")
```

Aliases for almost all operations exist on pool (`.set`, `.set_ex`, `.zrange`, etc).

## What is not supported?

- Pubsub (planned)
- Single connections. Just use a pool with 1 member.
