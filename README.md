# zangy

A fast redis library for python written in Rust using PyO3.

## Installation

I can't bother providing manylinux wheels yet and we are pre-1.0, so you will have to build it yourself.
You need python 3.8 or newer, nightly Rust and Cargo:

```sh
git clone https://github.com/Gelbpunkt/zangy.git
cd zangy
pip install --user maturin
maturin build --no-sdist --release --strip --manylinux off --interpreter python3
pip install target/wheels/*.whl -U --user
```

## How does it work?

zangy aims to be the fastest python redis library. This is done by using [pyo3](https://pyo3.rs) to generate shared objects in binary form. It is pretty much identical to writing this in C, but less of a pain to compile and identical in speed.

Due to being completely in Rust, zangy can't do lifetime-based connection pooling and instead will create connections on startup and not lazily. All actions are distributed over the pool based on round robin. Internally, [redis-rs](https://github.com/mitsuhiko/redis-rs) is used for the redis operations and [async-std](https://github.com/async-rs/async-std) is used to spawn tasks outside the GIL.

Because it uses async-std and rust-level tasks, zangy unleashes maximum performance when used with a *lot* of concurrent things to do.

## Is it fast?

Well... it depends. Currently, zangy is slower than [aioredis](https://github.com/aio-libs/aioredis) when looping over an operation. But async-std, no GIL lock and the speed of Rust show when setting 1 million keys *in parallel*.

Benchmark sources can be found in the `bench` directory.

Benchmarks below done with Redis 6.0.6 and Python 3.8, aioredis 1.3.1 and the latest zangy master:

| Task                                              | aioredis operations/s | aioredis total time | zangy operations/s | zangy total time |
| ------------------------------------------------- | --------------------- | ------------------- | ------------------ | ---------------- |
| Loop 1 million times, set key to value            | 8154.0                | 2m2.767s            | 7632.19            | 2m11.106s        |
| Set 1 million keys at once and wait for finishing | -                     | 0m49.456s           | -                  | 0m26.156s        |

TLDR: zangy is faster in actually concurrent situations. However I aim to outperform aioredis in the other benchmark as well.

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

* Pubsub (planned)
* Single connections. Just use a pool with 1 member.
