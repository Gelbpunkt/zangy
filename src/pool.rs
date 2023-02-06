use std::{
    intrinsics::unlikely,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use futures_util::StreamExt;
use pyo3::{
    prelude::{pyclass, pymethods, PyObject, PyResult, Python},
    pyasync::{IterANextOutput, PyIterANextOutput},
    types::PyType,
    IntoPy, Py, PyAny, PyRef,
};
use redis::{
    aio::{MultiplexedConnection, PubSub},
    Cmd, Value,
};
use tokio::sync::Mutex as TokioMutex;

use crate::{
    asyncio::{create_future, set_fut_exc, set_fut_result_none, set_fut_result_with_gil},
    conversion::{re_to_object, RedisValuePy},
    exceptions::{ArgumentError, PoolEmpty, PubSubClosed, RedisError},
    runtime::RUNTIME,
};

#[pyclass(module = "zangy")]
pub struct ConnectionPool {
    pub current: AtomicUsize,
    pub pool: Vec<MultiplexedConnection>,
    pub pubsub_pool: Arc<Mutex<Vec<PubSub>>>,
    #[pyo3(get)]
    pub pool_size: usize,
}

impl ConnectionPool {
    fn next_idx(&self) -> usize {
        self.current
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                if x + 1 == self.pool_size {
                    Some(0)
                } else {
                    Some(x + 1)
                }
            })
            .unwrap()
    }

    fn query_cmd(&self, cmd: Cmd) -> PyResult<PyObject> {
        let (fut, res_fut) = create_future()?;

        let idx = self.next_idx();
        let mut conn = self.pool[idx].clone();

        RUNTIME.spawn(async move {
            match cmd.query_async(&mut conn).await {
                Ok(v) => {
                    Python::with_gil(|py| {
                        if let Err(e) = set_fut_result_with_gil(&fut, re_to_object(&v, py), py) {
                            eprintln!("{e:?}");
                        };
                    });
                }
                Err(e) => {
                    let desc = e.to_string();
                    if let Err(e2) = set_fut_exc(&fut, RedisError::new_err(desc)) {
                        eprintln!("{e2:?}");
                    }
                }
            }
        });

        Ok(res_fut)
    }

    fn exec_cmd(&self, cmd: Cmd) -> PyResult<PyObject> {
        let (fut, res_fut) = create_future()?;

        let idx = self.next_idx();
        let mut conn = self.pool[idx].clone();

        RUNTIME.spawn(async move {
            if let Err(e) = cmd
                .query_async::<MultiplexedConnection, ()>(&mut conn)
                .await
            {
                let desc = e.to_string();
                if let Err(e2) = set_fut_exc(&fut, RedisError::new_err(desc)) {
                    eprintln!("{e2:?}");
                }
            } else {
                let _res = set_fut_result_none(&fut);
            };
        });

        Ok(res_fut)
    }
}

#[allow(clippy::needless_pass_by_value)]
#[pymethods]
impl ConnectionPool {
    /// Returns the index of the next connection to be used in the pool.
    #[pyo3(text_signature = "($self)")]
    fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Execute a raw redis command.
    #[pyo3(signature = (*args))]
    #[pyo3(text_signature = "($self, *args)")]
    fn execute(&self, args: Vec<RedisValuePy>) -> PyResult<PyObject> {
        if unlikely(args.is_empty()) {
            return Err(ArgumentError::new_err("no arguments provided to execute"));
        }

        let mut redis_cmd = Cmd::new();
        redis_cmd.arg(args);

        self.query_cmd(redis_cmd)
    }

    /// Set the string value of a key.
    #[pyo3(text_signature = "($self, key, value)")]
    fn set(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set(key, value);
        self.exec_cmd(redis_cmd)
    }

    /// Get the value of a key. If key is a list this becomes an `MGET`.
    #[pyo3(text_signature = "($self, key)")]
    fn get(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::get(key);
        self.query_cmd(redis_cmd)
    }

    /// Gets all keys matching pattern.
    #[pyo3(text_signature = "($self, key)")]
    fn keys(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::keys(key);
        self.query_cmd(redis_cmd)
    }

    /// Set the value and expiration of a key.
    #[pyo3(text_signature = "($self, key, value, seconds)")]
    fn set_ex(&self, key: RedisValuePy, value: RedisValuePy, seconds: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set_ex(key, value, seconds);
        self.exec_cmd(redis_cmd)
    }

    /// Sets multiple keys to their values.
    #[pyo3(text_signature = "($self, items)")]
    fn set_multiple(&self, items: Vec<(RedisValuePy, RedisValuePy)>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set_multiple(&items);
        self.exec_cmd(redis_cmd)
    }

    /// Set the value and expiration in milliseconds of a key.
    #[pyo3(text_signature = "($self, key, value, milliseconds)")]
    fn pset_ex(
        &self,
        key: RedisValuePy,
        value: RedisValuePy,
        milliseconds: usize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pset_ex(key, value, milliseconds);
        self.exec_cmd(redis_cmd)
    }

    /// Set the value of a key, only if the key does not exist.
    #[pyo3(text_signature = "($self, key, value)")]
    fn set_nx(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set_nx(key, value);
        self.exec_cmd(redis_cmd)
    }

    /// Sets multiple keys to their values failing if at least one already
    /// exists.
    #[pyo3(text_signature = "($self, items)")]
    fn mset_nx(&self, items: Vec<(RedisValuePy, RedisValuePy)>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::mset_nx(&items);
        self.exec_cmd(redis_cmd)
    }

    /// Set the string value of a key and return its old value.
    #[pyo3(text_signature = "($self, key, value)")]
    fn getset(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::getset(key, value);
        self.query_cmd(redis_cmd)
    }

    /// Get a range of bytes/substring from the value of a key. Negative values
    /// provide an offset from the end of the value.
    #[pyo3(text_signature = "($self, key, from, to)")]
    fn getrange(&self, key: RedisValuePy, from: isize, to: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::getrange(key, from, to);
        self.query_cmd(redis_cmd)
    }

    /// Overwrite the part of the value stored in key at the specified offset.
    #[pyo3(text_signature = "($self, key, offset, value)")]
    fn setrange(
        &self,
        key: RedisValuePy,
        offset: isize,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::setrange(key, offset, value);
        self.exec_cmd(redis_cmd)
    }

    /// Delete one or more keys.
    #[pyo3(text_signature = "($self, key)")]
    fn del(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::del(key);
        self.exec_cmd(redis_cmd)
    }

    /// Determine if a key exists.
    #[pyo3(text_signature = "($self, key)")]
    fn exists(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::exists(key);
        self.query_cmd(redis_cmd)
    }

    /// Set a key's time to live in seconds.
    #[pyo3(text_signature = "($self, key, seconds)")]
    fn expire(&self, key: RedisValuePy, seconds: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::expire(key, seconds);
        self.exec_cmd(redis_cmd)
    }

    /// Set the expiration for a key as a UNIX timestamp.
    #[pyo3(text_signature = "($self, key, ts)")]
    fn expire_at(&self, key: RedisValuePy, ts: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::expire_at(key, ts);
        self.exec_cmd(redis_cmd)
    }

    /// Set a key's time to live in milliseconds.
    #[pyo3(text_signature = "($self, key, ms)")]
    fn pexpire(&self, key: RedisValuePy, ms: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pexpire(key, ms);
        self.exec_cmd(redis_cmd)
    }

    /// Set the expiration for a key as a UNIX timestamp in milliseconds.
    #[pyo3(text_signature = "($self, key, ts)")]
    fn pexpire_at(&self, key: RedisValuePy, ts: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pexpire_at(key, ts);
        self.exec_cmd(redis_cmd)
    }

    /// Remove the expiration from a key.
    #[pyo3(text_signature = "($self, key)")]
    fn persist(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::persist(key);
        self.exec_cmd(redis_cmd)
    }

    /// Get the expiration time of a key.
    #[pyo3(text_signature = "($self, key)")]
    fn ttl(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::ttl(key);
        self.query_cmd(redis_cmd)
    }

    /// Get the expiration time of a key in milliseconds.
    #[pyo3(text_signature = "($self, key)")]
    fn pttl(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pttl(key);
        self.query_cmd(redis_cmd)
    }

    /// Rename a key.
    #[pyo3(text_signature = "($self, key, new_key)")]
    fn rename(&self, key: RedisValuePy, new_key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rename(key, new_key);
        self.exec_cmd(redis_cmd)
    }

    /// Rename a key, only if the new key does not exist.
    #[pyo3(text_signature = "($self, key, new_key)")]
    fn rename_nx(&self, key: RedisValuePy, new_key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rename_nx(key, new_key);
        self.exec_cmd(redis_cmd)
    }

    /// Append a value to a key.
    #[pyo3(text_signature = "($self, key, value)")]
    fn append(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::append(key, value);
        self.exec_cmd(redis_cmd)
    }

    /// Increment the numeric value of a key by the given amount. This issues a
    /// `INCRBY` or `INCRBYFLOAT` depending on the type.
    #[pyo3(text_signature = "($self, key, delta)")]
    fn incr(&self, key: RedisValuePy, delta: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::incr(key, delta);
        self.exec_cmd(redis_cmd)
    }

    /// Sets or clears the bit at offset in the string value stored at key.
    #[pyo3(text_signature = "($self, key, offset, value)")]
    fn setbit(&self, key: RedisValuePy, offset: usize, value: bool) -> PyResult<PyObject> {
        let redis_cmd = Cmd::setbit(key, offset, value);
        self.exec_cmd(redis_cmd)
    }

    /// Returns the bit value at offset in the string value stored at key.
    #[pyo3(text_signature = "($self, key, offset)")]
    fn getbit(&self, key: RedisValuePy, offset: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::getbit(key, offset);
        self.query_cmd(redis_cmd)
    }

    /// Count set bits in a string.
    #[pyo3(text_signature = "($self, key)")]
    fn bitcount(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bitcount(key);
        self.query_cmd(redis_cmd)
    }

    /// Count set bits in a string in a range.
    #[pyo3(text_signature = "($self, key, start, end)")]
    fn bitcount_range(&self, key: RedisValuePy, start: usize, end: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bitcount_range(key, start, end);
        self.query_cmd(redis_cmd)
    }

    /// Perform a bitwise AND between multiple keys (containing string values)
    /// and store the result in the destination key.
    #[pyo3(text_signature = "($self, dstkey, srckeys)")]
    fn bit_and(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bit_and(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    /// Perform a bitwise OR between multiple keys (containing string values)
    /// and store the result in the destination key.
    #[pyo3(text_signature = "($self, dstkey, srckeys)")]
    fn bit_or(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bit_or(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    /// Perform a bitwise XOR between multiple keys (containing string values)
    /// and store the result in the destination key.
    #[pyo3(text_signature = "($self, dstkey, srckeys)")]
    fn bit_xor(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bit_xor(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    /// Perform a bitwise NOT of the key (containing string values) and store
    /// the result in the destination key.
    #[pyo3(text_signature = "($self, dstkey, srckeys)")]
    fn bit_not(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bit_not(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    /// Get the length of the value stored in a key.
    #[pyo3(text_signature = "($self, key)")]
    fn strlen(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::strlen(key);
        self.query_cmd(redis_cmd)
    }

    /// Gets a single (or multiple) fields from a hash.
    #[pyo3(text_signature = "($self, key, field)")]
    fn hget(&self, key: RedisValuePy, field: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hget(key, field);
        self.query_cmd(redis_cmd)
    }

    /// Deletes a single (or multiple) fields from a hash.
    #[pyo3(text_signature = "($self, key, field)")]
    fn hdel(&self, key: RedisValuePy, field: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hdel(key, field);
        self.exec_cmd(redis_cmd)
    }

    /// Sets a single field in a hash.
    #[pyo3(text_signature = "($self, key, field, value)")]
    fn hset(
        &self,
        key: RedisValuePy,
        field: RedisValuePy,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hset(key, field, value);
        self.exec_cmd(redis_cmd)
    }

    /// Sets a single field in a hash if it does not exist.
    #[pyo3(text_signature = "($self, key, field, value)")]
    fn hset_nx(
        &self,
        key: RedisValuePy,
        field: RedisValuePy,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hset_nx(key, field, value);
        self.exec_cmd(redis_cmd)
    }

    /// Sets a multiple fields in a hash.
    #[pyo3(text_signature = "($self, key, items)")]
    fn hset_multiple(
        &self,
        key: RedisValuePy,
        items: Vec<(RedisValuePy, RedisValuePy)>,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hset_multiple(key, &items);
        self.exec_cmd(redis_cmd)
    }

    /// Increments a value.
    #[pyo3(text_signature = "($self, key, field, delta)")]
    fn hincr(
        &self,
        key: RedisValuePy,
        field: RedisValuePy,
        delta: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hincr(key, field, delta);
        self.exec_cmd(redis_cmd)
    }

    /// Checks if a field in a hash exists.
    #[pyo3(text_signature = "($self, key, field)")]
    fn hexists(&self, key: RedisValuePy, field: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hexists(key, field);
        self.query_cmd(redis_cmd)
    }

    /// Gets all the keys in a hash.
    #[pyo3(text_signature = "($self, key)")]
    fn hkeys(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hkeys(key);
        self.query_cmd(redis_cmd)
    }

    /// Gets all the values in a hash.
    #[pyo3(text_signature = "($self, key)")]
    fn hvals(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hvals(key);
        self.query_cmd(redis_cmd)
    }

    /// Gets all the fields and values in a hash.
    #[pyo3(text_signature = "($self, key)")]
    fn hgetall(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hgetall(key);
        self.query_cmd(redis_cmd)
    }

    /// Gets the length of a hash.
    #[pyo3(text_signature = "($self, key)")]
    fn hlen(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hlen(key);
        self.query_cmd(redis_cmd)
    }

    /// Remove and get the first element in a list, or block until one is
    /// available.
    #[pyo3(text_signature = "($self, key, timeout)")]
    fn blpop(&self, key: RedisValuePy, timeout: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::blpop(key, timeout);
        self.query_cmd(redis_cmd)
    }

    /// Remove and get the last element in a list, or block until one is
    /// available.
    #[pyo3(text_signature = "($self, key, timeout)")]
    fn brpop(&self, key: RedisValuePy, timeout: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::brpop(key, timeout);
        self.query_cmd(redis_cmd)
    }

    /// Pop a value from a list, push it to another list and return it; or block
    /// until one is available.
    #[pyo3(text_signature = "($self, srckey, dstkey, timeout)")]
    fn brpoplpush(
        &self,
        srckey: RedisValuePy,
        dstkey: RedisValuePy,
        timeout: usize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::brpoplpush(srckey, dstkey, timeout);
        self.query_cmd(redis_cmd)
    }

    /// Get an element from a list by its index.
    #[pyo3(text_signature = "($self, key, index)")]
    fn lindex(&self, key: RedisValuePy, index: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lindex(key, index);
        self.query_cmd(redis_cmd)
    }

    /// Insert an element before another element in a list.
    #[pyo3(text_signature = "($self, key, pivot, value)")]
    fn linsert_before(
        &self,
        key: RedisValuePy,
        pivot: RedisValuePy,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::linsert_before(key, pivot, value);
        self.exec_cmd(redis_cmd)
    }

    /// Insert an element after another element in a list.
    #[pyo3(text_signature = "($self, key, pivot, value)")]
    fn linsert_after(
        &self,
        key: RedisValuePy,
        pivot: RedisValuePy,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::linsert_after(key, pivot, value);
        self.exec_cmd(redis_cmd)
    }

    /// Returns the length of the list stored at key.
    #[pyo3(text_signature = "($self, key)")]
    fn llen(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::llen(key);
        self.query_cmd(redis_cmd)
    }

    /// Removes and returns the first element of the list stored at key.
    #[pyo3(text_signature = "($self, key)")]
    fn lpop(&self, key: RedisValuePy, count: Option<usize>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lpop(key, count.and_then(NonZeroUsize::new));
        self.query_cmd(redis_cmd)
    }

    /// Insert all the specified values at the head of the list stored at key.
    #[pyo3(text_signature = "($self, key, value)")]
    fn lpush(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lpush(key, value);
        self.exec_cmd(redis_cmd)
    }

    /// Inserts a value at the head of the list stored at key, only if key
    /// already exists and holds a list.
    #[pyo3(text_signature = "($self, key, value)")]
    fn lpush_exists(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lpush_exists(key, value);
        self.exec_cmd(redis_cmd)
    }

    /// Returns the specified elements of the list stored at key.
    #[pyo3(text_signature = "($self, key, start, stop)")]
    fn lrange(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lrange(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    /// Removes the first count occurrences of elements equal to value from the
    /// list stored at key.
    #[pyo3(text_signature = "($self, key, count, value)")]
    fn lrem(&self, key: RedisValuePy, count: isize, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lrem(key, count, value);
        self.exec_cmd(redis_cmd)
    }

    /// Trim an existing list so that it will contain only the specified range
    /// of elements specified.
    #[pyo3(text_signature = "($self, key, start, stop)")]
    fn ltrim(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::ltrim(key, start, stop);
        self.exec_cmd(redis_cmd)
    }

    /// Sets the list element at index to value.
    #[pyo3(text_signature = "($self, key, index, value)")]
    fn lset(&self, key: RedisValuePy, index: isize, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lset(key, index, value);
        self.exec_cmd(redis_cmd)
    }

    /// Removes and returns the last element of the list stored at key.
    #[pyo3(text_signature = "($self, key)")]
    fn rpop(&self, key: RedisValuePy, count: Option<usize>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rpop(key, count.and_then(NonZeroUsize::new));
        self.query_cmd(redis_cmd)
    }

    /// Pop a value from a list, push it to another list and return it.
    #[pyo3(text_signature = "($self, key, dstkey)")]
    fn rpoplpush(&self, key: RedisValuePy, dstkey: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rpoplpush(key, dstkey);
        self.query_cmd(redis_cmd)
    }

    /// Insert all the specified values at the tail of the list stored at key.
    #[pyo3(text_signature = "($self, key, value)")]
    fn rpush(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rpush(key, value);
        self.exec_cmd(redis_cmd)
    }

    /// Inserts value at the tail of the list stored at key, only if key already
    /// exists and holds a list.
    #[pyo3(text_signature = "($self, key, value)")]
    fn rpush_exists(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rpush_exists(key, value);
        self.exec_cmd(redis_cmd)
    }

    /// Add one or more members to a set.
    #[pyo3(text_signature = "($self, key, member)")]
    fn sadd(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sadd(key, member);
        self.exec_cmd(redis_cmd)
    }

    /// Get the number of members in a set.
    #[pyo3(text_signature = "($self, key)")]
    fn scard(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::scard(key);
        self.query_cmd(redis_cmd)
    }

    /// Subtract multiple sets.
    #[pyo3(text_signature = "($self, keys)")]
    fn sdiff(&self, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sdiff(keys);
        self.query_cmd(redis_cmd)
    }

    /// Subtract multiple sets and store the resulting set in a key.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn sdiffstore(&self, dstkey: RedisValuePy, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sdiffstore(dstkey, keys);
        self.exec_cmd(redis_cmd)
    }

    /// Intersect multiple sets.
    #[pyo3(text_signature = "($self, keys)")]
    fn sinter(&self, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sinter(keys);
        self.query_cmd(redis_cmd)
    }

    /// Intersect multiple sets and store the resulting set in a key.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn sinterstore(&self, dstkey: RedisValuePy, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sinterstore(dstkey, keys);
        self.exec_cmd(redis_cmd)
    }

    /// Determine if a given value is a member of a set.
    #[pyo3(text_signature = "($self, key, member)")]
    fn sismember(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sismember(key, member);
        self.query_cmd(redis_cmd)
    }

    /// Get all the members in a set.
    #[pyo3(text_signature = "($self, key)")]
    fn smembers(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::smembers(key);
        self.query_cmd(redis_cmd)
    }

    /// Move a member from one set to another.
    #[pyo3(text_signature = "($self, srckey, dstkey, member)")]
    fn smove(
        &self,
        srckey: RedisValuePy,
        dstkey: RedisValuePy,
        member: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::smove(srckey, dstkey, member);
        self.exec_cmd(redis_cmd)
    }

    /// Remove and return a random member from a set.
    #[pyo3(text_signature = "($self, key)")]
    fn spop(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::spop(key);
        self.query_cmd(redis_cmd)
    }

    /// Get one random member from a set.
    #[pyo3(text_signature = "($self, key)")]
    fn srandmember(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::srandmember(key);
        self.query_cmd(redis_cmd)
    }

    /// Get multiple random members from a set.
    #[pyo3(text_signature = "($self, key, count)")]
    fn srandmember_multiple(&self, key: RedisValuePy, count: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::srandmember_multiple(key, count);
        self.query_cmd(redis_cmd)
    }

    /// Remove one or more members from a set.
    #[pyo3(text_signature = "($self, key, member)")]
    fn srem(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::srem(key, member);
        self.exec_cmd(redis_cmd)
    }

    /// Add multiple sets.
    #[pyo3(text_signature = "($self, keys)")]
    fn sunion(&self, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sunion(keys);
        self.query_cmd(redis_cmd)
    }

    /// Add multiple sets and store the resulting set in a key.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn sunionstore(&self, dstkey: RedisValuePy, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sunionstore(dstkey, keys);
        self.exec_cmd(redis_cmd)
    }

    /// Add one member to a sorted set, or update its score if it already
    /// exists.
    #[pyo3(text_signature = "($self, key, member, score)")]
    fn zadd(
        &self,
        key: RedisValuePy,
        member: RedisValuePy,
        score: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zadd(key, member, score);
        self.exec_cmd(redis_cmd)
    }

    /// Add multiple members to a sorted set, or update its score if it already
    /// exists.
    #[pyo3(text_signature = "($self, key, items)")]
    fn zadd_multiple(
        &self,
        key: RedisValuePy,
        items: Vec<(RedisValuePy, RedisValuePy)>,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zadd_multiple(key, &items);
        self.exec_cmd(redis_cmd)
    }

    /// Get the number of members in a sorted set.
    #[pyo3(text_signature = "($self, key)")]
    fn zcard(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zcard(key);
        self.query_cmd(redis_cmd)
    }

    /// Count the members in a sorted set with scores within the given values.
    #[pyo3(text_signature = "($self, key, min, max)")]
    fn zcount(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zcount(key, min, max);
        self.query_cmd(redis_cmd)
    }

    /// Increments the member in a sorted set at key by delta. If the member
    /// does not exist, it is added with delta as its score.
    #[pyo3(text_signature = "($self, key, member, delta)")]
    fn zincr(
        &self,
        key: RedisValuePy,
        member: RedisValuePy,
        delta: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zincr(key, member, delta);
        self.exec_cmd(redis_cmd)
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in a
    /// new key using SUM as aggregation function.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn zinterstore(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zinterstore(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in a
    /// new key using MIN as aggregation function.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn zinterstore_min(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zinterstore_min(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in a
    /// new key using MAX as aggregation function.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn zinterstore_max(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zinterstore_max(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    /// Count the number of members in a sorted set between a given
    /// lexicographical range.
    #[pyo3(text_signature = "($self, key, min, max)")]
    fn zlexcount(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zlexcount(key, min, max);
        self.query_cmd(redis_cmd)
    }

    /// Removes and returns up to count members with the highest scores in a
    /// sorted set.
    #[pyo3(text_signature = "($self, key, count)")]
    fn zpopmax(&self, key: RedisValuePy, count: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zpopmax(key, count);
        self.query_cmd(redis_cmd)
    }

    /// Removes and returns up to count members with the lowest scores in a
    /// sorted set.
    #[pyo3(text_signature = "($self, key, count)")]
    fn zpopmin(&self, key: RedisValuePy, count: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zpopmin(key, count);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by index.
    #[pyo3(text_signature = "($self, key, start, stop)")]
    fn zrange(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrange(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by index with scores.
    #[pyo3(text_signature = "($self, key, start, stop)")]
    fn zrange_withscores(
        &self,
        key: RedisValuePy,
        start: isize,
        stop: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrange_withscores(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by lexicographical range.
    #[pyo3(text_signature = "($self, key, min, max)")]
    fn zrangebylex(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebylex(key, min, max);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by lexicographical range with
    /// offset and limit.
    #[pyo3(text_signature = "($self, key, min, max, offset, count)")]
    fn zrangebylex_limit(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
        offset: isize,
        count: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebylex_limit(key, min, max, offset, count);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by lexicographical range.
    #[pyo3(text_signature = "($self, key, max, min)")]
    fn zrevrangebylex(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebylex(key, max, min);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by lexicographical range with
    /// offset and limit.
    #[pyo3(text_signature = "($self, key, max, min, offset, count)")]
    fn zrevrangebylex_limit(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
        offset: isize,
        count: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebylex_limit(key, max, min, offset, count);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by score.
    #[pyo3(text_signature = "($self, key, min, max)")]
    fn zrangebyscore(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebyscore(key, min, max);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by score with scores.
    #[pyo3(text_signature = "($self, key, min, max)")]
    fn zrangebyscore_withscores(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebyscore_withscores(key, min, max);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by score with limit.
    #[pyo3(text_signature = "($self, key, min, max, offset, count)")]
    fn zrangebyscore_limit(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
        offset: isize,
        count: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebyscore_limit(key, min, max, offset, count);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by score with limit with
    /// scores.
    #[pyo3(text_signature = "($self, key, min, max, offset, count)")]
    fn zrangebyscore_limit_withscores(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
        offset: isize,
        count: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebyscore_limit_withscores(key, min, max, offset, count);
        self.query_cmd(redis_cmd)
    }

    /// Determine the index of a member in a sorted set.
    #[pyo3(text_signature = "($self, key, member)")]
    fn zrank(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrank(key, member);
        self.query_cmd(redis_cmd)
    }

    /// Remove one or more members from a sorted set.
    #[pyo3(text_signature = "($self, key, members)")]
    fn zrem(&self, key: RedisValuePy, members: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrem(key, members);
        self.exec_cmd(redis_cmd)
    }

    /// Remove all members in a sorted set between the given lexicographical
    /// range.
    #[pyo3(text_signature = "($self, key, min, max)")]
    fn zrembylex(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrembylex(key, min, max);
        self.exec_cmd(redis_cmd)
    }

    /// Remove all members in a sorted set within the given indexes.
    #[pyo3(text_signature = "($self, key, start, stop)")]
    fn zremrangebyrank(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zremrangebyrank(key, start, stop);
        self.exec_cmd(redis_cmd)
    }

    /// Remove all members in a sorted set within the given scores.
    #[pyo3(text_signature = "($self, key, min, max)")]
    fn zrembyscore(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrembyscore(key, min, max);
        self.exec_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by index, with scores ordered
    /// from high to low.
    #[pyo3(text_signature = "($self, key, start, stop)")]
    fn zrevrange(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrange(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by index, with scores ordered
    /// from high to low.
    #[pyo3(text_signature = "($self, key, start, stop)")]
    fn zrevrange_withscores(
        &self,
        key: RedisValuePy,
        start: isize,
        stop: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrange_withscores(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by score.
    #[pyo3(text_signature = "($self, key, max, min)")]
    fn zrevrangebyscore(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebyscore(key, max, min);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by score with scores.
    #[pyo3(text_signature = "($self, key, max, min)")]
    fn zrevrangebyscore_withscores(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebyscore_withscores(key, max, min);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by score with limit.
    #[pyo3(text_signature = "($self, key, max, min, offset, count)")]
    fn zrevrangebyscore_limit(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
        offset: isize,
        count: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebyscore_limit(key, max, min, offset, count);
        self.query_cmd(redis_cmd)
    }

    /// Return a range of members in a sorted set, by score with limit with
    /// scores.
    #[pyo3(text_signature = "($self, key, max, min, offset, count)")]
    fn zrevrangebyscore_limit_withscores(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
        offset: isize,
        count: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebyscore_limit_withscores(key, max, min, offset, count);
        self.query_cmd(redis_cmd)
    }

    /// Determine the index of a member in a sorted set, with scores ordered
    /// from high to low.
    #[pyo3(text_signature = "($self, key, member)")]
    fn zrevrank(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrank(key, member);
        self.query_cmd(redis_cmd)
    }

    /// Get the score associated with the given member in a sorted set.
    #[pyo3(text_signature = "($self, key, member)")]
    fn zscore(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zscore(key, member);
        self.query_cmd(redis_cmd)
    }

    /// Unions multiple sorted sets and store the resulting sorted set in a new
    /// key using SUM as aggregation function.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn zunionstore(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zunionstore(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    /// Unions multiple sorted sets and store the resulting sorted set in a new
    /// key using MIN as aggregation function.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn zunionstore_min(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zunionstore_min(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    /// Unions multiple sorted sets and store the resulting sorted set in a new
    /// key using MAX as aggregation function.
    #[pyo3(text_signature = "($self, dstkey, keys)")]
    fn zunionstore_max(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zunionstore_max(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    /// Adds the specified elements to the specified HyperLogLog.
    #[pyo3(text_signature = "($self, key, element)")]
    fn pfadd(&self, key: RedisValuePy, element: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pfadd(key, element);
        self.exec_cmd(redis_cmd)
    }

    /// Return the approximated cardinality of the set(s) observed by the
    /// HyperLogLog at key(s).
    #[pyo3(text_signature = "($self, key)")]
    fn pfcount(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pfcount(key);
        self.query_cmd(redis_cmd)
    }

    /// Merge N different HyperLogLogs into a single one.
    #[pyo3(text_signature = "($self, dstkey, srckeys)")]
    fn pfmerge(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pfmerge(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    /// Posts a message to the given channel.
    #[pyo3(text_signature = "($self, channel, message)")]
    fn publish(&self, channel: RedisValuePy, message: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::publish(channel, message);
        self.exec_cmd(redis_cmd)
    }

    #[pyo3(text_signature = "($self)")]
    fn pubsub(&mut self) -> PyResult<PyObject> {
        let other_conns = self.pubsub_pool.clone();
        let mut conns = self.pubsub_pool.lock().unwrap();
        match conns.pop() {
            Some(conn) => {
                let ctx = PubSubContext {
                    connection: Arc::new(TokioMutex::new(Some(conn))),
                    pool: other_conns,
                };

                Ok(Python::with_gil(|py| ctx.into_py(py)))
            }
            None => Err(PoolEmpty::new_err("PubSub pool is empty")),
        }
    }
}

#[pyclass(module = "zangy")]
struct PubSubContext {
    connection: Arc<TokioMutex<Option<PubSub>>>,
    pool: Arc<Mutex<Vec<PubSub>>>,
}

#[pymethods]
impl PubSubContext {
    /// Subscribes to a new channel.
    #[pyo3(text_signature = "($self, channel)")]
    fn subscribe(&self, channel: RedisValuePy) -> PyResult<PyObject> {
        let (fut, res_fut) = create_future()?;
        let conn = self.connection.clone();

        RUNTIME.spawn(async move {
            match *conn.lock().await {
                Some(ref mut v) => {
                    if let Err(e) = v.subscribe(channel).await {
                        let desc = e.to_string();
                        if let Err(e2) = set_fut_exc(&fut, RedisError::new_err(desc)) {
                            eprintln!("{e2:?}");
                        }
                    } else {
                        let _res = set_fut_result_none(&fut);
                    };
                }
                None => {
                    if let Err(e) = set_fut_exc(
                        &fut,
                        PubSubClosed::new_err("context manager has been exited"),
                    ) {
                        eprintln!("{e:?}");
                    }
                }
            }
        });

        Ok(res_fut)
    }

    /// Subscribes to a new channel with a pattern.
    #[pyo3(text_signature = "($self, pchannel)")]
    fn psubscribe(&self, pchannel: RedisValuePy) -> PyResult<PyObject> {
        let (fut, res_fut) = create_future()?;
        let conn = self.connection.clone();

        RUNTIME.spawn(async move {
            match *conn.lock().await {
                Some(ref mut v) => {
                    if let Err(e) = v.psubscribe(pchannel).await {
                        let desc = e.to_string();
                        if let Err(e2) = set_fut_exc(&fut, RedisError::new_err(desc)) {
                            eprintln!("{e2:?}");
                        }
                    } else {
                        let _res = set_fut_result_none(&fut);
                    };
                }
                None => {
                    if let Err(e) = set_fut_exc(
                        &fut,
                        PubSubClosed::new_err("context manager has been exited"),
                    ) {
                        eprintln!("{e:?}");
                    }
                }
            }
        });

        Ok(res_fut)
    }

    /// Unsubscribes from a channel.
    #[pyo3(text_signature = "($self, channel)")]
    fn unsubscribe(&self, channel: RedisValuePy) -> PyResult<PyObject> {
        let (fut, res_fut) = create_future()?;
        let conn = self.connection.clone();

        RUNTIME.spawn(async move {
            match *conn.lock().await {
                Some(ref mut v) => {
                    if let Err(e) = v.unsubscribe(channel).await {
                        let desc = e.to_string();
                        if let Err(e2) = set_fut_exc(&fut, RedisError::new_err(desc)) {
                            eprintln!("{e2:?}");
                        }
                    } else {
                        let _res = set_fut_result_none(&fut);
                    };
                }
                None => {
                    if let Err(e) = set_fut_exc(
                        &fut,
                        PubSubClosed::new_err("context manager has been exited"),
                    ) {
                        eprintln!("{e:?}");
                    }
                }
            }
        });

        Ok(res_fut)
    }

    /// Unsubscribes from a channel with a pattern.
    #[pyo3(text_signature = "($self, pchannel)")]
    fn punsubscribe(&self, pchannel: RedisValuePy) -> PyResult<PyObject> {
        let (fut, res_fut) = create_future()?;
        let conn = self.connection.clone();

        RUNTIME.spawn(async move {
            match *conn.lock().await {
                Some(ref mut v) => {
                    if let Err(e) = v.psubscribe(pchannel).await {
                        let desc = e.to_string();
                        if let Err(e2) = set_fut_exc(&fut, RedisError::new_err(desc)) {
                            eprintln!("{e2:?}");
                        }
                    } else {
                        let _res = set_fut_result_none(&fut);
                    };
                }
                None => {
                    if let Err(e) = set_fut_exc(
                        &fut,
                        PubSubClosed::new_err("context manager has been exited"),
                    ) {
                        eprintln!("{e:?}");
                    }
                }
            }
        });

        Ok(res_fut)
    }

    // Impossible to return Self in the ContextProtocol
    // so we do it here
    // https://github.com/PyO3/pyo3/issues/1205#issuecomment-778529199
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<&PyType>,
        _exc_value: Option<&PyAny>,
        _traceback: Option<&PyAny>,
    ) {
        let conn = self.connection.clone();
        let pool = self.pool.clone();

        RUNTIME.spawn(async move {
            let conn = conn.lock().await.take().unwrap();
            pool.lock().unwrap().push(conn);
        });
    }

    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__(&self) -> PyResult<PyIterANextOutput> {
        let (fut, res_fut) = create_future()?;
        let conn = self.connection.clone();

        RUNTIME.spawn(async move {
            match *conn.lock().await {
                Some(ref mut c) => match c.on_message().next().await {
                    Some(m) => {
                        // We need (channel, msg)
                        let val: Value = m.get_payload().unwrap();
                        let channel = m.get_channel_name();

                        Python::with_gil(|py| {
                            let channel_py: Py<PyAny> = channel.into_py(py);

                            if let Err(e) = set_fut_result_with_gil(
                                &fut,
                                (channel_py, re_to_object(&val, py)).into_py(py),
                                py,
                            ) {
                                eprintln!("{e:?}");
                            };
                        });
                    }
                    None => {
                        if let Err(e2) = set_fut_result_none(&fut) {
                            eprintln!("{e2:?}");
                        }
                    }
                },
                None => {
                    if let Err(e) = set_fut_exc(
                        &fut,
                        PubSubClosed::new_err("context manager has been exited"),
                    ) {
                        eprintln!("{e:?}");
                    }
                }
            }
        });

        Ok(IterANextOutput::Yield(res_fut))
    }
}
