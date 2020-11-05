use crate::asyncio::{create_future, set_fut_exc, set_fut_result, set_fut_result_none};
use crate::conversion::{re_to_object, RedisValuePy};
use crate::exceptions::{ArgumentError, ConnectionError, RedisError};
use async_std::task;
use pyo3::{
    prelude::{pyclass, pymethods, IntoPy, PyObject, PyResult, Python},
    types::PyType,
};
use redis::{aio::MultiplexedConnection, Client, Cmd};
use std::sync::atomic::{AtomicUsize, Ordering};

#[pyclass(module = "zangy")]
pub struct ConnectionPool {
    current: AtomicUsize,
    pool: Vec<MultiplexedConnection>,
    #[pyo3(get)]
    pool_size: usize,
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
        let (fut, res_fut, loop_) = create_future()?;

        let idx = self.next_idx();
        let mut conn = self.pool[idx].clone();

        task::spawn(async move {
            match cmd.query_async(&mut conn).await {
                Ok(v) => {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    if let Err(e) = set_fut_result(loop_, fut, re_to_object(&v, py)) {
                        eprintln!("{:?}", e);
                    };
                }
                Err(e) => {
                    let desc = format!("{}", e);
                    if let Err(e2) = set_fut_exc(loop_, fut, RedisError::new_err(desc)) {
                        eprintln!("{:?}", e2);
                    }
                }
            }
        });

        Ok(res_fut)
    }

    fn exec_cmd(&self, cmd: Cmd) -> PyResult<PyObject> {
        let (fut, res_fut, loop_) = create_future()?;

        let idx = self.next_idx();
        let mut conn = self.pool[idx].clone();

        task::spawn(async move {
            if let Err(e) = cmd
                .query_async::<MultiplexedConnection, ()>(&mut conn)
                .await
            {
                let desc = format!("{}", e);
                if let Err(e2) = set_fut_exc(loop_, fut, RedisError::new_err(desc)) {
                    eprintln!("{:?}", e2);
                }
            } else {
                let _ = set_fut_result_none(loop_, fut);
            };
        });

        Ok(res_fut)
    }
}

#[pymethods]
impl ConnectionPool {
    #[classmethod]
    fn connect(_cls: &PyType, address: String, pool_size: u16) -> PyResult<PyObject> {
        let (fut, res_fut, loop_) = create_future()?;

        task::spawn(async move {
            let client = Client::open(address);

            match client {
                Ok(client) => {
                    let mut connections = Vec::new();
                    for _ in 0..pool_size {
                        let connection = client.get_multiplexed_async_std_connection().await;

                        match connection {
                            Ok(conn) => connections.push(conn),
                            Err(e) => {
                                let _ = set_fut_exc(
                                    loop_,
                                    fut,
                                    ConnectionError::new_err(format!("{}", e)),
                                );
                                return;
                            }
                        }
                    }

                    let pool = Self {
                        current: AtomicUsize::new(0),
                        pool: connections,
                        pool_size: pool_size as usize,
                    };
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    let _ = set_fut_result(loop_, fut, pool.into_py(py));
                }
                Err(e) => {
                    let _ = set_fut_exc(loop_, fut, ConnectionError::new_err(format!("{}", e)));
                }
            }
        });

        Ok(res_fut)
    }

    fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    #[args(args = "*")]
    fn execute(&self, args: Vec<RedisValuePy>) -> PyResult<PyObject> {
        if args.len() == 0 {
            return Err(ArgumentError::new_err("no arguments provided to execute"));
        }

        let mut redis_cmd = Cmd::new();
        redis_cmd.arg(args);

        self.query_cmd(redis_cmd)
    }

    fn set(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set(key, value);
        self.exec_cmd(redis_cmd)
    }

    fn get(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::get(key);
        self.query_cmd(redis_cmd)
    }

    fn keys(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::keys(key);
        self.query_cmd(redis_cmd)
    }

    fn set_ex(&self, key: RedisValuePy, value: RedisValuePy, seconds: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set_ex(key, value, seconds);
        self.exec_cmd(redis_cmd)
    }

    fn set_multiple(&self, items: Vec<(RedisValuePy, RedisValuePy)>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set_multiple(&items);
        self.exec_cmd(redis_cmd)
    }

    fn pset_ex(
        &self,
        key: RedisValuePy,
        value: RedisValuePy,
        milliseconds: usize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pset_ex(key, value, milliseconds);
        self.exec_cmd(redis_cmd)
    }

    fn set_nx(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set_nx(key, value);
        self.exec_cmd(redis_cmd)
    }

    fn mset_nx(&self, items: Vec<(RedisValuePy, RedisValuePy)>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::mset_nx(&items);
        self.exec_cmd(redis_cmd)
    }

    fn getset(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::getset(key, value);
        self.query_cmd(redis_cmd)
    }

    fn getrange(&self, key: RedisValuePy, from: isize, to: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::getrange(key, from, to);
        self.query_cmd(redis_cmd)
    }

    fn setrange(
        &self,
        key: RedisValuePy,
        offset: isize,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::setrange(key, offset, value);
        self.exec_cmd(redis_cmd)
    }

    fn del(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::del(key);
        self.exec_cmd(redis_cmd)
    }

    fn exists(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::exists(key);
        self.query_cmd(redis_cmd)
    }

    fn expire(&self, key: RedisValuePy, seconds: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::expire(key, seconds);
        self.exec_cmd(redis_cmd)
    }

    fn expire_at(&self, key: RedisValuePy, ts: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::expire_at(key, ts);
        self.exec_cmd(redis_cmd)
    }

    fn pexpire(&self, key: RedisValuePy, ms: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pexpire(key, ms);
        self.exec_cmd(redis_cmd)
    }

    fn pexpire_at(&self, key: RedisValuePy, ts: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pexpire_at(key, ts);
        self.exec_cmd(redis_cmd)
    }

    fn persist(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::persist(key);
        self.exec_cmd(redis_cmd)
    }

    fn ttl(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::ttl(key);
        self.query_cmd(redis_cmd)
    }

    fn pttl(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pttl(key);
        self.query_cmd(redis_cmd)
    }

    fn rename(&self, key: RedisValuePy, new_key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rename(key, new_key);
        self.exec_cmd(redis_cmd)
    }

    fn rename_nx(&self, key: RedisValuePy, new_key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rename_nx(key, new_key);
        self.exec_cmd(redis_cmd)
    }

    fn append(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::append(key, value);
        self.exec_cmd(redis_cmd)
    }

    fn incr(&self, key: RedisValuePy, delta: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::incr(key, delta);
        self.exec_cmd(redis_cmd)
    }

    fn setbit(&self, key: RedisValuePy, offset: usize, value: bool) -> PyResult<PyObject> {
        let redis_cmd = Cmd::setbit(key, offset, value);
        self.exec_cmd(redis_cmd)
    }

    fn getbit(&self, key: RedisValuePy, offset: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::getbit(key, offset);
        self.query_cmd(redis_cmd)
    }

    fn bitcount(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bitcount(key);
        self.query_cmd(redis_cmd)
    }

    fn bitcount_range(&self, key: RedisValuePy, start: usize, end: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bitcount_range(key, start, end);
        self.query_cmd(redis_cmd)
    }

    fn bit_and(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bit_and(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    fn bit_or(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bit_or(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    fn bit_xor(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bit_xor(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    fn bit_not(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::bit_not(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    fn strlen(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::strlen(key);
        self.query_cmd(redis_cmd)
    }

    fn hget(&self, key: RedisValuePy, field: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hget(key, field);
        self.query_cmd(redis_cmd)
    }

    fn hdel(&self, key: RedisValuePy, field: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hdel(key, field);
        self.exec_cmd(redis_cmd)
    }

    fn hset(
        &self,
        key: RedisValuePy,
        field: RedisValuePy,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hset(key, field, value);
        self.exec_cmd(redis_cmd)
    }

    fn hset_nx(
        &self,
        key: RedisValuePy,
        field: RedisValuePy,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hset_nx(key, field, value);
        self.exec_cmd(redis_cmd)
    }

    fn hset_multiple(
        &self,
        key: RedisValuePy,
        items: Vec<(RedisValuePy, RedisValuePy)>,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hset_multiple(key, &items);
        self.exec_cmd(redis_cmd)
    }

    fn hincr(
        &self,
        key: RedisValuePy,
        field: RedisValuePy,
        delta: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hincr(key, field, delta);
        self.exec_cmd(redis_cmd)
    }

    fn hexists(&self, key: RedisValuePy, field: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hexists(key, field);
        self.query_cmd(redis_cmd)
    }

    fn hkeys(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hkeys(key);
        self.query_cmd(redis_cmd)
    }

    fn hvals(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hvals(key);
        self.query_cmd(redis_cmd)
    }

    fn hgetall(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hgetall(key);
        self.query_cmd(redis_cmd)
    }

    fn hlen(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::hlen(key);
        self.query_cmd(redis_cmd)
    }

    fn blpop(&self, key: RedisValuePy, timeout: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::blpop(key, timeout);
        self.query_cmd(redis_cmd)
    }

    fn brpop(&self, key: RedisValuePy, timeout: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::brpop(key, timeout);
        self.query_cmd(redis_cmd)
    }

    fn brpoplpush(
        &self,
        srckey: RedisValuePy,
        dstkey: RedisValuePy,
        timeout: usize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::brpoplpush(srckey, dstkey, timeout);
        self.query_cmd(redis_cmd)
    }

    fn lindex(&self, key: RedisValuePy, index: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lindex(key, index);
        self.query_cmd(redis_cmd)
    }

    fn linsert_before(
        &self,
        key: RedisValuePy,
        pivot: RedisValuePy,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::linsert_before(key, pivot, value);
        self.exec_cmd(redis_cmd)
    }

    fn linsert_after(
        &self,
        key: RedisValuePy,
        pivot: RedisValuePy,
        value: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::linsert_after(key, pivot, value);
        self.exec_cmd(redis_cmd)
    }

    fn llen(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::llen(key);
        self.query_cmd(redis_cmd)
    }

    fn lpop(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lpop(key);
        self.query_cmd(redis_cmd)
    }

    fn lpush(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lpush(key, value);
        self.exec_cmd(redis_cmd)
    }

    fn lpush_exists(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lpush_exists(key, value);
        self.exec_cmd(redis_cmd)
    }

    fn lrange(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lrange(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    fn lrem(&self, key: RedisValuePy, count: isize, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lrem(key, count, value);
        self.exec_cmd(redis_cmd)
    }

    fn ltrim(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::ltrim(key, start, stop);
        self.exec_cmd(redis_cmd)
    }

    fn lset(&self, key: RedisValuePy, index: isize, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::lset(key, index, value);
        self.exec_cmd(redis_cmd)
    }

    fn rpop(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rpop(key);
        self.query_cmd(redis_cmd)
    }

    fn rpoplpush(&self, key: RedisValuePy, dstkey: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rpoplpush(key, dstkey);
        self.query_cmd(redis_cmd)
    }

    fn rpush(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rpush(key, value);
        self.exec_cmd(redis_cmd)
    }

    fn rpush_exists(&self, key: RedisValuePy, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::rpush_exists(key, value);
        self.exec_cmd(redis_cmd)
    }

    fn sadd(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sadd(key, member);
        self.exec_cmd(redis_cmd)
    }

    fn scard(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::scard(key);
        self.query_cmd(redis_cmd)
    }

    fn sdiff(&self, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sdiff(keys);
        self.query_cmd(redis_cmd)
    }

    fn sdiffstore(&self, dstkey: RedisValuePy, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sdiffstore(dstkey, keys);
        self.exec_cmd(redis_cmd)
    }

    fn sinter(&self, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sinter(keys);
        self.query_cmd(redis_cmd)
    }

    fn sinterstore(&self, dstkey: RedisValuePy, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sinterstore(dstkey, keys);
        self.exec_cmd(redis_cmd)
    }

    fn sismember(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sismember(key, member);
        self.query_cmd(redis_cmd)
    }

    fn smembers(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::smembers(key);
        self.query_cmd(redis_cmd)
    }

    fn smove(
        &self,
        srckey: RedisValuePy,
        dstkey: RedisValuePy,
        member: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::smove(srckey, dstkey, member);
        self.exec_cmd(redis_cmd)
    }

    fn spop(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::spop(key);
        self.query_cmd(redis_cmd)
    }

    fn srandmember(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::srandmember(key);
        self.query_cmd(redis_cmd)
    }

    fn srandmember_multiple(&self, key: RedisValuePy, count: usize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::srandmember_multiple(key, count);
        self.query_cmd(redis_cmd)
    }

    fn srem(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::srem(key, member);
        self.exec_cmd(redis_cmd)
    }

    fn sunion(&self, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sunion(keys);
        self.query_cmd(redis_cmd)
    }

    fn sunionstore(&self, dstkey: RedisValuePy, keys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::sunionstore(dstkey, keys);
        self.exec_cmd(redis_cmd)
    }

    fn zadd(
        &self,
        key: RedisValuePy,
        member: RedisValuePy,
        score: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zadd(key, member, score);
        self.exec_cmd(redis_cmd)
    }

    fn zadd_multiple(
        &self,
        key: RedisValuePy,
        items: Vec<(RedisValuePy, RedisValuePy)>,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zadd_multiple(key, &items);
        self.exec_cmd(redis_cmd)
    }

    fn zcard(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zcard(key);
        self.query_cmd(redis_cmd)
    }

    fn zcount(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zcount(key, min, max);
        self.query_cmd(redis_cmd)
    }

    fn zincr(
        &self,
        key: RedisValuePy,
        member: RedisValuePy,
        delta: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zincr(key, member, delta);
        self.exec_cmd(redis_cmd)
    }

    fn zinterstore(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zinterstore(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    fn zinterstore_min(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zinterstore_min(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    fn zinterstore_max(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zinterstore_max(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    fn zlexcount(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zlexcount(key, min, max);
        self.query_cmd(redis_cmd)
    }

    fn zpopmax(&self, key: RedisValuePy, count: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zpopmax(key, count);
        self.query_cmd(redis_cmd)
    }

    fn zpopmin(&self, key: RedisValuePy, count: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zpopmin(key, count);
        self.query_cmd(redis_cmd)
    }

    fn zrange(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrange(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    fn zrange_withscores(
        &self,
        key: RedisValuePy,
        start: isize,
        stop: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrange_withscores(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    fn zrangebylex(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebylex(key, min, max);
        self.query_cmd(redis_cmd)
    }

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

    fn zrevrangebylex(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebylex(key, max, min);
        self.query_cmd(redis_cmd)
    }

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

    fn zrangebyscore(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebyscore(key, min, max);
        self.query_cmd(redis_cmd)
    }

    fn zrangebyscore_withscores(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrangebyscore_withscores(key, min, max);
        self.query_cmd(redis_cmd)
    }

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

    fn zrank(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrank(key, member);
        self.query_cmd(redis_cmd)
    }

    fn zrem(&self, key: RedisValuePy, members: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrem(key, members);
        self.exec_cmd(redis_cmd)
    }

    fn zrembylex(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrembylex(key, min, max);
        self.exec_cmd(redis_cmd)
    }

    fn zremrangebyrank(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zremrangebyrank(key, start, stop);
        self.exec_cmd(redis_cmd)
    }

    fn zrembyscore(
        &self,
        key: RedisValuePy,
        min: RedisValuePy,
        max: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrembyscore(key, min, max);
        self.exec_cmd(redis_cmd)
    }

    fn zrevrange(&self, key: RedisValuePy, start: isize, stop: isize) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrange(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    fn zrevrange_withscores(
        &self,
        key: RedisValuePy,
        start: isize,
        stop: isize,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrange_withscores(key, start, stop);
        self.query_cmd(redis_cmd)
    }

    fn zrevrangebyscore(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebyscore(key, max, min);
        self.query_cmd(redis_cmd)
    }

    fn zrevrangebyscore_withscores(
        &self,
        key: RedisValuePy,
        max: RedisValuePy,
        min: RedisValuePy,
    ) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrangebyscore_withscores(key, max, min);
        self.query_cmd(redis_cmd)
    }

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

    fn zrevrank(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zrevrank(key, member);
        self.query_cmd(redis_cmd)
    }

    fn zscore(&self, key: RedisValuePy, member: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zscore(key, member);
        self.query_cmd(redis_cmd)
    }

    fn zunionstore(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zunionstore(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    fn zunionstore_min(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zunionstore_min(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    fn zunionstore_max(&self, dstkey: RedisValuePy, keys: Vec<RedisValuePy>) -> PyResult<PyObject> {
        let redis_cmd = Cmd::zunionstore_max(dstkey, &keys);
        self.exec_cmd(redis_cmd)
    }

    fn pfadd(&self, key: RedisValuePy, element: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pfadd(key, element);
        self.exec_cmd(redis_cmd)
    }

    fn pfcount(&self, key: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pfcount(key);
        self.query_cmd(redis_cmd)
    }

    fn pfmerge(&self, dstkey: RedisValuePy, srckeys: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::pfmerge(dstkey, srckeys);
        self.exec_cmd(redis_cmd)
    }

    fn publish(&self, channel: RedisValuePy, message: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::publish(channel, message);
        self.exec_cmd(redis_cmd)
    }
}
