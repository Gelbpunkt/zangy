use crate::asyncio::{create_future, set_fut_exc, set_fut_result};
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

        let (fut, res_fut, loop_) = create_future()?;

        let idx = self.next_idx();
        let mut conn = self.pool[idx].clone();

        task::spawn(async move {
            match redis_cmd.query_async(&mut conn).await {
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

    fn set(&self, key: String, value: RedisValuePy) -> PyResult<PyObject> {
        let redis_cmd = Cmd::set(key, value);

        let (fut, res_fut, loop_) = create_future()?;

        let idx = self.next_idx();
        let mut conn = self.pool[idx].clone();

        task::spawn(async move {
            match redis_cmd.query_async(&mut conn).await {
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

    fn get(&self, key: String) -> PyResult<PyObject> {
        let redis_cmd = Cmd::get(key);

        let (fut, res_fut, loop_) = create_future()?;

        let idx = self.next_idx();
        let mut conn = self.pool[idx].clone();

        task::spawn(async move {
            match redis_cmd.query_async(&mut conn).await {
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
}
