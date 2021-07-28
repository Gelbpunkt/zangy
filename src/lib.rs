use pyo3::{
    prelude::{pyfunction, pymodule, IntoPy, PyModule, PyObject, PyResult, Python},
    wrap_pyfunction,
};
use redis::Client;
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

mod asyncio;
mod conversion;
mod exceptions;
mod pool;
mod runtime;

/// Connect to a redis server at `address` and use up to `pool_size` connections.
#[pyfunction]
#[pyo3(text_signature = "(address, pool_size)")]
fn create_pool(address: String, pool_size: u16, pubsub_size: u16) -> PyResult<PyObject> {
    let (fut, res_fut) = asyncio::create_future()?;

    runtime::RUNTIME.spawn(async move {
        let client = Client::open(address);

        match client {
            Ok(client) => {
                let mut connections = Vec::with_capacity(pool_size as usize);
                for _ in 0..pool_size {
                    let connection = client.get_multiplexed_tokio_connection().await;

                    match connection {
                        Ok(conn) => connections.push(conn),
                        Err(e) => {
                            let _ = asyncio::set_fut_exc(
                                fut,
                                exceptions::ConnectionError::new_err(format!("{}", e)),
                            );
                            return;
                        }
                    }
                }
                let mut pubsub_connections = Vec::with_capacity(pubsub_size as usize);
                for _ in 0..pubsub_size {
                    let connection = client.get_tokio_connection().await;

                    match connection {
                        Ok(conn) => pubsub_connections.push(conn.into_pubsub()),
                        Err(e) => {
                            let _ = asyncio::set_fut_exc(
                                fut,
                                exceptions::ConnectionError::new_err(format!("{}", e)),
                            );
                            return;
                        }
                    }
                }

                let pool = pool::ConnectionPool {
                    current: AtomicUsize::new(0),
                    pool: connections,
                    pubsub_pool: Arc::new(Mutex::new(pubsub_connections)),
                    pool_size: pool_size as usize,
                };
                let gil = Python::acquire_gil();
                let py = gil.python();
                let _ = asyncio::set_fut_result(fut, pool.into_py(py));
            }
            Err(e) => {
                let _ = asyncio::set_fut_exc(
                    fut,
                    exceptions::ConnectionError::new_err(format!("{}", e)),
                );
            }
        }
    });

    Ok(res_fut)
}

#[pymodule]
fn zangy(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_pool, m)?)?;
    m.add_class::<pool::ConnectionPool>()?;
    m.add(
        "ConnectionError",
        py.get_type::<exceptions::ConnectionError>(),
    )?;
    m.add("ArgumentError", py.get_type::<exceptions::ArgumentError>())?;
    m.add("RedisError", py.get_type::<exceptions::RedisError>())?;
    m.add("PoolEmpty", py.get_type::<exceptions::PoolEmpty>())?;
    m.add("PubSubClosed", py.get_type::<exceptions::PubSubClosed>())?;

    Ok(())
}
