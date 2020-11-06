use async_std::task;
use pyo3::{
    prelude::{pyfunction, pymodule, IntoPy, PyModule, PyObject, PyResult, Python},
    wrap_pyfunction,
};
use redis::Client;
use std::sync::atomic::AtomicUsize;

mod asyncio;
mod conversion;
mod exceptions;
mod pool;

/// Connect to a redis server at `address` and use up to `pool_size` connections.
#[pyfunction]
#[text_signature = "(address, pool_size)"]
fn create_pool(address: String, pool_size: u16) -> PyResult<PyObject> {
    let (fut, res_fut, loop_) = asyncio::create_future()?;

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
                            let _ = asyncio::set_fut_exc(
                                loop_,
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
                    pool_size: pool_size as usize,
                };
                let gil = Python::acquire_gil();
                let py = gil.python();
                let _ = asyncio::set_fut_result(loop_, fut, pool.into_py(py));
            }
            Err(e) => {
                let _ = asyncio::set_fut_exc(
                    loop_,
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

    Ok(())
}
