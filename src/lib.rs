#![feature(core_intrinsics)]
#![deny(clippy::pedantic)]
#![allow(
    clippy::used_underscore_binding,
    clippy::module_name_repetitions,
    clippy::doc_markdown,
    internal_features
)]
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

use pyo3::{
    prelude::{pyfunction, pymodule, IntoPy, PyModule, PyObject, PyResult, Python},
    types::PyModuleMethods,
    wrap_pyfunction, Bound,
};
use redis::Client;

mod asyncio;
mod conversion;
mod exceptions;
mod pool;
mod runtime;

/// Connect to a redis server at `address` and use up to `pool_size`
/// connections.
#[pyfunction]
#[pyo3(text_signature = "(address, pool_size, pubsub_size)")]
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
                            let _res = asyncio::set_fut_exc(
                                &fut,
                                exceptions::ConnectionError::new_err(e.to_string()),
                            );
                            return;
                        }
                    }
                }
                let mut pubsub_connections = Vec::with_capacity(pubsub_size as usize);
                for _ in 0..pubsub_size {
                    let pubsub = client.get_async_pubsub().await;

                    match pubsub {
                        Ok(conn) => pubsub_connections.push(conn),
                        Err(e) => {
                            let _res = asyncio::set_fut_exc(
                                &fut,
                                exceptions::ConnectionError::new_err(e.to_string()),
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

                let _res = Python::with_gil(|py| {
                    asyncio::set_fut_result_with_gil(&fut, pool.into_py(py), py)
                });
            }
            Err(e) => {
                let _res = asyncio::set_fut_exc(
                    &fut,
                    exceptions::ConnectionError::new_err(format!("{e}")),
                );
            }
        }
    });

    Ok(res_fut)
}

#[pymodule]
fn zangy(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_pool, m)?)?;
    m.add_class::<pool::ConnectionPool>()?;
    m.add(
        "ConnectionError",
        py.get_type_bound::<exceptions::ConnectionError>(),
    )?;
    m.add(
        "ArgumentError",
        py.get_type_bound::<exceptions::ArgumentError>(),
    )?;
    m.add("RedisError", py.get_type_bound::<exceptions::RedisError>())?;
    m.add("PoolEmpty", py.get_type_bound::<exceptions::PoolEmpty>())?;
    m.add(
        "PubSubClosed",
        py.get_type_bound::<exceptions::PubSubClosed>(),
    )?;

    Ok(())
}
