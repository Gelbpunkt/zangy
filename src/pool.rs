use crate::asyncio::{get_loop, set_fut_exc, set_fut_result};
use crate::connection::PooledConnection;
use crate::exceptions::ConnectionError;
use async_std::task;
use async_trait::async_trait;
use bb8::{ManageConnection, Pool};
use pyo3::prelude::{pyclass, pymethods, IntoPy, PyObject, PyResult, Python};
use redis::aio::MultiplexedConnection;
use redis::{Client, IntoConnectionInfo, RedisError};

/// A `bb8::ManageConnection` for `redis::Client::get_async_connection`.
#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: Client,
}

impl RedisConnectionManager {
    /// Create a new `RedisConnectionManager`.
    /// See `redis::Client::open` for a description of the parameter types.
    pub fn new<T: IntoConnectionInfo>(info: T) -> Result<RedisConnectionManager, RedisError> {
        Ok(RedisConnectionManager {
            client: Client::open(info.into_connection_info()?)?,
        })
    }
}

#[async_trait]
impl ManageConnection for RedisConnectionManager {
    type Connection = MultiplexedConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_multiplexed_async_std_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        redis::cmd("PING").query_async(conn).await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[pyclass]
pub struct ConnectionPoolManager {
    __manager: RedisConnectionManager,
}

#[pyclass]
pub struct ConnectionPool {
    __pool: &'static Pool<RedisConnectionManager>,
}

#[pymethods]
impl ConnectionPoolManager {
    #[new]
    fn new(address: String) -> PyResult<Self> {
        match RedisConnectionManager::new(address) {
            Ok(conn) => Ok(ConnectionPoolManager { __manager: conn }),
            Err(_) => Err(ConnectionError::py_err("error when connecting to redis")),
        }
    }

    fn connect(&self) -> PyResult<PyObject> {
        let (fut, res_fut, loop_): (PyObject, PyObject, PyObject) = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let loop_ = get_loop(py)?;
            let fut: PyObject = loop_.call_method0(py, "create_future")?.into();
            (fut.clone_ref(py), fut, loop_.into())
        };
        let manager = self.__manager.clone();

        task::spawn(async move {
            match Pool::builder().build(manager).await {
                Ok(p) => {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    let inst: PyObject = ConnectionPool { __pool: &p }.into_py(py);

                    if let Err(e) = set_fut_result(loop_, fut, inst) {
                        e.print(py);
                    };
                }
                Err(_) => {
                    if let Err(e) = set_fut_exc(
                        loop_,
                        fut,
                        ConnectionError::py_err("error when acquiring connection"),
                    ) {
                        eprintln!("{:?}", e);
                    }
                }
            }
        });

        Ok(res_fut)
    }
}

#[pymethods]
impl ConnectionPool {
    fn connect(&self) -> PyResult<PyObject> {
        let (fut, res_fut, loop_): (PyObject, PyObject, PyObject) = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let loop_ = get_loop(py)?;
            let fut: PyObject = loop_.call_method0(py, "create_future")?.into();
            (fut.clone_ref(py), fut, loop_.into())
        };
        let pool = self.__pool.clone();

        task::spawn(async move {
            match pool.get().await {
                Ok(c) => {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    let inst: PyObject = PooledConnection { __connection: c }.into_py(py);

                    if let Err(e) = set_fut_result(loop_, fut, inst) {
                        e.print(py);
                    };
                }
                Err(_) => {
                    if let Err(e) = set_fut_exc(
                        loop_,
                        fut,
                        ConnectionError::py_err("error when acquiring connection"),
                    ) {
                        eprintln!("{:?}", e);
                    }
                }
            }
        });

        Ok(res_fut)
    }
}
