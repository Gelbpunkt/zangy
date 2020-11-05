use crate::asyncio::{create_future, set_fut_exc, set_fut_result};
use crate::connection::Connection;
use crate::exceptions::ConnectionError;
use async_std::task;
use deadpool_redis::{Config, Pool};
use pyo3::prelude::{pyclass, pymethods, IntoPy, PyObject, PyResult, Python};

#[pyclass]
pub struct ConnectionPool {
    __pool: Pool,
}

#[pymethods]
impl ConnectionPool {
    #[new]
    fn new(address: String) -> PyResult<Self> {
        let mut cfg = Config::default();
        cfg.url = Some(address);
        match cfg.create_pool() {
            Ok(pool) => Ok(Self { __pool: pool }),
            Err(_) => Err(ConnectionError::new_err("error when connecting to redis")),
        }
    }

    fn get(&self) -> PyResult<PyObject> {
        let (fut, res_fut, loop_) = create_future()?;
        let pool = self.__pool.clone();

        task::spawn(async move {
            match pool.get().await {
                Ok(c) => {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    let inst: PyObject = Connection { __connection: c }.into_py(py);

                    if let Err(e) = set_fut_result(loop_, fut, inst) {
                        e.print(py);
                    };
                }
                Err(_) => {
                    if let Err(e) = set_fut_exc(
                        loop_,
                        fut,
                        ConnectionError::new_err("error when acquiring connection"),
                    ) {
                        eprintln!("{:?}", e);
                    }
                }
            }
        });

        Ok(res_fut)
    }
}
