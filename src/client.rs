use crate::asyncio::{get_loop, set_fut_exc, set_fut_result};
use crate::connection::Connection;
use crate::exceptions::ConnectionError;
use async_std::task;
use pyo3::prelude::{pyclass, pymethods, IntoPy, PyObject, PyResult, Python};
use pyo3::types::PyString;
use redis::Client as ReClient;
use async_std::sync::{Arc, Mutex};

#[pyclass]
pub struct Client {
    __client: ReClient,
}

#[pymethods]
impl Client {
    #[new]
    fn new(address: &PyString) -> PyResult<Self> {
        match ReClient::open(address.to_string().unwrap().to_string()) {
            Ok(conn) => Ok(Client { __client: conn }),
            Err(_) => Err(ConnectionError::py_err("error when connecting to redis")),
        }
    }

    fn get_connection(&self) -> PyResult<PyObject> {
        let (fut, res_fut, loop_): (PyObject, PyObject, PyObject) = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let loop_ = get_loop(py)?;
            let fut: PyObject = loop_.call_method0(py, "create_future")?.into();
            (fut.clone_ref(py), fut, loop_.into())
        };
        let client = self.__client.clone();

        task::spawn(async move {
            match client.get_async_std_connection().await {
                Ok(c) => {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    let inst: PyObject = Connection {
                        __connection: Arc::new(Mutex::new(c)),
                    }
                    .into_py(py);

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
