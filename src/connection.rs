use crate::asyncio::{get_loop, set_fut_exc, set_fut_result};
use crate::net;
use async_std::task;
use pyo3::create_exception;
use pyo3::prelude::{pyclass, pymethods, AsPyRef, Py, PyObject, PyResult, Python};
use pyo3::types::PyString;

create_exception!(zangy, ConnectionError, pyo3::exceptions::Exception);

#[pyclass]
pub struct Connection {
    address: String,
    __connection: Option<net::RedisConnection>,
}

#[pymethods]
impl Connection {
    #[new]
    fn new(address: &PyString) -> Self {
        Connection {
            address: address.to_string().unwrap().to_string(),
            __connection: None,
        }
    }

    fn connect(slf: Py<Self>) -> PyResult<PyObject> {
        let (fut, res_fut, loop_, addr): (PyObject, PyObject, PyObject, String) = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let loop_ = get_loop(py)?;
            let fut: PyObject = loop_.call_method0(py, "create_future")?.into();
            let addr = slf.as_ref(py).try_borrow().unwrap().address.clone();
            (fut.clone_ref(py), fut, loop_.into(), addr)
        };

        task::spawn(async move {
            let res = net::RedisConnection::from_address(&addr).await;

            match res {
                Ok(conn) => {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    let mut actual_new = slf.as_ref(py).try_borrow_mut().unwrap();
                    actual_new.__connection = Some(conn);
                }
                Err(_e) => {
                    if let Err(e) = set_fut_exc(
                        loop_,
                        fut,
                        ConnectionError::py_err("error when connecting to redis"),
                    ) {
                        println!("{:?}", e);
                    }
                    return;
                }
            }
            let gil = Python::acquire_gil();
            let py = gil.python();
            if let Err(e) = set_fut_result(loop_, fut, py.None()) {
                e.print(py);
            }
        });

        Ok(res_fut)
    }
}
