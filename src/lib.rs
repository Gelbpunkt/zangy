use async_std::task;
use bytes::BytesMut;
use nom::error::{VerboseError, VerboseErrorKind};
use nom::Err as nom_err;
use pyo3::class::sequence::PySequenceProtocol;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString};

mod net;
mod parser;

create_exception!(zangy, ProtocolError, pyo3::exceptions::Exception);
create_exception!(zangy, RedisError, pyo3::exceptions::Exception);
create_exception!(zangy, ConnectionError, pyo3::exceptions::Exception);

#[pyclass]
struct Reader {
    __buffer: BytesMut,
}

impl ToPyObject for parser::RedisType {
    fn to_object(&self, py: Python) -> PyObject {
        match self {
            parser::RedisType::SimpleString(s) => s.to_object(py),
            parser::RedisType::Error(s) => RedisError::py_err(s.clone()).to_object(py),
            parser::RedisType::Integer(i) => i.to_object(py),
            parser::RedisType::BulkString(s) => s.to_object(py),
            parser::RedisType::Array(a) => PyList::new(py, a).to_object(py),
            parser::RedisType::NullArray => py.None(),
            parser::RedisType::NullBulkString => py.None(),
        }
    }
}

fn format_verbose_error(e: nom_err<VerboseError<&[u8]>>) -> String {
    let mut errors = Vec::new();
    match e {
        nom_err::Error(e) | nom_err::Failure(e) => {
            for err in e.errors {
                let f = match err {
                    (_, VerboseErrorKind::Context(s)) => format!("Context: {}", s),
                    (_, VerboseErrorKind::Char(c)) => format!("Char: {}", c),
                    (_, VerboseErrorKind::Nom(n)) => format!("Nom: {:?}", n),
                };
                errors.push(f);
            }
        }
        _ => errors.push("incomplete".to_string()),
    };
    errors.join("\n")
}

#[pymethods]
impl Reader {
    #[new]
    fn new() -> Self {
        Reader {
            __buffer: BytesMut::with_capacity(16384),
        }
    }

    fn feed(&mut self, data: &PyBytes) -> PyResult<()> {
        self.__buffer.extend_from_slice(data.as_bytes());
        Ok(())
    }

    fn get_buffer(&self) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        Ok(PyBytes::new(py, &self.__buffer).to_object(py))
    }

    fn gets(&mut self) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        if self.__buffer.len() == 0 {
            return Ok(false.to_object(py));
        }
        let buf = self.__buffer.clone();
        let result = parser::parse::<VerboseError<&[u8]>>(&buf);
        match result {
            Ok(v) => {
                self.__buffer = BytesMut::from(v.0);
                match v.1 {
                    parser::RedisType::Error(t) => Err(RedisError::py_err(t)),
                    _ => Ok(v.1.to_object(py)),
                }
            }
            Err(e) => Err(ProtocolError::py_err(format_verbose_error(e))),
        }
    }
}

#[pyclass]
struct Connection {
    address: String,
    __connection: Option<net::RedisConnection>,
}

fn get_loop(py: Python) -> PyResult<PyObject> {
    let asyncio = PyModule::import(py, "asyncio")?;
    let loop_ = asyncio.call0("get_running_loop")?;

    Ok(loop_.into())
}

fn set_fut_result(loop_: PyObject, fut: PyObject, res: PyObject) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_result")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, res))?;

    Ok(())
}

fn set_fut_exc(loop_: PyObject, fut: PyObject, exc: PyErr) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_exception")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, exc))?;

    Ok(())
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

#[pyproto]
impl PySequenceProtocol for Reader {
    fn __len__(&self) -> PyResult<usize> {
        Ok(self.__buffer.len())
    }
}

#[pymodule]
fn zangy(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;
    m.add_class::<Connection>()?;
    m.add("RedisError", py.get_type::<RedisError>())?;
    m.add("ProtocolError", py.get_type::<ProtocolError>())?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;

    Ok(())
}
