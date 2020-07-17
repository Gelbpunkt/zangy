use bytes::BytesMut;
use nom::error::{VerboseError, VerboseErrorKind};
use nom::Err as nom_err;
use pyo3::class::sequence::PySequenceProtocol;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

mod net;
mod parser;

create_exception!(zangy, ProtocolError, pyo3::exceptions::Exception);
create_exception!(zangy, RedisError, pyo3::exceptions::Exception);

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
        let result = parser::parse::<VerboseError<&[u8]>>(&self.__buffer);
        match result {
            Ok(v) => Ok(v.to_object(py)),
            Err(e) => Err(ProtocolError::py_err(format_verbose_error(e))),
        }
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
    m.add("RedisError", py.get_type::<RedisError>())?;
    m.add("ProtocolError", py.get_type::<ProtocolError>())?;

    Ok(())
}
