use crate::parser;
use bytes::BytesMut;
use pyo3::class::sequence::PySequenceProtocol;
use pyo3::create_exception;
use pyo3::prelude::{pyclass, pymethods, pyproto, PyObject, PyResult, Python, ToPyObject};
use pyo3::types::{PyBytes, PyList};

create_exception!(zangy, ProtocolError, pyo3::exceptions::Exception);
create_exception!(zangy, RedisError, pyo3::exceptions::Exception);

#[pyclass]
pub struct Reader {
    __buffer: BytesMut,
}

#[pyproto]
impl PySequenceProtocol for Reader {
    fn __len__(&self) -> PyResult<usize> {
        Ok(self.__buffer.len())
    }
}

impl ToPyObject for parser::ParserError {
    fn to_object(&self, py: Python) -> PyObject {
        match self {
            parser::ParserError::UnknownStartingByte => "unknown starting byte".to_object(py),
            parser::ParserError::Eof => "end of file".to_object(py),
        }
    }
}

impl ToPyObject for parser::RedisType {
    fn to_object(&self, py: Python) -> PyObject {
        match self {
            parser::RedisType::SimpleString(s) => PyBytes::new(py, &s).to_object(py),
            parser::RedisType::Error(s) => RedisError::py_err(s.clone()).to_object(py),
            parser::RedisType::Integer(i) => i.to_object(py),
            parser::RedisType::BulkString(s) => PyBytes::new(py, &s).to_object(py),
            parser::RedisType::Array(a) => PyList::new(py, a).to_object(py),
            parser::RedisType::NullArray => py.None(),
            parser::RedisType::NullBulkString => py.None(),
        }
    }
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
        let result = parser::parse(&mut self.__buffer);
        match result {
            Ok(v) => match v {
                parser::RedisType::Error(t) => Err(RedisError::py_err(t)),
                _ => Ok(v.to_object(py)),
            },
            Err(e) => Err(ProtocolError::py_err(e)),
        }
    }
}
