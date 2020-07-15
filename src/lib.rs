use bytes::BytesMut;
use pyo3::class::sequence::PySequenceProtocol;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

mod resp;

create_exception!(zangy, ProtocolError, pyo3::exceptions::Exception);
create_exception!(zangy, RedisError, pyo3::exceptions::Exception);

#[pyclass]
struct Reader {
    __reader: resp::RespParser,
    __buffer: BytesMut,
}

fn convert_to_usable(py: Python, thing: resp::RedisValueRef) -> PyResult<PyObject> {
    match thing {
        resp::RedisValueRef::Error(e) => {
            return Err(RedisError::py_err(String::from_utf8(e.to_vec()).unwrap()))
        }
        resp::RedisValueRef::ErrorMsg(e) => {
            return Err(RedisError::py_err(String::from_utf8(e).unwrap()))
        }
        resp::RedisValueRef::SimpleString(s) => {
            return Ok(PyBytes::new(py, &s).to_object(py));
        }
        resp::RedisValueRef::BulkString(s) => {
            return Ok(PyBytes::new(py, &s).to_object(py));
        }
        resp::RedisValueRef::Array(arr) => {
            let mut fin = Vec::new();
            let mut arr = arr.clone();
            while arr.len() > 0 {
                let elem = arr.remove(0);
                fin.push(convert_to_usable(py, elem).unwrap())
            }
            return Ok(PyList::new(py, fin).to_object(py));
        }
        resp::RedisValueRef::Int(i) => return Ok(i.to_object(py)),
        // hiredis returns None on NullArray
        // resp::RedisValueRef::NullArray => Ok(PyList::empty(py).to_object(py)),
        resp::RedisValueRef::NullArray => Ok(py.None()),
        resp::RedisValueRef::NullBulkString => Ok("".to_object(py)),
    }
}

#[pymethods]
impl Reader {
    #[new]
    fn new() -> Self {
        Reader {
            __reader: resp::RespParser::default(),
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
        // Special case
        if self.__buffer.len() == 0 {
            return Ok(false.to_object(py));
        }
        let result = self.__reader.decode(&mut self.__buffer);
        match result {
            Err(x) => match x {
                resp::RESPError::UnexpectedEnd => return Ok(false.to_object(py)),
                resp::RESPError::UnknownStartingByte => {
                    return Err(ProtocolError::py_err("unknown starting byte"))
                }
                resp::RESPError::BadArraySize(x) => {
                    return Err(ProtocolError::py_err(format!("bad array size: {}", x)))
                }
                resp::RESPError::BadBulkStringSize(x) => {
                    return Err(ProtocolError::py_err(format!(
                        "bad bulk string size: {}",
                        x
                    )))
                }
                resp::RESPError::IntParseFailure => {
                    return Err(ProtocolError::py_err("integer parsing failed"))
                }
                resp::RESPError::IOError(_) => return Err(ProtocolError::py_err("io error")),
            },
            Ok(x) => match x {
                Some(x) => convert_to_usable(py, x),
                None => Ok(py.None()),
            },
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
