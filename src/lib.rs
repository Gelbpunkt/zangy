use bytes::{Bytes, BytesMut};
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyInt, PyList, PyString};

mod resp;

create_exception!(zangy, ProtocolError, pyo3::exceptions::Exception);
create_exception!(zangy, RedisError, pyo3::exceptions::Exception);

#[pyclass(unsendable)]
struct Reader {
    __reader: resp::RespParser,
    __buffer: BytesMut,
}

fn convert_to_usable(py: Python, thing: resp::RedisValueRef) -> PyResult<PyObject> {
    match thing {
        resp::RedisValueRef::Error(_e) => return Err(RedisError::py_err("")),
        resp::RedisValueRef::ErrorMsg(e) => return Err(RedisError::py_err(e)),
        resp::RedisValueRef::SimpleString(s) => {
            return Ok(String::from_utf8((&s as &[u8]).to_vec())
                .unwrap()
                .to_object(py))
        }
        resp::RedisValueRef::BulkString(s) => {
            return Ok(String::from_utf8((&s as &[u8]).to_vec())
                .unwrap()
                .to_object(py))
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
        resp::RedisValueRef::NullArray => Ok(PyList::empty(py).to_object(py)),
        resp::RedisValueRef::NullBulkString => Ok("".to_object(py)),
    }
}

#[pymethods]
impl Reader {
    #[new]
    fn new() -> Self {
        Reader {
            __reader: resp::RespParser::default(),
            __buffer: BytesMut::new(),
        }
    }

    fn feed(&mut self, data: &PyBytes) -> PyResult<()> {
        self.__buffer.extend_from_slice(data.as_bytes());
        Ok(())
    }

    fn gets(&mut self) -> PyResult<PyObject> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let result = self.__reader.decode(&mut self.__buffer);
        if !result.is_ok() {
            return Err(ProtocolError::py_err("parser error"));
        }
        let fin = result.unwrap();
        match fin {
            Some(x) => convert_to_usable(py, x),
            None => Ok(py.None()),
        }
    }
}

#[pymodule]
fn zangy(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;

    Ok(())
}
