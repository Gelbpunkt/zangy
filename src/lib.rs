use bytes::BytesMut;
use pyo3::class::sequence::PySequenceProtocol;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::mem;

mod resp;

create_exception!(zangy, RedisError, pyo3::exceptions::Exception);

#[pyclass]
struct Reader {
    __buffer: BytesMut,
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
        // Special case
        if self.__buffer.len() == 0 {
            return Ok(false.to_object(py));
        }
        let old_buffer = mem::replace(&mut self.__buffer, BytesMut::with_capacity(16384));
        let result = resp::parse_py(old_buffer.freeze(), py);
        result
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
    m.add("ProtocolError", py.get_type::<resp::ProtocolError>())?;

    Ok(())
}
