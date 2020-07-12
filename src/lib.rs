use hiredis_sys::*;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass(unsendable)]
struct Reader {
    __reader: *mut redisReader,
}

#[pymethods]
impl Reader {
    #[new]
    fn new() -> Self {
        Reader {
            __reader: unsafe { redisReaderCreate() },
        }
    }

    fn feed(&self, _data: &PyBytes) -> PyResult<()> {
        let actual_bytes = _data.as_bytes();
        unsafe {
            redisReaderFeed(self.__reader, actual_bytes, actual_bytes.len());
        }
        Ok(())
    }

    fn gets(&self) -> PyResult<()> {
        Ok(())
    }
}

#[pymodule]
fn zangy(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Reader>()?;

    Ok(())
}
