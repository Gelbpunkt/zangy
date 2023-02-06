use std::sync::LazyLock;

use pyo3::{
    intern,
    prelude::{PyErr, PyObject, PyResult, Python},
};

pub static EVENT_LOOP: LazyLock<PyObject> = LazyLock::new(|| {
    Python::with_gil(|py| {
        let asyncio = py.import("asyncio").unwrap();
        let loop_ = asyncio
            .getattr(intern!(py, "get_event_loop"))
            .unwrap()
            .call0()
            .unwrap();
        loop_.into()
    })
});

pub fn create_future() -> PyResult<(PyObject, PyObject)> {
    Python::with_gil(|py| {
        let fut: PyObject = EVENT_LOOP.call_method0(py, intern!(py, "create_future"))?;
        Ok((fut.clone_ref(py), fut))
    })
}

pub fn set_fut_result_with_gil(fut: &PyObject, res: PyObject, py: Python) -> PyResult<()> {
    let sr = fut.getattr(py, intern!(py, "set_result"))?;

    EVENT_LOOP.call_method1(py, intern!(py, "call_soon_threadsafe"), (sr, res))?;

    Ok(())
}

pub fn set_fut_result_none(fut: &PyObject) -> PyResult<()> {
    Python::with_gil(|py| {
        let sr = fut.getattr(py, intern!(py, "set_result"))?;

        EVENT_LOOP.call_method1(py, intern!(py, "call_soon_threadsafe"), (sr, py.None()))?;

        Ok(())
    })
}

pub fn set_fut_exc(fut: &PyObject, exc: PyErr) -> PyResult<()> {
    Python::with_gil(|py| {
        let sr = fut.getattr(py, intern!(py, "set_exception"))?;

        EVENT_LOOP.call_method1(py, intern!(py, "call_soon_threadsafe"), (sr, exc))?;

        Ok(())
    })
}
