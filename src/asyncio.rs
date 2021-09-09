use pyo3::prelude::{PyErr, PyObject, PyResult, Python};
use std::lazy::SyncLazy;

pub static EVENT_LOOP: SyncLazy<PyObject> = SyncLazy::new(|| {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let asyncio = py.import("asyncio").unwrap();
    let loop_ = asyncio.getattr("get_event_loop").unwrap().call0().unwrap();
    loop_.into()
});

pub fn create_future() -> PyResult<(PyObject, PyObject)> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let fut: PyObject = EVENT_LOOP.call_method0(py, "create_future")?;
    Ok((fut.clone_ref(py), fut))
}

pub fn set_fut_result(fut: &PyObject, res: PyObject) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_result")?;

    EVENT_LOOP.call_method1(py, "call_soon_threadsafe", (sr, res))?;

    Ok(())
}

pub fn set_fut_result_none(fut: &PyObject) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_result")?;

    EVENT_LOOP.call_method1(py, "call_soon_threadsafe", (sr, py.None()))?;

    Ok(())
}

pub fn set_fut_exc(fut: &PyObject, exc: PyErr) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_exception")?;

    EVENT_LOOP.call_method1(py, "call_soon_threadsafe", (sr, exc))?;

    Ok(())
}
