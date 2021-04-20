use pyo3::prelude::{PyErr, PyModule, PyObject, PyResult, Python};

pub fn get_loop(py: Python) -> PyResult<PyObject> {
    let asyncio = PyModule::import(py, "asyncio")?;
    let loop_ = asyncio.getattr("get_running_loop")?.call0()?;

    Ok(loop_.into())
}

pub fn create_future() -> PyResult<(PyObject, PyObject, PyObject)> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let loop_ = get_loop(py)?;
    let fut: PyObject = loop_.call_method0(py, "create_future")?;
    Ok((fut.clone_ref(py), fut, loop_))
}

pub fn set_fut_result(loop_: PyObject, fut: PyObject, res: PyObject) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_result")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, res))?;

    Ok(())
}

pub fn set_fut_result_none(loop_: PyObject, fut: PyObject) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_result")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, py.None()))?;

    Ok(())
}

pub fn set_fut_exc(loop_: PyObject, fut: PyObject, exc: PyErr) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_exception")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, exc))?;

    Ok(())
}
