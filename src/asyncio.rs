use pyo3::prelude::{PyErr, PyModule, PyObject, PyResult, Python};

pub fn get_loop(py: Python) -> PyResult<PyObject> {
    let asyncio = PyModule::import(py, "asyncio")?;
    let loop_ = asyncio.call0("get_running_loop")?;

    Ok(loop_.into())
}

pub fn set_fut_result(loop_: PyObject, fut: PyObject, res: PyObject) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_result")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, res))?;

    Ok(())
}

pub fn set_fut_exc(loop_: PyObject, fut: PyObject, exc: PyErr) -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let sr = fut.getattr(py, "set_exception")?;

    loop_.call_method1(py, "call_soon_threadsafe", (sr, exc))?;

    Ok(())
}
