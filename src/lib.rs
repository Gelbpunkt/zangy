use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

#[pyfunction]
fn test() -> String {
    String::from("hello world")
}

#[pymodule]
fn zangy(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(test))?;

    Ok(())
}
