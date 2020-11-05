use pyo3::prelude::{pymodule, PyModule, PyResult, Python};

mod asyncio;
mod conversion;
mod exceptions;
mod pool;

#[pymodule]
fn zangy(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<pool::ConnectionPool>()?;
    m.add(
        "ConnectionError",
        py.get_type::<exceptions::ConnectionError>(),
    )?;
    m.add("ArgumentError", py.get_type::<exceptions::ArgumentError>())?;
    m.add("RedisError", py.get_type::<exceptions::RedisError>())?;

    Ok(())
}
