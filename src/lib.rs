use pyo3::prelude::{pymodule, PyModule, PyResult, Python};

mod asyncio;
mod connection;
mod net;
mod parser;
mod reader;

#[pymodule]
fn zangy(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<reader::Reader>()?;
    m.add_class::<connection::Connection>()?;
    m.add("RedisError", py.get_type::<reader::RedisError>())?;
    m.add("ProtocolError", py.get_type::<reader::ProtocolError>())?;
    m.add(
        "ConnectionError",
        py.get_type::<connection::ConnectionError>(),
    )?;

    Ok(())
}
