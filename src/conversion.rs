use pyo3::{
    prelude::{FromPyObject, PyObject, Python, ToPyObject},
    types::PyBytes,
};
use redis::{RedisWrite, ToRedisArgs, Value};

#[derive(Debug, FromPyObject)]
pub enum RedisValuePy {
    Bool(bool),
    Bytes(Vec<u8>),
    String(String),
    Int(i64),
    Float(f64),
    Array(Vec<RedisValuePy>),
}

impl ToRedisArgs for RedisValuePy {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            RedisValuePy::Bytes(b) => b.write_redis_args(out),
            RedisValuePy::String(s) => s.write_redis_args(out),
            RedisValuePy::Int(i) => i.write_redis_args(out),
            RedisValuePy::Float(f) => f.write_redis_args(out),
            RedisValuePy::Bool(b) => b.write_redis_args(out),
            RedisValuePy::Array(a) => a.write_redis_args(out),
        }
    }
}

pub fn re_to_object(v: &Value, py: Python) -> PyObject {
    match v {
        Value::Nil => py.None(),
        Value::Int(i) => i.to_object(py),
        Value::Data(d) => PyBytes::new(py, d).to_object(py),
        Value::Bulk(b) => b
            .iter()
            .map(|i| re_to_object(i, py))
            .collect::<Vec<PyObject>>()
            .to_object(py),
        Value::Status(s) => s.to_object(py),
        Value::Okay => true.to_object(py),
    }
}
