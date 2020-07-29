use pyo3::prelude::{PyObject, Python, ToPyObject};
use pyo3::types::{PyAny, PyBool, PyBytes, PyInt, PyList};
use redis::{ToRedisArgs, Value};
use std::io::{Error, ErrorKind};

#[derive(Debug)]
pub enum RedisValuePy {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Array(Vec<RedisValuePy>),
}

impl ToRedisArgs for RedisValuePy {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            RedisValuePy::String(s) => s.write_redis_args(out),
            RedisValuePy::Int(i) => i.write_redis_args(out),
            RedisValuePy::Float(f) => f.write_redis_args(out),
            RedisValuePy::Bool(b) => b.write_redis_args(out),
            RedisValuePy::Array(a) => a.write_redis_args(out),
        }
    }
}

pub fn object_to_re(arg: &PyAny) -> Result<RedisValuePy, Error> {
    let name = arg.get_type().name().to_string();
    let name_str = name.as_str();
    if name_str == "str" {
        Ok(RedisValuePy::String(arg.to_string()))
    } else if name_str == "int" {
        let actual: &PyInt = arg.downcast().unwrap();
        Ok(RedisValuePy::Int(actual.extract::<i64>().unwrap()))
    } else if name_str == "float" {
        let actual: &PyInt = arg.downcast().unwrap();
        Ok(RedisValuePy::Float(actual.extract::<f64>().unwrap()))
    } else if name_str == "bool" {
        let actual: &PyBool = arg.downcast().unwrap();
        Ok(RedisValuePy::Bool(actual.extract::<bool>().unwrap()))
    } else if name_str == "list" {
        let actual: &PyList = arg.downcast().unwrap();
        let res = actual.iter().map(|i| object_to_re(i).unwrap()).collect();
        Ok(RedisValuePy::Array(res))
    } else {
        Err(Error::new(ErrorKind::Other, "unsupported type"))
    }
}

pub fn re_to_object(v: &Value, py: Python) -> PyObject {
    match v {
        redis::Value::Nil => py.None(),
        redis::Value::Int(i) => i.to_object(py),
        redis::Value::Data(d) => PyBytes::new(py, &d).to_object(py),
        redis::Value::Bulk(b) => b
            .iter()
            .map(|i| re_to_object(i, py))
            .collect::<Vec<PyObject>>()
            .to_object(py),
        redis::Value::Status(s) => s.to_object(py),
        redis::Value::Okay => true.to_object(py),
    }
}
