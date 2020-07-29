use crate::asyncio::{get_loop, set_fut_exc, set_fut_result};
use crate::exceptions::{ArgumentError, RedisError};
use async_std::task;
use pyo3::prelude::{pyclass, pymethods, PyObject, PyResult, Python, ToPyObject};
use pyo3::types::{PyBytes, PyTuple};
use redis::aio::MultiplexedConnection as ReConnection;
use redis::Value;

#[pyclass]
pub struct Connection {
    pub __connection: ReConnection,
}

fn re_to_object(v: &Value, py: Python) -> PyObject {
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

#[pymethods]
impl Connection {
    #[args(args = "*")]
    fn execute(&self, args: &PyTuple) -> PyResult<PyObject> {
        if args.len() == 0 {
            return Err(ArgumentError::py_err("no arguments provided to execute"));
        }

        // TODO: Only strings possible as arguments right now
        let new_args: Vec<String> = args
            .as_slice()
            .iter()
            .map(|i| {
                println!("{:?}", i.get_type().name());
                i.to_string()
            })
            .collect();

        let mut redis_cmd = redis::Cmd::new();
        redis_cmd.arg(new_args);

        let (fut, res_fut, loop_): (PyObject, PyObject, PyObject) = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let loop_ = get_loop(py)?;
            let fut: PyObject = loop_.call_method0(py, "create_future")?.into();
            (fut.clone_ref(py), fut, loop_.into())
        };
        let mut conn = self.__connection.clone();

        task::spawn(async move {
            match redis_cmd
                .query_async::<ReConnection, Value>(&mut conn)
                .await
            {
                Ok(v) => {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    if let Err(e) = set_fut_result(loop_, fut, re_to_object(&v, py)) {
                        eprintln!("{:?}", e);
                    };
                }
                Err(e) => {
                    let desc = e.detail().unwrap_or_default().to_string();
                    if let Err(e2) = set_fut_exc(loop_, fut, RedisError::py_err(desc)) {
                        eprintln!("{:?}", e2);
                    }
                }
            }
        });

        Ok(res_fut)
    }
}
