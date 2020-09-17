use crate::asyncio::{get_loop, set_fut_exc, set_fut_result};
use crate::conversion::{object_to_re, re_to_object, RedisValuePy};
use crate::exceptions::{ArgumentError, RedisError};
use crate::pool::RedisConnectionManager;
use bb8::PooledConnection as Bb8PooledConnection;
use pyo3::prelude::{pyclass, pymethods, PyObject, PyResult, Python};
use pyo3::types::PyTuple;
use redis::aio::MultiplexedConnection as ReConnection;
use redis::Value;
use tokio::task;

#[pyclass]
pub struct Connection {
    __connection: Bb8PooledConnection<'static, RedisConnectionManager>,
}

#[pymethods]
impl Connection {
    #[args(args = "*")]
    fn execute(&self, args: &PyTuple) -> PyResult<PyObject> {
        if args.len() == 0 {
            return Err(ArgumentError::new_err("no arguments provided to execute"));
        }

        let new_args: Vec<RedisValuePy> = args
            .as_slice()
            .iter()
            .map(|i| object_to_re(i).unwrap())
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
                    if let Err(e2) = set_fut_exc(loop_, fut, RedisError::new_err(desc)) {
                        eprintln!("{:?}", e2);
                    }
                }
            }
        });

        Ok(res_fut)
    }
}
