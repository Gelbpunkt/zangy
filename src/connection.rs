use crate::asyncio::{get_loop, set_fut_exc, set_fut_result};
use crate::conversion::{re_to_object, RedisValuePy};
use crate::exceptions::{ArgumentError, RedisError};
use async_std::task;
use deadpool_redis::{redis::Cmd, Connection as ReConnection};
use pyo3::prelude::{pyclass, pymethods, PyObject, PyResult, Python};

#[pyclass]
pub struct Connection {
    pub __connection: ReConnection,
}

#[pymethods]
impl Connection {
    #[args(args = "*")]
    fn execute(&self, args: Vec<RedisValuePy>) -> PyResult<PyObject> {
        if args.len() == 0 {
            return Err(ArgumentError::new_err("no arguments provided to execute"));
        }

        let mut redis_cmd = Cmd::new();
        redis_cmd.arg(args);

        let (fut, res_fut, loop_): (PyObject, PyObject, PyObject) = {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let loop_ = get_loop(py)?;
            let fut: PyObject = loop_.call_method0(py, "create_future")?.into();
            (fut.clone_ref(py), fut, loop_.into())
        };
        let mut conn = self.__connection.clone();

        task::spawn(async move {
            match redis_cmd.query_async(&mut conn).await {
                Ok(v) => {
                    let gil = Python::acquire_gil();
                    let py = gil.python();
                    if let Err(e) = set_fut_result(loop_, fut, re_to_object(&v, py)) {
                        eprintln!("{:?}", e);
                    };
                }
                Err(e) => {
                    let desc = format!("{}", e);
                    if let Err(e2) = set_fut_exc(loop_, fut, RedisError::new_err(desc)) {
                        eprintln!("{:?}", e2);
                    }
                }
            }
        });

        Ok(res_fut)
    }
}
