use pyo3::{create_exception, exceptions::Exception};

create_exception!(zangy, ConnectionError, Exception);
create_exception!(zangy, ArgumentError, Exception);
create_exception!(zangy, RedisError, Exception);
