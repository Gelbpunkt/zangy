use lazy_static::lazy_static;
use tokio::runtime::Runtime;

lazy_static! {
    pub static ref RUNTIME: Runtime = Runtime::new().unwrap();
}
