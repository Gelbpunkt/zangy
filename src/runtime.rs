use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());
