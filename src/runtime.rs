use std::sync::LazyLock;

use tokio::runtime::Runtime;

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());
