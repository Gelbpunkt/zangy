use std::lazy::SyncLazy;
use tokio::runtime::Runtime;

pub static RUNTIME: SyncLazy<Runtime> = SyncLazy::new(|| Runtime::new().unwrap());
