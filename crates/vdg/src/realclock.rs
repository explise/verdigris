//! The production `Clock`. Lives in the shell (not core) so the core stays free
//! of any real time source. Under DST this is replaced by `SimClock`.

use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH};
use verdigris_core::clock::{Clock, Millis};

pub struct RealClock;

#[async_trait]
impl Clock for RealClock {
    fn now_millis(&self) -> Millis {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis() as Millis
    }

    async fn sleep(&self, ms: Millis) {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}
