use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub struct Metrics {
    pub replayed_wal_records: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let this = Metrics {
            replayed_wal_records: AtomicU64::new(0),
        };
        Arc::new(this)
    }
}
