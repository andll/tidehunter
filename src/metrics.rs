use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

pub struct Metrics {
    pub replayed_wal_records: AtomicU64,
    pub max_index_size: AtomicUsize,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let this = Metrics {
            replayed_wal_records: AtomicU64::new(0),
            max_index_size: AtomicUsize::new(0),
        };
        Arc::new(this)
    }
}
