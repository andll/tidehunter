use prometheus::{IntCounter, IntGauge, Registry};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

pub struct Metrics {
    pub replayed_wal_records: IntCounter,
    pub max_index_size: AtomicUsize,
    pub max_index_size_metric: IntGauge,
    // pub loaded_keys_total: IntGauge,
    // pub loaded_keys_total_bytes: IntGauge,
}

#[macro_export]
macro_rules! gauge (
    ($name:expr, $r:expr) => {prometheus::register_int_gauge_with_registry!($name, $name, $r).unwrap()};
);
#[macro_export]
macro_rules! counter (
    ($name:expr, $r:expr) => {prometheus::register_int_counter_with_registry!($name, $name, $r).unwrap()};
);

impl Metrics {
    pub fn new() -> Arc<Self> {
        Self::new_in(&Registry::default())
    }

    pub fn new_in(registry: &Registry) -> Arc<Self> {
        let this = Metrics {
            replayed_wal_records: counter!("replayed_wal_records", registry),
            max_index_size: AtomicUsize::new(0),
            max_index_size_metric: gauge!("max_index_size_metric", registry),
            // loaded_keys_total: gauge!("loaded_keys_total", registry),
            // loaded_keys_total_bytes: gauge!("loaded_keys_total_bytes", registry),
        };
        Arc::new(this)
    }
}
