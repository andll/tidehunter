use prometheus::{exponential_buckets, Histogram, IntCounter, IntGauge, Registry};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub struct Metrics {
    pub replayed_wal_records: IntCounter,
    pub index_size: Histogram,
    pub max_index_size: AtomicUsize,
    pub max_index_size_metric: IntGauge,
    pub max_index_size_cell: IntGauge,
    pub wal_written_bytes: IntGauge,
    pub unload_unmerge: IntCounter,
    pub unload_flush: IntCounter,
    pub unload_dirty_unloaded: IntCounter,
    pub unload_clean: IntCounter,
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

#[macro_export]
macro_rules! histogram (
    ($name:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_with_registry!($name, $name, $buck.unwrap(), $r).unwrap()}
);

impl Metrics {
    pub fn new() -> Arc<Self> {
        Self::new_in(&Registry::default())
    }

    pub fn new_in(registry: &Registry) -> Arc<Self> {
        let index_size_buckets = exponential_buckets(100., 2., 20);
        let this = Metrics {
            replayed_wal_records: counter!("replayed_wal_records", registry),
            max_index_size: AtomicUsize::new(0),
            index_size: histogram!("index_size", index_size_buckets, registry),
            max_index_size_metric: gauge!("max_index_size", registry),
            max_index_size_cell: gauge!("max_index_size_cell", registry),
            wal_written_bytes: gauge!("wal_written_bytes", registry),
            unload_unmerge: counter!("unload_unmerge", registry),
            unload_flush: counter!("unload_flush", registry),
            unload_dirty_unloaded: counter!("unload_dirty_unloaded", registry),
            unload_clean: counter!("unload_clean", registry),
            // loaded_keys_total: gauge!("loaded_keys_total", registry),
            // loaded_keys_total_bytes: gauge!("loaded_keys_total_bytes", registry),
        };
        Arc::new(this)
    }
}
