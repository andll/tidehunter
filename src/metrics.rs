use prometheus::{
    exponential_buckets, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Registry,
};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub struct Metrics {
    pub replayed_wal_records: IntCounter,
    pub index_size: Histogram,
    pub max_index_size: AtomicUsize,
    pub max_index_size_metric: IntGauge,
    pub wal_written_bytes: IntGauge,
    pub wal_written_bytes_type: IntCounterVec,
    pub unload: IntCounterVec,
    pub entry_state: IntGaugeVec,
    pub compacted_keys: IntCounterVec,
    pub read: IntCounterVec,
    pub read_bytes: IntCounterVec,
    pub loaded_keys: IntGaugeVec,

    pub lookup_mcs: HistogramVec,
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
macro_rules! counter_vec (
    ($name:expr, $b:expr, $r:expr) => {prometheus::register_int_counter_vec_with_registry!($name, $name, $b, $r).unwrap()};
);
#[macro_export]
macro_rules! gauge_vec (
    ($name:expr, $b:expr, $r:expr) => {prometheus::register_int_gauge_vec_with_registry!($name, $name, $b, $r).unwrap()};
);
#[macro_export]
macro_rules! histogram (
    ($name:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_with_registry!($name, $name, $buck.unwrap(), $r).unwrap()}
);
#[macro_export]
macro_rules! histogram_vec (
    ($name:expr, $labels:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_vec_with_registry!($name, $name, $labels, $buck.unwrap(), $r).unwrap()};
);
impl Metrics {
    pub fn new() -> Arc<Self> {
        Self::new_in(&Registry::default())
    }

    pub fn new_in(registry: &Registry) -> Arc<Self> {
        let index_size_buckets = exponential_buckets(100., 2., 20);
        let lookup_buckets = exponential_buckets(10., 2., 12);
        let this = Metrics {
            replayed_wal_records: counter!("replayed_wal_records", registry),
            max_index_size: AtomicUsize::new(0),
            index_size: histogram!("index_size", index_size_buckets, registry),
            max_index_size_metric: gauge!("max_index_size", registry),
            wal_written_bytes: gauge!("wal_written_bytes", registry),
            wal_written_bytes_type: counter_vec!("wal_written_bytes_type", &["type"], registry),
            unload: counter_vec!("unload", &["kind"], registry),
            entry_state: gauge_vec!("entry_state", &["state"], registry),
            compacted_keys: counter_vec!("compacted_keys", &["ks"], registry),
            read: counter_vec!("read", &["ks", "kind", "type"], registry),
            read_bytes: counter_vec!("read_bytes", &["ks", "kind", "type"], registry),
            loaded_keys: gauge_vec!("loaded_keys", &["ks"], registry),

            lookup_mcs: histogram_vec!("lookup_mcs", &["type", "ks"], lookup_buckets, registry),
            // loaded_keys_total_bytes: gauge!("loaded_keys_total_bytes", registry),
        };
        Arc::new(this)
    }
}
