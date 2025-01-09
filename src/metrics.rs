use prometheus::{
    exponential_buckets, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Registry,
};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::time::Instant;

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

    pub large_table_contention: HistogramVec,
    pub wal_contention: Histogram,
    pub db_op_mcs: HistogramVec,
    pub map_time_mcs: Histogram,
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
    ($name:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_with_registry!($name, $name, $buck, $r).unwrap()}
);
#[macro_export]
macro_rules! histogram_vec (
    ($name:expr, $labels:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_vec_with_registry!($name, $name, $labels, $buck, $r).unwrap()};
);
impl Metrics {
    pub fn new() -> Arc<Self> {
        Self::new_in(&Registry::default())
    }

    pub fn new_in(registry: &Registry) -> Arc<Self> {
        let index_size_buckets = exponential_buckets(100., 2., 20).unwrap();
        let lookup_buckets = exponential_buckets(5., 1.5, 12).unwrap();
        let db_op_buckets = exponential_buckets(5., 1.5, 16).unwrap();
        let lock_buckets = exponential_buckets(1., 1.5, 12).unwrap();
        let this = Metrics {
            replayed_wal_records: counter!("replayed_wal_records", registry),
            max_index_size: AtomicUsize::new(0),
            index_size: histogram!("index_size", index_size_buckets, registry),
            max_index_size_metric: gauge!("max_index_size", registry),
            wal_written_bytes: gauge!("wal_written_bytes", registry),
            wal_written_bytes_type: counter_vec!("wal_written_bytes_type", &["type"], registry),
            unload: counter_vec!("unload", &["kind"], registry),
            entry_state: gauge_vec!("entry_state", &["ks", "state"], registry),
            compacted_keys: counter_vec!("compacted_keys", &["ks"], registry),
            read: counter_vec!("read", &["ks", "kind", "type"], registry),
            read_bytes: counter_vec!("read_bytes", &["ks", "kind", "type"], registry),
            loaded_keys: gauge_vec!("loaded_keys", &["ks"], registry),

            lookup_mcs: histogram_vec!(
                "lookup_mcs",
                &["type", "ks"],
                lookup_buckets.clone(),
                registry
            ),
            large_table_contention: histogram_vec!(
                "large_table_contention",
                &["ks"],
                lock_buckets.clone(),
                registry
            ),
            wal_contention: histogram!("wal_contention", lock_buckets.clone(), registry),
            db_op_mcs: histogram_vec!("db_op", &["op", "ks"], db_op_buckets, registry),
            map_time_mcs: histogram!("map_time_mcs", lookup_buckets.clone(), registry),
            // loaded_keys_total_bytes: gauge!("loaded_keys_total_bytes", registry),
        };
        Arc::new(this)
    }
}

pub trait TimerExt {
    fn mcs_timer(self) -> impl Drop;
}

pub struct McsTimer {
    histogram: Histogram,
    start: Instant,
}

impl TimerExt for Histogram {
    fn mcs_timer(self) -> impl Drop {
        McsTimer {
            histogram: self,
            start: Instant::now(),
        }
    }
}

impl Drop for McsTimer {
    fn drop(&mut self) {
        self.histogram
            .observe(self.start.elapsed().as_micros() as f64)
    }
}
