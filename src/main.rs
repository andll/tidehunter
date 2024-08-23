pub mod batch;
pub mod config;
mod control;
mod crc;
pub mod db;
mod large_table;
pub mod metrics;
mod primitives;
#[cfg(feature = "stress")]
mod stress;
mod wal;

fn main() {
    #[cfg(feature = "stress")]
    stress::main();
}

#[allow(dead_code)]
fn mutex_speed_test() {
    use parking_lot::Mutex;
    use std::time::Instant;
    const C: usize = 1_000_000;
    let mut v = Vec::with_capacity(C);
    for i in 0..C {
        v.push(Mutex::new(i));
    }
    let start = Instant::now();
    for m in &v {
        *m.lock() += 1;
    }
    println!("Duration {:?}", start.elapsed());
}
