use parking_lot::Mutex;
use std::time::Instant;

mod batch;
mod config;
mod control;
mod crc;
mod db;
mod large_table;
mod metrics;
mod primitives;
mod wal;

fn main() {
    mutex_speed_test();
}

fn mutex_speed_test() {
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
