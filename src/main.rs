pub mod batch;
pub mod config;
mod control;
mod crc;
pub mod db;
mod index_table;
pub mod iterators;
mod key_shape;
mod large_table;
mod lookup;
pub mod metrics;
mod primitives;
#[cfg(feature = "stress")]
mod stress;
mod wal;

fn main() {
    // random_access_speed_test::random_access_speed_test().unwrap();
    #[cfg(feature = "stress")]
    stress::main();
}

#[allow(dead_code)]
#[cfg(feature = "stress")]
mod random_access_speed_test {
    use rand::rngs::ThreadRng;
    use rand::{Rng, RngCore};
    use std::fs::File;
    use std::os::unix::fs::FileExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use std::{io, thread};

    pub(crate) fn random_access_speed_test() -> io::Result<()> {
        let path: PathBuf = "f".into();
        let len = 30 * 1024 * 1024 * 1024; // 10 Gb
        let file = if !path.exists() {
            println!("Generating file");
            let file = File::create(path)?;
            let w_block = 1024u64 * 1024;
            let mut w_buf = vec![0u8; w_block as usize];
            let mut rng = ThreadRng::default();
            file.set_len(len)?;
            for p in 0..(len / w_block) {
                rng.fill_bytes(&mut w_buf);
                file.write_all_at(&w_buf, p * w_block)?;
            }
            file.sync_all()?;
            println!("Starting read test");
            file
        } else {
            println!("File exists");
            File::open(path)?
        };
        let file = Arc::new(file);
        for _ in 0..4 {
            test_buf(file.clone(), 1536, len);
            test_buf(file.clone(), 48 * 1024, len);
        }
        Ok(())
    }

    fn test_buf(f: Arc<File>, buf_size: usize, file_size: u64) {
        use parking_lot::RwLock;
        let rwl = Arc::new(RwLock::new(()));
        let wl = rwl.write();
        let ops = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        for _ in 0..8 {
            let rwl = rwl.clone();
            let stop = stop.clone();
            let ops = ops.clone();
            let f = f.clone();
            thread::spawn(move || {
                let mut rng = ThreadRng::default();
                let mut buf = vec![0u8; buf_size];
                let _l = rwl.read();
                while !stop.load(Ordering::Relaxed) {
                    let pos = rng.gen_range(0..(file_size - buf_size as u64));
                    f.read_exact_at(&mut buf, pos).unwrap();
                    ops.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        drop(wl);
        let secs = 20;
        thread::sleep(Duration::from_secs(secs));
        stop.store(true, Ordering::Relaxed);
        let _ = rwl.write();
        let ops = ops.load(Ordering::Relaxed);
        println!(
            "Buf: {buf_size} bytes. Operations: {ops}, IOPS/sec {}",
            ops as u64 / secs
        );
    }
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
