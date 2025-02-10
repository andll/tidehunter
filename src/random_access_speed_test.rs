use rand::rngs::ThreadRng;
use rand::{Rng, RngCore};
use std::alloc::Layout;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const DIRECT_IO: bool = false;

pub(crate) fn random_access_speed_test() {
    let path: PathBuf = "/opt/sui/thdb/ras".into();
    // let len = 10 * 1024 * 1024 * 1024; // 10 Gb
    let len = 2 * 1024 * 1024 * 1024 * 1024; // 2 Tb
    {
        if !path.exists() {
            println!("Generating file");
            let file = File::create(&path).unwrap();
            let w_block = 1024u64 * 1024;
            let mut w_buf = vec![0u8; w_block as usize];
            let mut rng = ThreadRng::default();
            file.set_len(len).unwrap();
            for p in 0..(len / w_block) {
                rng.fill_bytes(&mut w_buf);
                file.write_all_at(&w_buf, p * w_block).unwrap();
            }
            file.sync_all().unwrap();
            println!("Starting read test");
        } else {
            println!("File exists");
        }
    }
    let mut options = OpenOptions::new();
    options.read(true);
    if DIRECT_IO {
        options.custom_flags(0x4000 /*O_DIRECT*/);
    }
    let file = options.open(&path).unwrap();
    let file = Arc::new(file);
    for _ in 0..4 {
        test_buf(file.clone(), 1024, len);
        test_buf(file.clone(), 4 * 1024, len);
        test_buf(file.clone(), 16 * 1024, len);
        test_buf(file.clone(), 32 * 1024, len);
        test_buf(file.clone(), 64 * 1024, len);
        test_buf(file.clone(), 128 * 1024, len);
        test_buf(file.clone(), 256 * 1024, len);
        test_buf(file.clone(), 512 * 1024, len);
        test_buf(file.clone(), 1024 * 1024, len);
    }
}

fn test_buf(f: Arc<File>, buf_size: usize, file_size: u64) {
    use parking_lot::RwLock;
    let rwl = Arc::new(RwLock::new(()));
    let wl = rwl.write();
    let ops = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    for _ in 0..thread::available_parallelism().unwrap().get() {
        let rwl = rwl.clone();
        let stop = stop.clone();
        let ops = ops.clone();
        let f = f.clone();
        thread::spawn(move || {
            let mut rng = ThreadRng::default();
            let mut buf = if DIRECT_IO {
                unsafe {
                    let mem =
                        std::alloc::alloc(Layout::from_size_align(buf_size, 4 * 1024).unwrap());
                    Vec::from_raw_parts(mem, buf_size, buf_size)
                }
            } else {
                vec![0; buf_size]
            };
            let _l = rwl.read();
            while !stop.load(Ordering::Relaxed) {
                let mut pos = rng.gen_range(0..(file_size - buf_size as u64));
                if DIRECT_IO {
                    pos = pos / 512 * 512;
                }
                f.read_exact_at(&mut buf, pos).unwrap();
                ops.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    drop(wl);
    let secs = 60;
    thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    let _ = rwl.write();
    let ops = ops.load(Ordering::Relaxed);
    println!(
        "Buf: {} Kb. IOPS {}, throughput {}/s",
        buf_size / 1024,
        ops as u64 / secs,
        format_bytes(ops * buf_size / secs as usize)
    );
}

fn format_bytes(l: usize) -> String {
    if l < 1024 {
        format!("{}  b", l)
    } else if l < 1024 * 1024 {
        format!("{:.1} Kb", l as f64 / 1024.)
    } else if l < 1024 * 1024 * 1024 {
        format!("{:.1} Mb", l as f64 / 1024. / 1024.)
    } else {
        format!("{:.1} Gb", l as f64 / 1024. / 1024. / 1024.)
    }
}
