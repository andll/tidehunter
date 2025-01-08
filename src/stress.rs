use crate::config::Config;
use crate::db::Db;
use crate::key_shape::{KeyShape, KeySpace};
use crate::metrics::Metrics;
use bytes::BufMut;
use clap::Parser;
use parking_lot::RwLock;
use rand::rngs::{StdRng, ThreadRng};
use rand::{Rng, RngCore, SeedableRng};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
struct StressArgs {
    #[arg(long, short = 't', help = "Number of write threads")]
    threads: usize,
    #[arg(long, short = 'b', help = "Write block size")]
    write_size: usize,
    #[arg(long, short = 'k', help = "Length of the key")]
    key_len: usize,
    #[arg(long, short = 'w', help = "Blocks to write per thread")]
    writes: usize,
    #[arg(long, short = 'r', help = "Blocks to read per thread")]
    reads: usize,
}

pub fn main() {
    let args = StressArgs::parse();
    let args = Arc::new(args);
    let dir = tempdir::TempDir::new("stress").unwrap();
    let config = Arc::new(Config::default());
    let metrics = Metrics::new();
    let (key_shape, ks) = KeyShape::new_single(32, 1024, 32);
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let db = Arc::new(db);
    let stress = Stress { db, args };
    let elapsed = stress.measure(StressThread::run_writes);
    let written = stress.args.writes * stress.args.threads;
    let written_bytes = written * stress.args.write_size;
    let msecs = elapsed.as_millis() as usize;
    println!(
        "Done in {elapsed:?}: {} writes/s, {}/sec",
        dec_div(written / msecs * 1000),
        byte_div(written_bytes / msecs * 1000)
    );
    let elapsed = stress.measure(StressThread::run_reads);
    let read = stress.args.reads * stress.args.threads;
    let read_bytes = read * stress.args.write_size;
    let msecs = elapsed.as_millis() as usize;
    println!(
        "Done in {elapsed:?}: {} reads/s, {}/sec",
        dec_div(read / msecs * 1000),
        byte_div(read_bytes / msecs * 1000)
    );
    println!(
        "Max index size {} entries",
        metrics.max_index_size.load(Ordering::Relaxed)
    )
}

struct Stress {
    db: Arc<Db>,
    args: Arc<StressArgs>,
}

impl Stress {
    pub fn measure<F: FnOnce(StressThread) + Clone + Send + 'static>(&self, f: F) -> Duration {
        let mut threads = Vec::with_capacity(self.args.threads);
        let start_lock = Arc::new(RwLock::new(()));
        let start_w = start_lock.write();
        for index in 0..self.args.threads {
            let thread = StressThread {
                ks,
                db: self.db.clone(),
                start_lock: start_lock.clone(),
                args: self.args.clone(),
                index: index as u64,
            };
            let f = f.clone();
            let thread = thread::spawn(move || f(thread));
            threads.push(thread);
        }
        let start = Instant::now();
        drop(start_w);
        for t in threads {
            t.join().unwrap();
        }
        start.elapsed()
    }
}

fn dec_div(n: usize) -> String {
    const M: usize = 1_000_000;
    const K: usize = 1_000;
    if n > M {
        format!("{}M", n / M)
    } else if n > K {
        format!("{}K", n / K)
    } else {
        format!("{n}")
    }
}

fn byte_div(n: usize) -> String {
    const K: usize = 1_024;
    const M: usize = K * K;
    if n > M {
        format!("{}Mb", n / M)
    } else if n > K {
        format!("{}Kb", n / K)
    } else {
        format!("{n}")
    }
}

struct StressThread {
    db: Arc<Db>,
    ks: KeySpace,
    start_lock: Arc<RwLock<()>>,
    args: Arc<StressArgs>,
    index: u64,
}

impl StressThread {
    pub fn run_writes(self) {
        let _ = self.start_lock.read();
        for pos in 0..self.args.writes {
            let (key, value) = self.key_value(pos);
            self.db.insert(self.ks, key, value).unwrap();
        }
    }

    pub fn run_reads(self) {
        let _ = self.start_lock.read();
        let mut pos_rng = ThreadRng::default();
        for _ in 0..self.args.reads {
            let pos = pos_rng.gen_range(0..self.args.writes);
            let (key, value) = self.key_value(pos);
            let found_value = self
                .db
                .get(self.ks, &key)
                .unwrap()
                .expect("Expected value not found");
            assert_eq!(
                &value[..],
                &found_value[..],
                "Found value does not match expected value"
            );
        }
    }

    fn key_value(&self, pos: usize) -> (Vec<u8>, Vec<u8>) {
        let mut value = vec![0u8; self.args.write_size];
        let mut key = vec![0u8; self.args.key_len];
        let mut rng = self.rng_at(pos as u64);
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut value);
        (key, value)
    }

    fn rng_at(&self, pos: u64) -> StdRng {
        let mut seed = <StdRng as SeedableRng>::Seed::default();
        let mut writer = &mut seed[..];
        writer.put_u64(self.index);
        writer.put_u64(pos);
        StdRng::from_seed(seed)
    }
}
