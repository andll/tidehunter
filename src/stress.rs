use crate::config::Config;
use crate::db::Db;
use crate::metrics::Metrics;
use clap::Parser;
use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

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
}

pub fn main() {
    let args = StressArgs::parse();
    let args = Arc::new(args);
    let dir = tempdir::TempDir::new("stress").unwrap();
    let config = Config::default();
    let db = Db::open(dir.path(), &config, Metrics::new()).unwrap();
    let db = Arc::new(db);
    let mut threads = Vec::with_capacity(args.threads);
    let start_lock = Arc::new(RwLock::new(()));
    let start_w = start_lock.write();
    for i in 0..args.threads {
        let rng = StdRng::seed_from_u64(i as u64);
        let thread = StressThread {
            db: db.clone(),
            start_lock: start_lock.clone(),
            args: args.clone(),
            rng,
        };
        let thread = thread::spawn(move || thread.run());
        threads.push(thread);
    }
    let start = Instant::now();
    drop(start_w);
    for t in threads {
        t.join().unwrap();
    }
    let elapsed = start.elapsed();
    println!("Done in {:?}", elapsed);
    let written = args.writes * args.threads;
    let written_bytes = written * args.write_size;
    let msecs = elapsed.as_millis() as usize;
    println!(
        "{} writes/s, {} bytes/sec",
        dec_div(written / msecs * 1000),
        byte_div(written_bytes / msecs * 1000)
    );
}

fn dec_div(n: usize) -> String {
    const M: usize = 1_000_000;
    const K: usize = 1_000;
    if n > M {
        format!("{}M ", n / M)
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
        format!("{}Mb ", n / M)
    } else if n > K {
        format!("{}Kb", n / K)
    } else {
        format!("{n}")
    }
}

struct StressThread {
    db: Arc<Db>,
    start_lock: Arc<RwLock<()>>,
    args: Arc<StressArgs>,
    rng: StdRng,
}

impl StressThread {
    pub fn run(mut self) {
        let _ = self.start_lock.read();
        for _ in 0..self.args.writes {
            let mut data = vec![0u8; self.args.write_size];
            let mut key = vec![0u8; self.args.key_len];
            self.rng.fill_bytes(&mut key);
            self.rng.fill_bytes(&mut data);
            self.db.insert(key, data).unwrap();
        }
    }
}
