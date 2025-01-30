use crate::wal::WalLayout;
use rand::Rng;
use std::cmp;

// todo - remove pub
pub struct Config {
    pub frag_size: u64,
    pub max_maps: usize,
    /// Maximum number of dirty keys per LargeTable entry before it's counted as loaded
    pub max_dirty_keys: usize,
    /// How often to take snapshot depending on the number of entries written to the wal
    pub snapshot_written_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frag_size: 128 * 1024 * 1024,
            max_maps: 16, // Max 2 Gb mapped space
            max_dirty_keys: 16 * 1024,
            snapshot_written_bytes: 2 * 1024 * 1024 * 1024, // 2 Gb
        }
    }
}

impl Config {
    pub fn small() -> Self {
        Self {
            frag_size: 1024 * 1024,
            max_maps: 16,
            max_dirty_keys: 32,
            snapshot_written_bytes: 128 * 1024 * 1024, // 128 Mb
        }
    }

    pub fn frag_size(&self) -> u64 {
        self.frag_size
    }

    pub fn wal_layout(&self) -> WalLayout {
        WalLayout {
            frag_size: self.frag_size,
            max_maps: self.max_maps,
        }
    }

    pub fn snapshot_written_bytes(&self) -> u64 {
        self.snapshot_written_bytes
    }

    pub fn gen_dirty_keys_jitter(&self, rng: &mut impl Rng) -> usize {
        rng.gen_range(0..self.max_dirty_keys_jitter())
    }

    fn max_dirty_keys_jitter(&self) -> usize {
        cmp::max(1, self.max_dirty_keys / 10)
    }

    #[inline]
    pub fn excess_dirty_keys(&self, dirty_keys_count: usize) -> bool {
        dirty_keys_count > self.max_dirty_keys
    }
}
