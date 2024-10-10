use crate::control::ControlRegion;
use crate::crc::CrcFrame;
use crate::wal::WalLayout;

// todo - remove pub
pub struct Config {
    pub frag_size: u64,
    pub large_table_size: usize,
    pub max_maps: usize,
    /// Maximum number of loaded entries per LargeTable row
    pub max_loaded_entries: usize,
    /// Maximum number of dirty keys per LargeTable entry before it's counted as loaded
    pub max_dirty_keys: usize,
    /// How often to take snapshot depending on the number of entries written to the wal
    pub snapshot_written_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frag_size: 128 * 1024 * 1024,
            large_table_size: 64 * 1024,
            max_maps: 16, // Max 2 Gb mapped space
            max_loaded_entries: 16,
            max_dirty_keys: 16 * 1024,
            snapshot_written_bytes: 2 * 1024 * 1024 * 1024, // 2 Gb
        }
    }
}

impl Config {
    pub fn small() -> Self {
        Self {
            frag_size: 1024 * 1024,
            large_table_size: 2 * 1024,
            max_maps: 16,
            max_loaded_entries: 1024,
            max_dirty_keys: 32,
            snapshot_written_bytes: 128 * 1024 * 1024, // 128 Mb
        }
    }

    pub fn large_table_size(&self) -> usize {
        self.large_table_size
    }

    pub fn cr_len(&self) -> usize {
        ControlRegion::len_bytes_from_large_table_size(self.large_table_size())
            + CrcFrame::CRC_HEADER_LENGTH
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

    pub fn max_loaded_entries(&self) -> usize {
        self.max_loaded_entries
    }

    pub fn snapshot_written_bytes(&self) -> u64 {
        self.snapshot_written_bytes
    }

    #[inline]
    pub fn excess_dirty_keys(&self, dirty_keys_count: usize) -> bool {
        dirty_keys_count > self.max_dirty_keys
    }
}
