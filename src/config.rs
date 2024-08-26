use crate::control::ControlRegion;
use crate::crc::CrcFrame;
use crate::wal::WalLayout;

pub struct Config {
    frag_size: u64,
    large_table_size: usize,
    max_maps: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frag_size: 128 * 1024 * 1024,
            large_table_size: 64 * 1024,
            max_maps: 16, // Max 2 Gb mapped space
        }
    }
}

impl Config {
    pub fn small() -> Self {
        Self {
            frag_size: 1024 * 1024,
            large_table_size: 256,
            max_maps: 16,
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
}
