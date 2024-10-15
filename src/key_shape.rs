use crate::config::Config;
use std::cmp;

pub struct KeyShape {
    large_table_size: usize,
}

impl KeyShape {
    pub fn from_config(config: &Config) -> Self {
        let large_table_size = config.large_table_size();
        Self { large_table_size }
    }

    pub fn cell(&self, k: &[u8]) -> usize {
        /* pub(crate) for tests */
        self.cell_by_prefix(Self::cell_prefix(k))
    }

    fn cell_prefix(k: &[u8]) -> u32 {
        let copy = cmp::min(k.len(), 4);
        let mut p = [0u8; 4];
        p[..copy].copy_from_slice(&k[..copy]);
        u32::from_le_bytes(p)
    }

    fn cell_by_prefix(&self, prefix: u32) -> usize {
        (prefix as usize) % self.large_table_size
    }

    /// Returns the cell containing the range.
    /// Right now, this only works if the entire range "fits" single cell.
    pub fn range_cell(&self, from_included: &[u8], to_included: &[u8]) -> usize {
        let start_prefix = Self::cell_prefix(&from_included);
        let end_prefix = Self::cell_prefix(&to_included);
        if start_prefix == end_prefix {
            self.cell_by_prefix(start_prefix)
        } else {
            panic!("Can't have ordered iterator over key range that does not fit same large table cell");
        }
    }
}
