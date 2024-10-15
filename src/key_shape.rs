use crate::config::Config;
use std::cmp;
use std::ops::Range;

pub struct KeyShape {
    large_table_size: usize,
}

pub struct KeySpace {
    range: Range<usize>,
    config: KeySpaceConfig,
}

#[derive(Default)]
pub struct KeySpaceConfig {
    key_offset: usize,
}

pub struct KeySpaceBuilder {
    large_table_size: usize,
    frac_base: usize,
    const_spaces: usize,
    frac_spaces: usize,
}

impl KeySpaceBuilder {
    pub fn from_config(config: &Config, frac_base: usize) -> Self {
        let large_table_size = config.large_table_size();
        Self {
            large_table_size,
            const_spaces: 0,
            frac_spaces: 0,
            frac_base,
        }
    }

    pub fn const_key_space(&mut self, size: usize, config: KeySpaceConfig) -> KeySpace {
        assert!(size > 0, "Key space size should be greater then 0");
        assert!(
            size + self.const_spaces <= self.large_table_size,
            "Total key space size should not exceed configured large table size"
        );
        assert_eq!(
            self.frac_spaces, 0,
            "Should add all const key spaces before frac key space"
        );
        let start = self.const_spaces;
        self.const_spaces += size;
        let range = start..self.const_spaces;
        KeySpace { range, config }
    }

    pub fn frac_key_space(&mut self, frac: usize, config: KeySpaceConfig) -> KeySpace {
        assert!(frac > 0, "Key space size should be greater then 0");
        assert!(
            frac + self.frac_spaces <= self.frac_base,
            "Total frac key space size({}) should not exceed frac_base({})",
            frac + self.frac_spaces,
            self.frac_spaces
        );
        let total_frac_space = self.large_table_size - self.const_spaces;
        let per_frac = total_frac_space / self.frac_base;
        assert_eq!(
            total_frac_space,
            per_frac * self.frac_base,
            "Total frac space ({total_frac_space}) is not divisible by requested frac_base({})",
            self.frac_base
        );
        let start = self.frac_spaces * per_frac;
        self.frac_spaces += frac;
        let end = self.frac_spaces * per_frac;
        let range = start..end;
        KeySpace { range, config }
    }
}
impl KeySpace {
    pub(crate) fn cell(&self, k: &[u8]) -> usize {
        self.cell_by_prefix(self.cell_prefix(k))
    }

    fn cell_by_prefix(&self, prefix: u32) -> usize {
        let prefix = prefix as usize;
        let len = self.range.end - self.range.start;
        self.range.start + (prefix % len)
    }

    fn cell_prefix(&self, k: &[u8]) -> u32 {
        KeyShape::cell_prefix(&k[self.config.key_offset..])
    }

    /// Returns the cell containing the range.
    /// Right now, this only works if the entire range "fits" single cell.
    pub(crate) fn range_cell(&self, from_included: &[u8], to_included: &[u8]) -> usize {
        let start_prefix = self.cell_prefix(&from_included);
        let end_prefix = self.cell_prefix(&to_included);
        if start_prefix == end_prefix {
            self.cell_by_prefix(start_prefix)
        } else {
            panic!("Can't have ordered iterator over key range that does not fit same large table cell");
        }
    }
}

impl KeySpaceConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_key_offset(key_offset: usize) -> Self {
        Self {
            key_offset,
        }
    }
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
