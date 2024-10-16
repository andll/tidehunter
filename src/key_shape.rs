use crate::config::Config;
use crate::large_table::LARGE_TABLE_MUTEXES;
use std::cmp;
use std::ops::Range;

#[derive(Clone)]
pub struct KeyShape {
    key_spaces: Vec<KeySpaceDesc>,
}

pub struct KeyShapeBuilder {
    large_table_size: usize,
    frac_base: usize,
    const_spaces: usize,
    frac_spaces: usize,
    key_spaces: Vec<KeySpaceDesc>,
}

#[derive(Clone, Copy)]
pub struct KeySpace(pub(crate) u8);

#[derive(Clone)]
struct KeySpaceDesc {
    range: Range<usize>,
    config: KeySpaceConfig,
}

#[derive(Default, Clone)]
pub struct KeySpaceConfig {
    key_offset: usize,
}

impl KeyShapeBuilder {
    pub fn from_config(config: &Config, frac_base: usize) -> Self {
        let large_table_size = config.large_table_size();
        Self {
            large_table_size,
            const_spaces: 0,
            frac_spaces: 0,
            frac_base,
            key_spaces: vec![],
        }
    }

    pub fn const_key_space(&mut self, size: usize) -> KeySpace {
        self.const_key_space_config(size, KeySpaceConfig::default())
    }

    /// round up const_spaces to a multiple of LARGE_TABLE_MUTEXES and return const_space size
    pub fn pad_const_space(&mut self) -> usize {
        let b = (self.const_spaces + LARGE_TABLE_MUTEXES - 1) / LARGE_TABLE_MUTEXES;
        b * LARGE_TABLE_MUTEXES
    }

    pub fn const_key_space_config(&mut self, size: usize, config: KeySpaceConfig) -> KeySpace {
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
        self.add_key_space(KeySpaceDesc { range, config })
    }

    pub fn frac_key_space(&mut self, frac: usize) -> KeySpace {
        self.frac_key_space_config(frac, KeySpaceConfig::default())
    }

    pub fn frac_key_space_config(&mut self, frac: usize, config: KeySpaceConfig) -> KeySpace {
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
        self.add_key_space(KeySpaceDesc { range, config })
    }

    fn add_key_space(&mut self, key_space: KeySpaceDesc) -> KeySpace {
        assert!(
            self.key_spaces.len() < (u8::MAX - 1) as usize,
            "Maximum {} key spaces allowed",
            u8::MAX
        );
        self.key_spaces.push(key_space);
        KeySpace(self.key_spaces.len() as u8)
    }

    pub fn build(self) -> KeyShape {
        KeyShape {
            key_spaces: self.key_spaces,
        }
    }
}
impl KeySpaceDesc {
    pub(crate) fn cell(&self, k: &[u8]) -> usize {
        self.cell_by_prefix(self.cell_prefix(k))
    }

    fn cell_by_prefix(&self, prefix: u32) -> usize {
        let prefix = prefix as usize;
        let len = self.range.end - self.range.start;
        self.range.start + (prefix % len)
    }

    fn cell_prefix(&self, k: &[u8]) -> u32 {
        let k = &k[self.config.key_offset..];
        let copy = cmp::min(k.len(), 4);
        let mut p = [0u8; 4];
        p[..copy].copy_from_slice(&k[..copy]);
        u32::from_le_bytes(p)
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
        Self { key_offset }
    }
}

impl KeyShape {
    pub fn new_whole(config: &Config) -> (Self, KeySpace) {
        let key_space = KeySpaceDesc {
            range: 0..config.large_table_size,
            config: Default::default(),
        };
        let key_spaces = vec![key_space];
        let this = Self { key_spaces };
        (this, KeySpace(0))
    }

    pub(crate) fn cell(&self, ks: KeySpace, k: &[u8]) -> usize {
        self.ks(ks).cell(k)
    }

    pub(crate) fn range_cell(
        &self,
        ks: KeySpace,
        from_included: &[u8],
        to_included: &[u8],
    ) -> usize {
        self.ks(ks).range_cell(from_included, to_included)
    }

    pub(crate) fn key_space_range(&self, ks: KeySpace) -> Range<usize> {
        let ks = self.ks(ks);
        ks.range.clone()
    }

    fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        let Some(key_space) = self.key_spaces.get(ks.0 as usize) else {
            panic!("Key space {} not found", ks.0)
        };
        key_space
    }
}
