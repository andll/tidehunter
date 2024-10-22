use crate::config::Config;
use crate::large_table::LARGE_TABLE_MUTEXES;
use crate::wal::WalPosition;
use minibytes::Bytes;
use std::cmp;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

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
pub(crate) struct KeySpaceDesc {
    id: KeySpace,
    name: String,
    range: Range<usize>,
    config: KeySpaceConfig,
}

#[derive(Default, Clone)]
pub struct KeySpaceConfig {
    key_offset: usize,
    compactor: Option<Arc<Compactor>>,
}

// todo - we want better compactor API that does not expose too much internal details
// todo - make mod wal private
pub type Compactor = Box<dyn Fn(&mut BTreeMap<Bytes, WalPosition>) + Sync + Send>;

impl KeyShapeBuilder {
    pub fn from_config(config: &Config, frac_base: usize) -> Self {
        let large_table_size = config.large_table_size();
        Self::new(large_table_size, frac_base)
    }

    pub fn new(large_table_size: usize, frac_base: usize) -> Self {
        Self {
            large_table_size,
            const_spaces: 0,
            frac_spaces: 0,
            frac_base,
            key_spaces: vec![],
        }
    }

    pub fn const_key_space(&mut self, name: impl Into<String>, size: usize) -> KeySpace {
        self.const_key_space_config(name, size, KeySpaceConfig::default())
    }

    /// round up const_spaces to a multiple of LARGE_TABLE_MUTEXES and return const_space size
    pub fn pad_const_space(&mut self) -> usize {
        let b = (self.const_spaces + LARGE_TABLE_MUTEXES - 1) / LARGE_TABLE_MUTEXES;
        b * LARGE_TABLE_MUTEXES
    }

    pub fn const_key_space_config(
        &mut self,
        name: impl Into<String>,
        size: usize,
        config: KeySpaceConfig,
    ) -> KeySpace {
        let name = name.into();
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
        self.add_key_space(name, range, config)
    }

    pub fn frac_key_space(&mut self, name: impl Into<String>, frac: usize) -> KeySpace {
        self.frac_key_space_config(name, frac, KeySpaceConfig::default())
    }

    pub fn frac_key_space_config(
        &mut self,
        name: impl Into<String>,
        frac: usize,
        config: KeySpaceConfig,
    ) -> KeySpace {
        let name = name.into();
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
        let start = self.const_spaces + self.frac_spaces * per_frac;
        self.frac_spaces += frac;
        let end = self.const_spaces + self.frac_spaces * per_frac;
        let range = start..end;
        self.add_key_space(name, range, config)
    }

    fn add_key_space(
        &mut self,
        name: String,
        range: Range<usize>,
        config: KeySpaceConfig,
    ) -> KeySpace {
        assert!(
            self.key_spaces.len() < (u8::MAX - 1) as usize,
            "Maximum {} key spaces allowed",
            u8::MAX
        );
        let ks = KeySpace(self.key_spaces.len() as u8);
        let key_space = KeySpaceDesc {
            id: ks,
            name,
            range,
            config,
        };
        self.key_spaces.push(key_space);
        ks
    }

    pub fn build(self) -> KeyShape {
        self.check_no_overlap();
        KeyShape {
            key_spaces: self.key_spaces,
        }
    }

    fn check_no_overlap(&self) {
        let mut last: Option<usize> = None;
        for (i, ks) in self.key_spaces.iter().enumerate() {
            if let Some(last) = last {
                let start = ks.range.start;
                assert!(
                    start >= last,
                    "Found overlap: ks {i} starting at {start}, previous ended at {last}"
                );
            }
            last = Some(ks.range.end);
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

    pub(crate) fn compactor(&self) -> Option<&Compactor> {
        self.config.compactor.as_ref().map(Arc::as_ref)
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> KeySpace {
        self.id
    }
}

impl KeySpaceConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_key_offset(key_offset: usize) -> Self {
        Self {
            key_offset,
            compactor: None,
        }
    }

    pub fn with_compactor(mut self, compactor: Compactor) -> Self {
        self.compactor = Some(Arc::new(compactor));
        self
    }
}

impl KeyShape {
    pub fn new_whole(config: &Config) -> (Self, KeySpace) {
        let key_space = KeySpaceDesc {
            id: KeySpace(0),
            name: "root".into(),
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

    pub(crate) fn iter_ks_cells(&self) -> impl Iterator<Item = KeySpaceDesc> + '_ {
        self.key_spaces
            .iter()
            .flat_map(|desc| desc.range.clone().into_iter().map(|_| desc.clone()))
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

    pub(crate) fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        let Some(key_space) = self.key_spaces.get(ks.0 as usize) else {
            panic!("Key space {} not found", ks.0)
        };
        key_space
    }
}

#[test]
fn test_ks_builder() {
    let mut ksb = KeyShapeBuilder::new(1024 * 1024 + 1, 8);
    let ks1 = ksb.const_key_space("a", 1);
    let ks2 = ksb.frac_key_space("b", 1);
    let ks3 = ksb.frac_key_space("c", 2);
    let shape = ksb.build();
    assert_eq!(0..1, shape.key_space_range(ks1));
    assert_eq!(1..(1 + 1024 * 1024 / 8), shape.key_space_range(ks2));
    assert_eq!(
        (1 + 1024 * 1024 / 8)..(1 + 1024 * 1024 / 8 * 3),
        shape.key_space_range(ks3)
    );
}
