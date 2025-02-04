use crate::control::ControlRegion;
use crate::crc::CrcFrame;
use crate::db::MAX_KEY_LEN;
use crate::math::downscale_u32;
use crate::wal::WalPosition;
use minibytes::Bytes;
use std::cmp;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone)]
pub struct KeyShape {
    key_spaces: Vec<KeySpaceDesc>,
}

pub struct KeyShapeBuilder {
    key_spaces: Vec<KeySpaceDesc>,
}

#[derive(Clone, Copy)]
pub struct KeySpace(pub(crate) u8);

#[derive(Clone)]
pub(crate) struct KeySpaceDesc {
    id: KeySpace,
    name: String,
    key_size: usize,
    mutexes: usize,
    per_mutex: usize,
    config: KeySpaceConfig,
}

#[derive(Default, Clone)]
pub struct KeySpaceConfig {
    key_offset: usize,
    compactor: Option<Arc<Compactor>>,
    disable_unload: bool,
    bloom_filter: Option<BloomFilterParams>,
    value_cache_size: usize,
}

#[derive(Default, Clone)]
pub(crate) struct BloomFilterParams {
    pub rate: f32,
    pub count: u32,
}

// todo - we want better compactor API that does not expose too much internal details
// todo - make mod wal private
pub type Compactor = Box<dyn Fn(&mut BTreeMap<Bytes, WalPosition>) + Sync + Send>;

impl KeyShapeBuilder {
    pub fn new() -> Self {
        Self { key_spaces: vec![] }
    }

    pub fn add_key_space(
        &mut self,
        name: impl Into<String>,
        key_size: usize,
        mutexes: usize,
        per_mutex: usize,
    ) -> KeySpace {
        self.add_key_space_config(
            name,
            key_size,
            mutexes,
            per_mutex,
            KeySpaceConfig::default(),
        )
    }

    pub fn add_key_space_config(
        &mut self,
        name: impl Into<String>,
        key_size: usize,
        mutexes: usize,
        per_mutex: usize,
        config: KeySpaceConfig,
    ) -> KeySpace {
        let name = name.into();
        assert!(mutexes > 0, "mutexes should be greater then 0");
        assert!(per_mutex > 0, "per_mutex should be greater then 0");

        assert!(
            self.key_spaces.len() < (u8::MAX - 1) as usize,
            "Maximum {} key spaces allowed",
            u8::MAX
        );
        assert!(
            key_size <= MAX_KEY_LEN,
            "Specified key size exceeding max key length"
        );

        let ks = KeySpace(self.key_spaces.len() as u8);
        let key_space = KeySpaceDesc {
            id: ks,
            name,
            key_size,
            mutexes,
            per_mutex,
            config,
        };
        self.key_spaces.push(key_space);
        ks
    }

    pub fn build(self) -> KeyShape {
        KeyShape {
            key_spaces: self.key_spaces,
        }
    }
}

impl KeySpaceDesc {
    pub(crate) fn check_key(&self, k: &[u8]) {
        if k.len() != self.key_size {
            panic!(
                "Key space {} accepts keys size {}, given {}",
                self.name,
                self.key_size,
                k.len()
            );
        }
    }

    pub(crate) fn locate(&self, k: &[u8]) -> (usize, usize) {
        let prefix = self.cell_prefix(k);
        let cell = self.cell_by_prefix(prefix);
        self.locate_cell(cell)
    }

    pub(crate) fn locate_cell(&self, cell: usize) -> (usize, usize) {
        let mutex = cell % self.num_mutexes();
        let offset = cell / self.num_mutexes();
        (mutex, offset)
    }

    // Reverse of locate_cell
    pub(crate) fn cell_by_location(&self, row: usize, offset: usize) -> usize {
        offset * self.num_mutexes() + row
    }

    pub fn num_mutexes(&self) -> usize {
        self.mutexes
    }

    pub fn cells_per_mutex(&self) -> usize {
        self.per_mutex
    }

    pub fn next_cell(&self, cell: usize) -> Option<usize> {
        if cell >= self.num_cells() - 1 {
            None
        } else {
            Some(cell + 1)
        }
    }

    pub(crate) fn key_size(&self) -> usize {
        self.key_size
    }

    pub(crate) fn num_cells(&self) -> usize {
        self.mutexes * self.per_mutex
    }

    pub(crate) fn cell_by_prefix(&self, prefix: u32) -> usize {
        let bucket = downscale_u32(prefix, self.num_cells() as u32) as usize;
        bucket
    }

    pub(crate) fn cell_prefix(&self, k: &[u8]) -> u32 {
        let k = &k[self.config.key_offset..];
        let copy = cmp::min(k.len(), 4);
        let mut p = [0u8; 4];
        p[..copy].copy_from_slice(&k[..copy]);
        u32::from_be_bytes(p)
    }

    pub(crate) fn cell_prefix_range(&self, cell: usize) -> Range<u64> {
        let cell = cell as u64;
        let cell_size = self.cell_size();
        cell * cell_size..((cell + 1) * cell_size)
    }

    pub(crate) fn cell_size(&self) -> u64 {
        let cells = self.num_cells() as u64;
        // If you have only 1 cell, it has u32::MAX+1 elements,
        (u32::MAX as u64 + 1) / cells
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

    pub(crate) fn bloom_filter(&self) -> Option<&BloomFilterParams> {
        self.config.bloom_filter.as_ref()
    }

    pub(crate) fn value_cache_size(&self) -> Option<NonZeroUsize> {
        NonZeroUsize::new(self.config.value_cache_size)
    }

    pub(crate) fn unloading_disabled(&self) -> bool {
        self.config.disable_unload
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
            disable_unload: false,
            bloom_filter: None,
            value_cache_size: 0,
        }
    }

    pub fn with_compactor(mut self, compactor: Compactor) -> Self {
        self.compactor = Some(Arc::new(compactor));
        self
    }

    pub fn disable_unload(mut self) -> Self {
        self.disable_unload = true;
        self
    }

    pub fn with_bloom_filter(mut self, rate: f32, count: u32) -> Self {
        self.bloom_filter = Some(BloomFilterParams { rate, count });
        self
    }

    pub fn with_value_cache_size(mut self, size: usize) -> Self {
        self.value_cache_size = size;
        self
    }
}

impl KeyShape {
    pub fn new_single(key_size: usize, mutexes: usize, per_mutex: usize) -> (Self, KeySpace) {
        let key_space = KeySpaceDesc {
            id: KeySpace(0),
            name: "root".into(),
            key_size,
            mutexes,
            per_mutex,
            config: Default::default(),
        };
        let key_spaces = vec![key_space];
        let this = Self { key_spaces };
        (this, KeySpace(0))
    }

    pub(crate) fn iter_ks(&self) -> impl Iterator<Item = &KeySpaceDesc> + '_ {
        self.key_spaces.iter()
    }

    pub(crate) fn num_ks(&self) -> usize {
        self.key_spaces.len()
    }

    pub fn cr_len(&self) -> usize {
        ControlRegion::len_bytes_from_key_shape(self) + CrcFrame::CRC_HEADER_LENGTH
    }

    pub(crate) fn range_cell(
        &self,
        ks: KeySpace,
        from_included: &[u8],
        to_included: &[u8],
    ) -> usize {
        self.ks(ks).range_cell(from_included, to_included)
    }

    pub(crate) fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        let Some(key_space) = self.key_spaces.get(ks.0 as usize) else {
            panic!("Key space {} not found", ks.0)
        };
        key_space
    }
}

impl KeySpace {
    pub(crate) fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_by_location() {
        let ks = KeySpaceDesc {
            id: KeySpace(0),
            name: "".to_string(),
            key_size: 0,
            mutexes: 128,
            per_mutex: 512,
            config: Default::default(),
        };
        for cell in 0..1024usize {
            let (row, offset) = ks.locate_cell(cell);
            let evaluated_cell = ks.cell_by_location(row, offset);
            assert_eq!(evaluated_cell, cell);
        }
    }
}
