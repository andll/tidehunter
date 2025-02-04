use crate::key_shape::KeySpaceDesc;
use crate::lookup::RandomRead;
use crate::math::rescale_u32;
use crate::wal::WalPosition;
use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use std::collections::{BTreeMap, HashSet};
use std::ops::RangeInclusive;

#[derive(Default, Clone, Debug)]
pub(crate) struct IndexTable {
    // todo instead of loading entire BTreeMap in memory we should be able
    // to load parts of it from disk
    pub(crate) data: BTreeMap<Bytes, WalPosition>,
}

const HEADER_ELEMENTS: usize = 1024;
const HEADER_ELEMENT_SIZE: usize = 8;
const HEADER_SIZE: usize = HEADER_ELEMENTS * HEADER_ELEMENT_SIZE;

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) -> Option<WalPosition> {
        self.data.insert(k, v)
    }

    pub fn remove(&mut self, k: &[u8]) -> Option<WalPosition> {
        self.data.remove(k)
    }

    /// Merges dirty IndexTable into a loaded IndexTable
    pub fn merge_dirty(&mut self, dirty: &Self) {
        // todo implement this efficiently taking into account both self and dirty are sorted
        for (k, v) in dirty.data.iter() {
            if v == &WalPosition::INVALID {
                self.remove(k);
            } else {
                self.insert(k.clone(), *v);
            }
        }
    }

    /// Remove flushed index entries, returning number of entries changed
    pub fn unmerge_flushed(&mut self, original: &Self) -> i64 {
        let mut delta = 0i64;
        for (k, v) in original.data.iter() {
            let prev = self.data.get(k);
            let Some(prev) = prev else {
                panic!("Original entry not found during unmerge_flushed")
            };
            if prev == v {
                self.data.remove(k);
                delta -= 1;
            }
        }
        delta
    }

    /// Change loaded dirty IndexTable into unloaded dirty by retaining dirty keys and tombstones
    /// Returns delta in number of entries
    pub fn make_dirty(&mut self, mut dirty_keys: HashSet<Bytes>) -> i64 {
        let original_data_len = self.data.len() as i64;
        // todo this method can be optimized if dirty_keys are made sorted
        // only retain keys that are dirty, removing all clean keys
        self.data.retain(|k, _| dirty_keys.remove(k));
        // remaining dirty_keys are not in this index, means they were deleted
        // turn them into tombstones
        for dirty_key in dirty_keys {
            self.insert(dirty_key, WalPosition::INVALID);
        }
        let data_len = self.data.len() as i64;
        data_len - original_data_len
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        self.data.get(k).copied()
    }

    /// If next_entry is None returns first entry.
    ///
    /// If next_entry is not None, returns entry on or after specified next_entry.
    ///
    /// Returns tuple of a key, value and an optional next key if present.
    ///
    /// This works even if next is set to Some(k), but the value at k does not exist (for ex. was deleted).
    /// For this reason, the returned key might be different from the next key requested.
    pub fn next_entry(&self, next: Option<Bytes>) -> Option<(Bytes, WalPosition, Option<Bytes>)> {
        fn take_next<'a>(
            mut iter: impl Iterator<Item = (&'a Bytes, &'a WalPosition)>,
        ) -> Option<(Bytes, WalPosition, Option<Bytes>)> {
            let (key, value) = iter.next()?;
            let next_key = iter.next().map(|(k, _v)| k.clone());
            Some((key.clone(), *value, next_key))
        }

        if let Some(next) = next {
            let range = self.data.range(next..);
            take_next(range.into_iter())
        } else {
            take_next(self.data.iter())
        }
    }

    pub fn last_in_range(
        &self,
        from_included: &Bytes,
        to_included: &Bytes,
    ) -> Option<(Bytes, WalPosition)> {
        let range = self
            .data
            .range::<Bytes, RangeInclusive<&Bytes>>(from_included..=to_included);
        if let Some((bytes, position)) = range.last() {
            Some((bytes.clone(), *position))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn to_bytes(&self, ks: &KeySpaceDesc) -> Bytes {
        let element_size = Self::element_size(ks);
        let capacity = element_size * self.data.len() + HEADER_SIZE;
        let mut out = BytesMut::with_capacity(capacity);
        out.put_bytes(0, HEADER_SIZE);
        let mut header = IndexTableHeaderBuilder::new(ks);
        for (key, value) in self.data.iter() {
            if key.len() != ks.key_size() {
                // todo make into debug assertion
                panic!(
                    "Index in ks {} contains key length {} (configured {})",
                    ks.name(),
                    key.len(),
                    ks.key_size()
                );
            }
            header.add_key(key, out.len());
            out.put_slice(&key);
            value.write_to_buf(&mut out);
        }
        assert_eq!(out.len(), capacity);
        header.write_header(out.len(), &mut out[..HEADER_SIZE]);
        out.to_vec().into()
    }

    pub fn from_bytes(ks: &KeySpaceDesc, b: Bytes) -> Self {
        let b = b.slice(HEADER_SIZE..);
        let element_size = Self::element_size(ks);
        let elements = b.len() / element_size;
        assert_eq!(b.len(), elements * element_size);

        let mut data = BTreeMap::new();
        for i in 0..elements {
            let key = b.slice(i * element_size..(i * element_size + ks.key_size()));
            let value = WalPosition::from_slice(
                &b[(i * element_size + ks.key_size())..(i * element_size + element_size)],
            );
            data.insert(key, value);
        }

        assert_eq!(data.len(), elements);
        Self { data }
    }

    pub fn element_size(ks: &KeySpaceDesc) -> usize {
        ks.key_size() + WalPosition::LENGTH
    }

    pub fn lookup_unloaded(
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        key: &[u8],
    ) -> Option<WalPosition> {
        let key_size = ks.key_size();
        assert_eq!(key.len(), key_size);
        let micro_cell = Self::key_micro_cell(ks, key);
        let header_element =
            reader.read(micro_cell * HEADER_ELEMENT_SIZE..(micro_cell + 1) * HEADER_ELEMENT_SIZE);
        let mut header_element = &header_element[..];
        let from_offset = header_element.get_u32() as usize;
        let to_offset = header_element.get_u32() as usize;
        if from_offset == 0 && to_offset == 0 {
            return None;
        }
        let buffer = reader.read(from_offset..to_offset);
        let mut buffer = &buffer[..];
        let element_size = Self::element_size(ks);
        while !buffer.is_empty() {
            let k = &buffer[..key_size];
            if k == key {
                buffer = &buffer[key_size..];
                let position = WalPosition::read_from_buf(&mut buffer);
                return Some(position);
            }
            buffer = &buffer[element_size..];
        }
        None
    }

    fn key_micro_cell(ks: &KeySpaceDesc, key: &[u8]) -> usize {
        let prefix = ks.cell_prefix(key);
        let cell = ks.cell_by_prefix(prefix);
        let cell_prefix_range = ks.cell_prefix_range(cell);
        let cell_offset = prefix
            // cell_prefix_range.start is always u32 (but not cell_prefix_range.end)
            .checked_sub(cell_prefix_range.start as u32)
            .expect("Key prefix is out of cell prefix range");
        let cell_size = ks.cell_size();
        let micro_cell = rescale_u32(cell_offset, cell_size, HEADER_ELEMENTS as u32);
        micro_cell as usize
    }
}

pub struct IndexTableHeaderBuilder<'a> {
    ks: &'a KeySpaceDesc,
    header: Vec<(u32, u32)>,
    last_micro_cell: Option<usize>,
}

impl<'a> IndexTableHeaderBuilder<'a> {
    pub fn new(ks: &'a KeySpaceDesc) -> Self {
        let header = (0..HEADER_ELEMENTS).map(|_| (0, 0)).collect();
        Self {
            ks,
            header,
            last_micro_cell: None,
        }
    }

    pub fn add_key(&mut self, key: &[u8], offset: usize) {
        let offset = Self::check_offset(offset);
        let micro_cell = IndexTable::key_micro_cell(&self.ks, key);
        if let Some(last_micro_cell) = self.last_micro_cell {
            if last_micro_cell == micro_cell {
                return;
            }
            self.header[last_micro_cell].1 = offset;
        }
        self.last_micro_cell = Some(micro_cell);
        self.header[micro_cell].0 = offset;
    }

    pub fn write_header(self, end_offset: usize, mut buf: &mut [u8]) {
        let mut header = self.header;
        let end_offset = Self::check_offset(end_offset);
        if let Some(last_micro_cell) = self.last_micro_cell {
            header[last_micro_cell].1 = end_offset;
        }
        for (start, end) in header {
            buf.put_u32(start);
            buf.put_u32(end);
        }
    }

    fn check_offset(offset: usize) -> u32 {
        assert!(offset < u32::MAX as usize, "Index table is too large");
        offset as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_shape::KeyShape;
    use rand::rngs::ThreadRng;
    use rand::{Rng, RngCore};

    #[test]
    pub fn test_index_lookup() {
        let (shape, ks) = KeyShape::new_single(16, 8, 8);
        let ks = shape.ks(ks);
        let mut index = IndexTable::default();
        index.insert(k(1), w(5));
        index.insert(k(5), w(10));
        let bytes = index.to_bytes(ks);
        assert_eq!(None, IndexTable::lookup_unloaded(ks, &bytes, &k(0)));
        assert_eq!(Some(w(5)), IndexTable::lookup_unloaded(ks, &bytes, &k(1)));
        assert_eq!(Some(w(10)), IndexTable::lookup_unloaded(ks, &bytes, &k(5)));
        assert_eq!(None, IndexTable::lookup_unloaded(ks, &bytes, &k(10)));
        let mut index = IndexTable::default();
        index.insert(k(u128::MAX), w(15));
        index.insert(k(u128::MAX - 5), w(25));
        let bytes = index.to_bytes(ks);
        assert_eq!(
            Some(w(15)),
            IndexTable::lookup_unloaded(ks, &bytes, &k(u128::MAX))
        );
        assert_eq!(
            Some(w(25)),
            IndexTable::lookup_unloaded(ks, &bytes, &k(u128::MAX - 5))
        );
        assert_eq!(
            None,
            IndexTable::lookup_unloaded(ks, &bytes, &k(u128::MAX - 1))
        );
        assert_eq!(
            None,
            IndexTable::lookup_unloaded(ks, &bytes, &k(u128::MAX - 100))
        );
    }

    #[test]
    pub fn test_index_lookup_random() {
        const M: usize = 8;
        const P: usize = 8;
        let (shape, ks) = KeyShape::new_single(16, M, P);
        let ks = shape.ks(ks);
        let mut index = IndexTable::default();
        let mut rng = ThreadRng::default();
        let target_bucket = rng.gen_range(0..((M * P) as u128));
        let bucket_size = u128::MAX / ((M * P) as u128);
        let target_range = target_bucket * bucket_size..(target_bucket + 1) * bucket_size;
        const ITERATIONS: usize = 1000;
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            let pos = rng.next_u64();
            index.insert(k(key), w(pos));
        }
        let bytes = index.to_bytes(ks);
        for (key, expected_value) in index.data {
            let value = IndexTable::lookup_unloaded(ks, &bytes, &key);
            assert_eq!(Some(expected_value), value);
        }
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            let value = IndexTable::lookup_unloaded(ks, &bytes, &k(key));
            assert!(value.is_none());
        }
    }

    #[test]
    pub fn test_unmerge_flushed() {
        let mut index = IndexTable::default();
        index.insert(vec![1].into(), WalPosition::test_value(2));
        index.insert(vec![2].into(), WalPosition::test_value(3));
        index.insert(vec![6].into(), WalPosition::test_value(4));
        let mut index2 = index.clone();
        index2.insert(vec![1].into(), WalPosition::test_value(5));
        index2.insert(vec![3].into(), WalPosition::test_value(8));
        assert_eq!(index2.unmerge_flushed(&index), -2);
        let data = index2.data.into_iter().collect::<Vec<_>>();
        assert_eq!(
            data,
            vec![
                (vec![1].into(), WalPosition::test_value(5)),
                (vec![3].into(), WalPosition::test_value(8))
            ]
        );
    }

    fn k(k: u128) -> Bytes {
        k.to_be_bytes().to_vec().into()
    }

    fn w(w: u64) -> WalPosition {
        WalPosition::test_value(w)
    }
}
