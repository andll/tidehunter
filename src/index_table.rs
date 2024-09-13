use crate::wal::WalPosition;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct IndexTable {
    // todo instead of loading entire BTreeMap in memory we should be able
    // to load parts of it from disk
    data: BTreeMap<Bytes, WalPosition>,
}

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        self.data.insert(k, v);
    }

    pub fn remove(&mut self, k: &[u8]) {
        self.data.remove(k);
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

    /// Change loaded dirty IndexTable into unloaded dirty by retaining dirty keys and tombstones
    pub fn make_dirty(&mut self, mut dirty_keys: HashSet<Bytes>) {
        // todo this method can be optimized if dirty_keys are made sorted

        // only retain keys that are dirty, removing all clean keys
        self.data.retain(|k, _| dirty_keys.remove(k));
        // remaining dirty_keys are not in this index, means they were deleted
        // turn them into tombstones
        for dirty_key in dirty_keys {
            self.insert(dirty_key, WalPosition::INVALID);
        }
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

    pub fn len(&self) -> usize {
        self.data.len()
    }
}
