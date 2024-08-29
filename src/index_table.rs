use crate::wal::WalPosition;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct IndexTable {
    data: Vec<(Bytes, WalPosition)>,
}

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        match self.data.binary_search_by_key(&&k[..], |(k, _v)| &k[..]) {
            Ok(found) => self.data[found] = (k, v),
            Err(insert) => self.data.insert(insert, (k, v)),
        }
    }

    pub fn remove(&mut self, k: &[u8]) {
        match self.data.binary_search_by_key(&k, |(k, _v)| &k[..]) {
            Ok(found) => {
                self.data.remove(found);
            }
            Err(_) => {}
        }
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
        self.data.retain(|(k, _)| dirty_keys.remove(k));
        // remaining dirty_keys are not in this index, means they were deleted
        // turn them into tombstones
        for dirty_key in dirty_keys {
            self.insert(dirty_key, WalPosition::INVALID);
        }
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        let pos = self.data.binary_search_by_key(&k, |(k, _v)| &k[..]).ok()?;
        Some(self.data.get(pos).unwrap().1)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}
