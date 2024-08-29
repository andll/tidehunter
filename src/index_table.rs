use crate::wal::WalPosition;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};

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

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        let pos = self.data.binary_search_by_key(&k, |(k, _v)| &k[..]).ok()?;
        Some(self.data.get(pos).unwrap().1)
    }

    pub fn clear(&mut self) {
        self.data = Default::default();
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}
