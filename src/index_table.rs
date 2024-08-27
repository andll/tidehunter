use crate::primitives::arc_cow::ArcCow;
use crate::wal::WalPosition;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct IndexTable {
    data: ArcCow<Vec<(Bytes, WalPosition)>>,
}

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        let data = self.data.make_mut();
        match data.binary_search_by_key(&&k[..], |(k, _v)| &k[..]) {
            Ok(found) => data[found] = (k, v),
            Err(insert) => data.insert(insert, (k, v)),
        }
    }

    pub fn remove(&mut self, k: &[u8]) -> bool {
        match self.data.binary_search_by_key(&k, |(k, _v)| &k[..]) {
            Ok(found) => {
                self.data.make_mut().remove(found);
                true
            }
            Err(_) => false,
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

    pub fn clone_shared(&mut self) -> Self {
        Self {
            data: self.data.clone_shared(),
        }
    }
}
