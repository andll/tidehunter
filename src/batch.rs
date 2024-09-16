use crate::db::{WalEntry, MAX_KEY_LEN};
use crate::wal::PreparedWalWrite;
use minibytes::Bytes;

pub struct WriteBatch {
    pub(crate) writes: Vec<(Bytes, PreparedWalWrite)>,
    pub(crate) deletes: Vec<(Bytes, PreparedWalWrite)>,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            writes: Default::default(),
            deletes: Default::default(),
        }
    }

    pub fn write(&mut self, k: impl Into<Bytes>, v: impl Into<Bytes>) {
        let k = k.into();
        let v = v.into();
        assert!(k.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let w = PreparedWalWrite::new(&WalEntry::Record(k.clone(), v));
        self.writes.push((k, w))
    }

    pub fn delete(&mut self, k: impl Into<Bytes>) {
        let k = k.into();
        assert!(k.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let w = PreparedWalWrite::new(&WalEntry::Remove(k.clone()));
        self.deletes.push((k, w))
    }
}
