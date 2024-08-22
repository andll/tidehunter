use crate::db::{WalEntry, MAX_KEY_LEN};
use crate::wal::PreparedWalWrite;
use minibytes::Bytes;

pub struct WriteBatch {
    writes: Vec<(Bytes, PreparedWalWrite)>,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            writes: Default::default(),
        }
    }

    pub fn write(&mut self, k: impl Into<Bytes>, v: impl Into<Bytes>) {
        let k = k.into();
        let v = v.into();
        assert!(k.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let w = PreparedWalWrite::new(&WalEntry::Record(k.clone(), v));
        self.writes.push((k, w))
    }

    pub(crate) fn into_writes(self) -> Vec<(Bytes, PreparedWalWrite)> {
        self.writes
    }
}
