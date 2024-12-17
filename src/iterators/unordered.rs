use crate::db::{Db, DbResult};
use crate::key_shape::KeySpace;
use minibytes::Bytes;
use std::sync::Arc;

pub struct UnorderedIterator {
    db: Arc<Db>,
    ks: KeySpace,
    next_cell: Option<usize>,
    next_key: Option<Bytes>,
}

impl UnorderedIterator {
    pub(crate) fn new(db: Arc<Db>, ks: KeySpace) -> Self {
        Self {
            db,
            ks,
            next_cell: Some(0),
            next_key: None,
        }
    }
}

impl Iterator for UnorderedIterator {
    type Item = DbResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<DbResult<(Bytes, Bytes)>> {
        let Some(next_cell) = self.next_cell else {
            return None;
        };
        match self
            .db
            .next_entry(self.ks, next_cell, self.next_key.take(), None)
        {
            Ok(Some((next_cell, next_key, key, value))) => {
                self.next_cell = next_cell;
                self.next_key = next_key;
                Some(Ok((key, value)))
            }
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
