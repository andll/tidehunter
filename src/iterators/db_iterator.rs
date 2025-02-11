use crate::db::{Db, DbResult};
use crate::key_shape::KeySpace;
use minibytes::Bytes;
use std::sync::Arc;

pub struct DbIterator {
    db: Arc<Db>,
    ks: KeySpace,
    next_cell: Option<usize>,
    next_key: Option<Bytes>,
    upper_bound: Option<Bytes>,
    max_cell_exclusive: Option<usize>,
}

impl DbIterator {
    pub(crate) fn new(db: Arc<Db>, ks: KeySpace) -> Self {
        Self {
            db,
            ks,
            next_cell: Some(0),
            next_key: None,
            upper_bound: None,
            max_cell_exclusive: None,
        }
    }

    /// Set lower bound(inclusive) for the iterator.
    /// Updating boundaries resets the iterator.
    pub fn set_lower_bound(&mut self, lower_bound: impl Into<Bytes>) {
        let lower_bound = lower_bound.into();
        let ks = self.db.ks(self.ks);
        self.next_cell = Some(ks.cell_for_key(&lower_bound));
        self.next_key = Some(lower_bound);
    }

    /// Set upper bound(exclusive) for the iterator.
    /// Updating boundaries may reset the iterator.
    pub fn set_upper_bound(&mut self, upper_bound: impl Into<Bytes>) {
        let upper_bound = upper_bound.into();
        let ks = self.db.ks(self.ks);
        self.max_cell_exclusive = ks.next_cell(ks.cell_for_key(&upper_bound));
        self.upper_bound = Some(upper_bound);
    }

    fn exceeds_upper_bound(&self, key: &Bytes) -> bool {
        if let Some(upper_bound) = &self.upper_bound {
            key >= upper_bound
        } else {
            false
        }
    }
}

impl Iterator for DbIterator {
    type Item = DbResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<DbResult<(Bytes, Bytes)>> {
        let Some(next_cell) = self.next_cell else {
            return None;
        };
        let next_key = self.next_key.take();
        if let Some(next_key) = &next_key {
            if self.exceeds_upper_bound(&next_key) {
                return None;
            }
        }
        match self
            .db
            .next_entry(self.ks, next_cell, next_key, self.max_cell_exclusive)
        {
            Ok(Some((next_cell, next_key, key, value))) => {
                if self.exceeds_upper_bound(&key) {
                    // todo pass it to Db and do not fetch the value in this case
                    return None;
                }
                self.next_cell = next_cell;
                self.next_key = next_key;
                Some(Ok((key, value)))
            }
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
