use crate::db::{Db, DbResult};
use minibytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

pub struct RangeOrderedIterator {
    db: Arc<Db>,
    cell: usize,
    next_key: Option<Bytes>,
    end_excluded: Bytes,
}

impl RangeOrderedIterator {
    pub(crate) fn new(db: Arc<Db>, cell: usize, range: Range<Bytes>) -> Self {
        // db checks that range ends fit the same large table entry
        Self {
            db,
            cell,
            next_key: Some(range.start),
            end_excluded: range.end,
        }
    }
}

impl Iterator for RangeOrderedIterator {
    type Item = DbResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<DbResult<(Bytes, Bytes)>> {
        let next_key = self.next_key.take()?;
        match self.db.next_entry(self.cell, Some(next_key)) {
            Ok(Some((next_cell, next_key, key, value))) => {
                if next_cell == Some(self.cell) {
                    self.next_key = if let Some(next_key) = next_key {
                        if next_key >= self.end_excluded {
                            None
                        } else {
                            Some(next_key)
                        }
                    } else {
                        None
                    };
                }
                Some(Ok((key, value)))
            }
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}
