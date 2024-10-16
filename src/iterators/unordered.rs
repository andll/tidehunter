use crate::db::{Db, DbResult};
use minibytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

pub struct UnorderedIterator {
    db: Arc<Db>,
    cell_range: Range<usize>,
    next_cell: Option<usize>,
    next_key: Option<Bytes>,
}

impl UnorderedIterator {
    pub(crate) fn new(db: Arc<Db>, cell_range: Range<usize>) -> Self {
        Self {
            db,
            next_cell: Some(cell_range.start),
            next_key: None,
            cell_range,
        }
    }
}

impl Iterator for UnorderedIterator {
    type Item = DbResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<DbResult<(Bytes, Bytes)>> {
        let Some(next_cell) = self.next_cell else {
            return None;
        };
        if next_cell >= self.cell_range.end {
            return None;
        }
        match self.db.next_entry(next_cell, self.next_key.take(), true) {
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
