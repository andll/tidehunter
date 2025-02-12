use crate::db::{Db, DbResult};
use crate::key_shape::KeySpace;
use minibytes::Bytes;
use std::sync::Arc;

pub struct DbIterator {
    db: Arc<Db>,
    ks: KeySpace,
    next_cell: Option<usize>,
    next_key: Option<Bytes>,
    lower_bound: Option<Bytes>,
    upper_bound: Option<Bytes>,
    end_cell_exclusive: Option<usize>,
    reverse: bool,
}

impl DbIterator {
    pub(crate) fn new(db: Arc<Db>, ks: KeySpace) -> Self {
        Self {
            db,
            ks,
            next_cell: Some(0),
            next_key: None,
            lower_bound: None,
            upper_bound: None,
            end_cell_exclusive: None,
            reverse: false,
        }
    }

    /// Set lower bound(inclusive) for the iterator.
    /// Updating boundaries may reset the iterator.
    pub fn set_lower_bound(&mut self, lower_bound: impl Into<Bytes>) {
        let lower_bound = lower_bound.into();
        let ks = self.db.ks(self.ks);
        if self.reverse {
            self.end_cell_exclusive = ks.next_cell(ks.cell_for_key(&lower_bound), true);
        } else {
            self.next_cell = Some(ks.cell_for_key(&lower_bound));
            self.next_key = Some(lower_bound.clone());
        }
        self.lower_bound = Some(lower_bound);
    }

    /// Set upper bound(exclusive) for the iterator.
    /// Updating boundaries may reset the iterator.
    pub fn set_upper_bound(&mut self, upper_bound: impl Into<Bytes>) {
        let upper_bound = upper_bound.into();
        let ks = self.db.ks(self.ks);
        if self.reverse {
            let next_key = saturated_decrement_vec(&upper_bound);
            self.next_cell = Some(ks.cell_for_key(&next_key));
            self.next_key = Some(next_key);
        } else {
            self.end_cell_exclusive = ks.next_cell(ks.cell_for_key(&upper_bound), false);
        }
        self.upper_bound = Some(upper_bound);
    }

    pub fn reverse(&mut self) {
        self.reverse = !self.reverse;
        if let Some(lower_bound) = self.lower_bound.take() {
            self.set_lower_bound(lower_bound);
        }
        if let Some(upper_bound) = self.upper_bound.take() {
            self.set_upper_bound(upper_bound);
        }
    }

    fn exceeds_end_bound(&self, key: &Bytes) -> bool {
        if self.reverse {
            if let Some(lower_bound) = &self.lower_bound {
                key < lower_bound
            } else {
                false
            }
        } else {
            if let Some(upper_bound) = &self.upper_bound {
                key >= upper_bound
            } else {
                false
            }
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
            if self.exceeds_end_bound(&next_key) {
                return None;
            }
        }
        match self.db.next_entry(
            self.ks,
            next_cell,
            next_key,
            self.end_cell_exclusive,
            self.reverse,
        ) {
            Ok(Some((next_cell, next_key, key, value))) => {
                if self.exceeds_end_bound(&key) {
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

fn saturated_decrement_vec(original_bytes: &[u8]) -> Bytes {
    let mut bytes = original_bytes.to_vec();
    for v in bytes.iter_mut().rev() {
        let sub = v.checked_sub(1);
        if let Some(sub) = sub {
            *v = sub;
            return bytes.into();
        } else {
            *v = 255;
        }
    }
    original_bytes.to_vec().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_saturated_decrement_vec() {
        assert_eq!(saturated_decrement_vec(&[1, 2, 3]).as_ref(), &[1, 2, 2]);
        assert_eq!(saturated_decrement_vec(&[1, 2, 0]).as_ref(), &[1, 1, 255]);
        assert_eq!(saturated_decrement_vec(&[1, 0, 0]).as_ref(), &[0, 255, 255]);
        assert_eq!(saturated_decrement_vec(&[0, 0, 0]).as_ref(), &[0, 0, 0]);
    }
}
