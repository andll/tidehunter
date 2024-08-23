use std::collections::{BTreeMap, HashMap};

/// Allows inserting values and pop oldest inserted value.
///
/// This is almost like a binary heap, but inserting value again
/// pushes the value to the end of the queue.
#[derive(Default)]
pub struct Lru {
    pos: u64,
    map: HashMap<u64, u64>,
    rmap: BTreeMap<u64, u64>,
}

impl Lru {
    pub fn insert(&mut self, id: u64) {
        let prev_pos = self.map.insert(id, self.pos);
        if let Some(prev_pos) = prev_pos {
            self.rmap.remove(&prev_pos);
        }
        self.rmap.insert(self.pos, id);
        debug_assert_eq!(self.map.len(), self.rmap.len());
        self.pos += 1;
    }

    pub fn pop(&mut self) -> Option<u64> {
        let (pos, id) = self.rmap.pop_first()?;
        let map_pos = self.map.remove(&id);
        debug_assert_eq!(Some(pos), map_pos);
        Some(id)
    }
}

#[test]
fn test_lru() {
    let mut lru = Lru::default();
    lru.insert(1);
    lru.insert(2);
    lru.insert(3);
    lru.insert(1);
    assert_eq!(lru.pop(), Some(2));
    assert_eq!(lru.pop(), Some(3));
    assert_eq!(lru.pop(), Some(1));
    assert_eq!(lru.pop(), None);
}
