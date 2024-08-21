use crate::wal::WalPosition;
use minibytes::Bytes;
use parking_lot::Mutex;

pub struct LargeTable {
    data: Box<[Mutex<LargeTableEntry>]>,
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Version(pub u64);

pub struct LargeTableEntry {
    data: IndexTable,
    state: LargeTableEntryState,
}

enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    Dirty(Version),
}

#[derive(Clone)]
struct IndexTable {
    data: Vec<(Bytes, WalPosition)>,
}

pub struct LargeTableSnapshot {
    data: Box<[LargeTableSnapshotEntry]>,
}

enum LargeTableSnapshotEntry {
    Empty,
    Clean(WalPosition),
    Dirty(Version, IndexTable),
}

impl LargeTable {
    pub fn new(size: usize) -> Self {
        assert!(size <= u32::MAX as usize);
        assert!(size >= 2);
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(Mutex::new(LargeTableEntry::new_empty()));
        }
        let data = data.into_boxed_slice();
        Self { data }
    }

    pub fn from_unloaded(snapshot: &[WalPosition]) -> Self {
        let data = snapshot
            .iter()
            .map(|p| {
                let e = if p == &WalPosition::INVALID {
                    LargeTableEntry::new_empty()
                } else {
                    LargeTableEntry::new_unloaded(*p)
                };
                Mutex::new(e)
            })
            .collect();
        Self { data }
    }

    pub fn insert(&self, k: Bytes, v: WalPosition) {
        let entry = self.entry(&k);
        entry.lock().insert(k, v);
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        let entry = self.entry(k);
        entry.lock().get(k)
    }

    fn entry(&self, k: &[u8]) -> &Mutex<LargeTableEntry> {
        assert!(k.len() >= 4);
        let mut p = [0u8; 4];
        p.copy_from_slice(&k[..4]);
        let pos = u32::from_le_bytes(p) as usize;
        &self.data[pos % self.data.len()]
    }

    pub fn snapshot(&self) -> LargeTableSnapshot {
        let mut data = Vec::with_capacity(self.data.len());
        for entry in &self.data {
            let entry = entry.lock().snapshot();
            data.push(entry);
        }
        let data = data.into_boxed_slice();
        LargeTableSnapshot { data }
    }
}

impl LargeTableEntry {
    pub fn new_unloaded(position: WalPosition) -> Self {
        Self {
            data: IndexTable { data: vec![] },
            state: LargeTableEntryState::Unloaded(position),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            data: IndexTable { data: vec![] },
            state: LargeTableEntryState::Empty,
        }
    }

    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        match &mut self.state {
            LargeTableEntryState::Empty => self.state = LargeTableEntryState::Dirty(Version::ZERO),
            LargeTableEntryState::Loaded(_) => {
                self.state = LargeTableEntryState::Dirty(Version::ZERO)
            }
            LargeTableEntryState::Dirty(version) => version.increment(),
            LargeTableEntryState::Unloaded(_) => {
                panic!("Insert is not allowed on the Unloaded entry")
            }
        }
        self.data.insert(k, v);
    }

    pub fn snapshot(&self) -> LargeTableSnapshotEntry {
        match self.state {
            LargeTableEntryState::Empty => LargeTableSnapshotEntry::Empty,
            LargeTableEntryState::Unloaded(pos) => LargeTableSnapshotEntry::Clean(pos),
            LargeTableEntryState::Loaded(pos) => LargeTableSnapshotEntry::Clean(pos),
            LargeTableEntryState::Dirty(version) => {
                LargeTableSnapshotEntry::Dirty(version, self.data.clone())
            }
        }
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        if matches!(&self.state, LargeTableEntryState::Unloaded(_)) {
            panic!("Can't get in unloaded state");
        }
        self.data.get(k)
    }
}

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        match self.data.binary_search_by_key(&&k[..], |(k, _v)| &k[..]) {
            Ok(found) => self.data[found] = (k, v),
            Err(insert) => self.data.insert(insert, (k, v)),
        }
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        let pos = self.data.binary_search_by_key(&k, |(k, _v)| &k[..]).ok()?;
        Some(self.data.get(pos).unwrap().1)
    }
}

impl Version {
    pub const ZERO: Version = Version(0);
    pub const LENGTH: usize = 8;

    pub fn increment(&mut self) {
        self.0 += 1;
    }
}
