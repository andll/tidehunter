use minibytes::Bytes;
use parking_lot::Mutex;

pub struct LargeTable {
    data: Box<[Mutex<LargeTableEntry>]>,
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Position(pub u64);
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Version(pub u64);

pub struct LargeTableEntry {
    data: IndexTable,
    state: LargeTableEntryState,
}

enum LargeTableEntryState {
    Empty,
    Unloaded(Position),
    Loaded(Position),
    Dirty(Version),
}

#[derive(Clone)]
struct IndexTable {
    data: Vec<(Bytes, Position)>,
}

pub struct LargeTableSnapshot {
    data: Box<[LargeTableSnapshotEntry]>,
}

enum LargeTableSnapshotEntry {
    Empty,
    Clean(Position),
    Dirty(Version, IndexTable),
}

impl LargeTable {
    const LENGTH: usize = 0x1_00_00;
    const PREFIX_SIZE: usize = 2;

    pub fn new() -> Self {
        let mut data = Vec::with_capacity(Self::LENGTH);
        for _ in 0..Self::LENGTH {
            data.push(Mutex::new(LargeTableEntry::new_empty()));
        }
        let data = data.into_boxed_slice();
        Self { data }
    }

    pub fn insert(&self, k: Bytes, v: Position) {
        let entry = self.entry(&k);
        entry.lock().insert(k, v);
    }

    pub fn get(&self, k: &[u8]) -> Option<Position> {
        let entry = self.entry(k);
        entry.lock().get(k)
    }

    fn entry(&self, k: &[u8]) -> &Mutex<LargeTableEntry> {
        assert!(k.len() >= Self::PREFIX_SIZE);
        let mut p = [0u8; Self::PREFIX_SIZE];
        p.copy_from_slice(&k[..Self::PREFIX_SIZE]);
        let pos = u16::from_le_bytes(p) as usize;
        &self.data[pos]
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
    pub fn new_empty() -> Self {
        Self {
            data: IndexTable { data: vec![] },
            state: LargeTableEntryState::Empty,
        }
    }

    pub fn insert(&mut self, k: Bytes, v: Position) {
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

    pub fn get(&self, k: &[u8]) -> Option<Position> {
        self.data.get(k)
    }
}

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: Position) {
        match self.data.binary_search_by_key(&&k[..], |(k, _v)| &k[..]) {
            Ok(found) => self.data[found] = (k, v),
            Err(insert) => self.data.insert(insert, (k, v)),
        }
    }

    pub fn get(&self, k: &[u8]) -> Option<Position> {
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

impl Position {
    pub const LENGTH: usize = 8;
}
