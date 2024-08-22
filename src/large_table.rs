use std::time::{Duration, Instant};
use crate::wal::WalPosition;
use minibytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

pub struct LargeTable {
    data: Box<[Mutex<LargeTableEntry>]>,
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Version(pub u64);

pub struct LargeTableEntry {
    data: IndexTable,
    last_added_position: Option<WalPosition>,
    state: LargeTableEntryState,
    last_accessed: Instant,
}

#[derive(PartialEq)]
enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    Dirty(Version),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct IndexTable {
    data: Vec<(Bytes, WalPosition)>,
}

pub(crate) struct LargeTableSnapshot {
    data: Box<[LargeTableSnapshotEntry]>,
    last_added_position: Option<WalPosition>,
}

pub(crate) enum LargeTableSnapshotEntry {
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

    pub fn insert<L: Loader>(&self, k: Bytes, v: WalPosition, loader: L) -> Result<(), L::Error> {
        let entry = self.entry(&k);
        let mut entry = entry.lock();
        entry.maybe_load(loader)?;
        entry.insert(k, v);
        Ok(())
    }

    pub fn get<L: Loader>(&self, k: &[u8], loader: L) -> Result<Option<WalPosition>, L::Error> {
        let entry = self.entry(k);
        let mut entry = entry.lock();
        entry.maybe_load(loader)?;
        Ok(entry.get(k))
    }

    fn entry(&self, k: &[u8]) -> &Mutex<LargeTableEntry> {
        assert!(k.len() >= 4);
        let mut p = [0u8; 4];
        p.copy_from_slice(&k[..4]);
        let pos = u32::from_le_bytes(p) as usize;
        &self.data[pos % self.data.len()]
    }

    /// Provides a snapshot of this large table.
    /// Takes &mut reference to ensure consistency of last_added_position.
    pub fn snapshot(&mut self) -> LargeTableSnapshot {
        let mut data = Vec::with_capacity(self.data.len());
        let mut last_added_position = None;
        for entry in &self.data {
            let entry = entry.lock();
            let snapshot = entry.snapshot();
            LargeTableSnapshot::update_last_added_position(
                &mut last_added_position,
                entry.last_added_position,
            );
            data.push(snapshot);
        }
        let data = data.into_boxed_slice();
        LargeTableSnapshot {
            data,
            last_added_position,
        }
    }

    /// Update dirty entries to 'Loaded', if they have not changed since the time snapshot was taken
    pub fn maybe_update_entries(&self, updates: Vec<(usize, Version, WalPosition)>) {
        for (index, version, position) in updates {
            let mut entry = self.data[index].lock();
            entry.maybe_set_to_loaded(version, position);
        }
    }

    pub fn unload_clean(&self, max_last_accessed: Instant) {
        for entry in &self.data {
            entry.lock().unload_clean(max_last_accessed);
        }
    }
}

pub trait Loader {
    type Error;

    fn load(self, position: WalPosition) -> Result<IndexTable, Self::Error>;
}

impl LargeTableEntry {
    pub fn new_unloaded(position: WalPosition) -> Self {
        Self {
            data: IndexTable { data: vec![] },
            last_added_position: None,
            state: LargeTableEntryState::Unloaded(position),
            last_accessed: Instant::now(),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            data: IndexTable { data: vec![] },
            last_added_position: None,
            state: LargeTableEntryState::Empty,
            last_accessed: Instant::now(),
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
        self.last_added_position = Some(v);
        self.last_accessed = Instant::now();
    }

    pub fn get(&mut self, k: &[u8]) -> Option<WalPosition> {
        if matches!(&self.state, LargeTableEntryState::Unloaded(_)) {
            panic!("Can't get in unloaded state");
        }
        self.last_accessed = Instant::now();
        self.data.get(k)
    }

    pub fn maybe_load<L: Loader>(&mut self, loader: L) -> Result<(), L::Error> {
        let LargeTableEntryState::Unloaded(position) = self.state else {
            return Ok(());
        };
        let data = loader.load(position)?;
        self.data = data;
        self.state = LargeTableEntryState::Loaded(position);
        Ok(())
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

    pub fn maybe_set_to_loaded(&mut self, version: Version, position: WalPosition) {
        if self.state == LargeTableEntryState::Dirty(version) {
            self.state = LargeTableEntryState::Loaded(position)
        }
    }

    pub fn unload_clean(&mut self, max_last_accessed: Instant) {
        if let LargeTableEntryState::Loaded(position) = self.state {
            if self.last_accessed > max_last_accessed {
                return;
            }
            self.data.data.clear();
            self.state = LargeTableEntryState::Unloaded(position);
        }
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

impl LargeTableSnapshot {
    pub fn into_entries(self) -> Box<[LargeTableSnapshotEntry]> {
        self.data
    }

    pub fn last_added_position(&self) -> Option<WalPosition> {
        self.last_added_position
    }

    fn update_last_added_position(u: &mut Option<WalPosition>, v: Option<WalPosition>) {
        let Some(v) = v else {
            return;
        };
        if let Some(u) = u {
            if v > *u {
                *u = v;
            }
        } else {
            *u = Some(v);
        }
    }
}

impl Version {
    pub const ZERO: Version = Version(0);
    pub const LENGTH: usize = 8;

    pub fn increment(&mut self) {
        self.0 += 1;
    }
}
