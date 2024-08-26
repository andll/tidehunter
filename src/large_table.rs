use crate::primitives::sharded_mutex::ShardedMutex;
use crate::wal::WalPosition;
use minibytes::Bytes;
use parking_lot::MutexGuard;
use serde::{Deserialize, Serialize};
use std::iter::repeat_with;
use std::time::Instant;

pub struct LargeTable {
    data: ShardedMutex<Box<[LargeTableEntry]>, LARGE_TABLE_MUTEXES>,
    size: usize,
}

const LARGE_TABLE_MUTEXES: usize = 1024;

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

macro_rules! entry {
    ($self:expr, $k:expr, $entry:ident) => {
        let (mut lock, offset) = $self.entry($k);
        let $entry = &mut lock[offset];
    };
}

impl LargeTable {
    pub fn new(size: usize) -> Self {
        let it = repeat_with(|| LargeTableEntry::new_empty()).take(size);
        Self::from_iterator_size(it, size)
    }

    pub fn from_unloaded(snapshot: &[WalPosition]) -> Self {
        let size = snapshot.len();
        let it = snapshot
            .into_iter()
            .map(LargeTableEntry::from_snapshot_position);
        Self::from_iterator_size(it, size)
    }

    fn from_iterator_size(mut it: impl Iterator<Item = LargeTableEntry>, size: usize) -> Self {
        assert!(size <= u32::MAX as usize);
        assert!(
            size >= LARGE_TABLE_MUTEXES,
            "Large table size should be at least LARGE_TABLE_MUTEXES"
        );
        assert_eq!(
            size % LARGE_TABLE_MUTEXES,
            0,
            "Large table size should be dividable by LARGE_TABLE_MUTEXES"
        );
        let per_mutex = size / LARGE_TABLE_MUTEXES;
        let mut rows = Vec::with_capacity(size);
        for _ in 0..LARGE_TABLE_MUTEXES {
            let mut data = Vec::with_capacity(per_mutex);
            for _ in 0..per_mutex {
                data.push(it.next().expect("Iterator has less data then table size"));
            }
            rows.push(data.into_boxed_slice());
        }
        assert!(
            it.next().is_none(),
            "Iterator has more data then table size"
        );
        let data = ShardedMutex::from_iterator(rows.into_iter());
        Self { data, size }
    }

    pub fn insert<L: Loader>(&self, k: Bytes, v: WalPosition, loader: L) -> Result<(), L::Error> {
        entry!(self, &k, entry);
        entry.maybe_load(loader)?;
        entry.insert(k, v);
        Ok(())
    }

    pub fn remove<L: Loader>(&self, k: &[u8], v: WalPosition, loader: L) -> Result<bool, L::Error> {
        entry!(self, &k, entry);
        entry.maybe_load(loader)?;
        Ok(entry.remove(k, v))
    }

    pub fn get<L: Loader>(&self, k: &[u8], loader: L) -> Result<Option<WalPosition>, L::Error> {
        entry!(self, &k, entry);
        entry.maybe_load(loader)?;
        Ok(entry.get(k))
    }

    fn entry(&self, k: &[u8]) -> (MutexGuard<'_, Box<[LargeTableEntry]>>, usize) {
        assert!(k.len() >= 4);
        let mut p = [0u8; 4];
        p.copy_from_slice(&k[..4]);
        let pos = u32::from_le_bytes(p) as usize;
        let pos = pos % self.size;
        let (mutex, offset) = Self::locate(pos);
        (self.data.lock(mutex), offset)
    }

    /// Provides a snapshot of this large table.
    /// Takes &mut reference to ensure consistency of last_added_position.
    pub fn snapshot(&mut self) -> LargeTableSnapshot {
        let mut data = Vec::with_capacity(self.size);
        let mut last_added_position = None;
        for mutex in self.data.as_ref() {
            let lock = mutex.lock();
            for entry in lock.iter() {
                let snapshot = entry.snapshot();
                LargeTableSnapshot::update_last_added_position(
                    &mut last_added_position,
                    entry.last_added_position,
                );
                data.push(snapshot);
            }
        }
        let data = data.into_boxed_slice();
        LargeTableSnapshot {
            data,
            last_added_position,
        }
    }

    /// Update dirty entries to 'Loaded', if they have not changed since the time snapshot was taken.
    pub fn maybe_update_entries(&self, updates: Vec<(usize, Version, WalPosition)>) {
        // todo - it is possible to optimize this to minimize number of mutex locks
        for (index, version, position) in updates {
            let (mutex, offset) = Self::locate(index);
            let mut lock = self.data.lock(mutex);
            let entry = &mut lock[offset];
            entry.maybe_set_to_loaded(version, position);
        }
    }

    fn locate(pos: usize) -> (usize, usize) {
        let mutex = pos % LARGE_TABLE_MUTEXES;
        let offset = pos / LARGE_TABLE_MUTEXES;
        (mutex, offset)
    }

    // todo cleanup
    // pub fn unload_clean(&self, max_last_accessed: Instant) {
    //     for entry in &self.data {
    //         entry.lock().unload_clean(max_last_accessed);
    //     }
    // }
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

    pub fn from_snapshot_position(position: &WalPosition) -> Self {
        if position == &WalPosition::INVALID {
            LargeTableEntry::new_empty()
        } else {
            LargeTableEntry::new_unloaded(*position)
        }
    }

    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        self.prepare_for_mutation();
        self.data.insert(k, v);
        self.last_added_position = Some(v);
    }

    pub fn remove(&mut self, k: &[u8], v: WalPosition) -> bool {
        self.prepare_for_mutation();
        let result = self.data.remove(k);
        self.last_added_position = Some(v);
        result
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

    fn prepare_for_mutation(&mut self) {
        match &mut self.state {
            LargeTableEntryState::Empty => self.state = LargeTableEntryState::Dirty(Version::ZERO),
            LargeTableEntryState::Loaded(_) => {
                self.state = LargeTableEntryState::Dirty(Version::ZERO)
            }
            LargeTableEntryState::Dirty(version) => version.increment(),
            LargeTableEntryState::Unloaded(_) => {
                panic!("Mutation is not allowed on the Unloaded entry")
            }
        }
        self.last_accessed = Instant::now();
    }
}

impl IndexTable {
    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        match self.data.binary_search_by_key(&&k[..], |(k, _v)| &k[..]) {
            Ok(found) => self.data[found] = (k, v),
            Err(insert) => self.data.insert(insert, (k, v)),
        }
    }

    pub fn remove(&mut self, k: &[u8]) -> bool {
        match self.data.binary_search_by_key(&k, |(k, _v)| &k[..]) {
            Ok(found) => {
                self.data.remove(found);
                true
            }
            Err(_) => false,
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
