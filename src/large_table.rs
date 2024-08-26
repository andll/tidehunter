use crate::config::Config;
use crate::primitives::lru::Lru;
use crate::primitives::sharded_mutex::ShardedMutex;
use crate::wal::WalPosition;
use minibytes::Bytes;
use parking_lot::{MappedMutexGuard, MutexGuard};
use serde::{Deserialize, Serialize};
use std::iter::repeat_with;
use std::sync::Arc;

pub struct LargeTable {
    data: Box<ShardedMutex<Row, LARGE_TABLE_MUTEXES>>,
    config: Arc<Config>,
}

const LARGE_TABLE_MUTEXES: usize = 1024;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Version(pub u64);

pub struct LargeTableEntry {
    data: IndexTable,
    last_added_position: Option<WalPosition>,
    state: LargeTableEntryState,
}

#[derive(PartialEq)]
enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    Dirty(Version),
}

struct Row {
    lru: Lru,
    data: Box<[LargeTableEntry]>,
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
    pub fn new(config: Arc<Config>) -> Self {
        let it = repeat_with(|| LargeTableEntry::new_empty()).take(config.large_table_size());
        Self::from_iterator_size(it, config)
    }

    pub fn from_unloaded(snapshot: &[WalPosition], config: Arc<Config>) -> Self {
        let size = snapshot.len();
        assert_eq!(
            size,
            config.large_table_size(),
            "Configured large table size does not match loaded snapshot size"
        );
        let it = snapshot
            .into_iter()
            .map(LargeTableEntry::from_snapshot_position);
        Self::from_iterator_size(it, config)
    }

    fn from_iterator_size(
        mut it: impl Iterator<Item = LargeTableEntry>,
        config: Arc<Config>,
    ) -> Self {
        let size = config.large_table_size();
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
        assert!(
            config.max_loaded() > 1,
            "max_loaded should be greater then 1"
        );
        let per_mutex = size / LARGE_TABLE_MUTEXES;
        let mut rows = Vec::with_capacity(size);
        for _ in 0..LARGE_TABLE_MUTEXES {
            let mut data = Vec::with_capacity(per_mutex);
            for _ in 0..per_mutex {
                data.push(it.next().expect("Iterator has less data then table size"));
            }
            let data = data.into_boxed_slice();
            let row = Row {
                data,
                lru: Lru::default(),
            };
            rows.push(row);
        }
        assert!(
            it.next().is_none(),
            "Iterator has more data then table size"
        );
        let data = Box::new(ShardedMutex::from_iterator(rows.into_iter()));
        Self { data, config }
    }

    pub fn insert<L: Loader>(&self, k: Bytes, v: WalPosition, loader: &L) -> Result<(), L::Error> {
        let mut entry = self.load_entry(&k, loader)?;
        entry.insert(k, v);
        Ok(())
    }

    pub fn remove<L: Loader>(
        &self,
        k: &[u8],
        v: WalPosition,
        loader: &L,
    ) -> Result<bool, L::Error> {
        let mut entry = self.load_entry(k, loader)?;
        Ok(entry.remove(k, v))
    }

    pub fn get<L: Loader>(&self, k: &[u8], loader: &L) -> Result<Option<WalPosition>, L::Error> {
        let entry = self.load_entry(k, loader)?;
        Ok(entry.get(k))
    }

    fn load_entry<L: Loader>(
        &self,
        k: &[u8],
        loader: &L,
    ) -> Result<MappedMutexGuard<'_, LargeTableEntry>, L::Error> {
        assert!(k.len() >= 4);
        let mut p = [0u8; 4];
        p.copy_from_slice(&k[..4]);
        let pos = u32::from_le_bytes(p) as usize;
        let pos = pos % self.config.large_table_size();
        let (mutex, offset) = Self::locate(pos);
        let mut row = self.data.lock(mutex);
        row.lru.insert(offset as u64);
        if loader.unload_supported() && row.lru.len() > self.config.max_loaded() {
            // todo - try to unload Loaded entry even if unload is not supported
            let unload = row.lru.pop().expect("Lru is not empty");
            assert_ne!(
                unload, offset as u64,
                "Attempting unload entry we are just trying to load"
            );
            // todo - we can try different approaches,
            // for example prioritize unloading Loaded entries over Dirty entries
            row.data[unload as usize].unload(loader)?;
        }
        let mut entry = MutexGuard::map(row, |l| &mut l.data[offset]);
        entry.maybe_load(loader)?;
        Ok(entry)
    }

    /// Provides a snapshot of this large table.
    /// Takes &mut reference to ensure consistency of last_added_position.
    pub fn snapshot(&mut self) -> LargeTableSnapshot {
        let mut data = Vec::with_capacity(self.config.large_table_size());
        let mut last_added_position = None;
        for mutex in self.data.as_ref().as_ref() {
            let lock = mutex.lock();
            for entry in lock.data.iter() {
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
            let entry = &mut lock.data[offset];
            entry.maybe_set_to_loaded(version, position);
        }
    }

    fn locate(pos: usize) -> (usize, usize) {
        let mutex = pos % LARGE_TABLE_MUTEXES;
        let offset = pos / LARGE_TABLE_MUTEXES;
        (mutex, offset)
    }
}

pub trait Loader {
    type Error;

    fn load(&self, position: WalPosition) -> Result<IndexTable, Self::Error>;

    fn unload_supported(&self) -> bool;

    fn unload(&self, data: &IndexTable) -> Result<WalPosition, Self::Error>;
}

impl LargeTableEntry {
    pub fn new_unloaded(position: WalPosition) -> Self {
        Self {
            data: IndexTable { data: vec![] },
            last_added_position: None,
            state: LargeTableEntryState::Unloaded(position),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            data: IndexTable { data: vec![] },
            last_added_position: None,
            state: LargeTableEntryState::Empty,
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

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        if matches!(&self.state, LargeTableEntryState::Unloaded(_)) {
            panic!("Can't get in unloaded state");
        }
        self.data.get(k)
    }

    pub fn maybe_load<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
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

    pub fn unload<L: Loader>(&mut self, _loader: &L) -> Result<(), L::Error> {
        // if let LargeTableEntryState::Loaded(position) = self.state {
        //     self.state = LargeTableEntryState::Unloaded(position);
        //     self.data.clear();
        // } else if let LargeTableEntryState::Dirty(_) = self.state {
        //     let position = loader.unload(&self.data)?;
        //     // todo trigger re-index to cap memory during restart?
        //     self.state = LargeTableEntryState::Unloaded(position);
        //     self.data.clear();
        // }
        Ok(())
    }

    fn prepare_for_mutation(&mut self) {
        match &mut self.state {
            LargeTableEntryState::Empty => self.state = LargeTableEntryState::Dirty(Version::ZERO),
            LargeTableEntryState::Loaded(_) => {
                self.state = LargeTableEntryState::Dirty(Version::ZERO)
            }
            LargeTableEntryState::Dirty(version) => version.wrapping_increment(),
            LargeTableEntryState::Unloaded(_) => {
                panic!("Mutation is not allowed on the Unloaded entry")
            }
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

    pub fn clear(&mut self) {
        self.data = Vec::new();
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

    pub fn wrapping_increment(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }

    pub fn checked_increment(&mut self) {
        self.0 = self
            .0
            .checked_add(1)
            .expect("Can not increment id: too large");
    }
}
