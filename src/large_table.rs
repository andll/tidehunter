use crate::config::Config;
use crate::index_table::IndexTable;
use crate::metrics::Metrics;
use crate::primitives::arc_cow::ArcCow;
use crate::primitives::lru::Lru;
use crate::primitives::sharded_mutex::ShardedMutex;
use crate::wal::WalPosition;
use minibytes::Bytes;
use parking_lot::{MappedMutexGuard, MutexGuard};
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct LargeTable {
    data: Box<ShardedMutex<Row, LARGE_TABLE_MUTEXES>>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
}

const LARGE_TABLE_MUTEXES: usize = 1024;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Version(pub u64);

pub struct LargeTableEntry {
    dirty_keys: HashSet<Bytes>,
    data: ArcCow<IndexTable>,
    last_added_position: Option<WalPosition>,
    state: LargeTableEntryState,
}

#[derive(PartialEq)]
enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    DirtyUnloaded(WalPosition, Version),
    DirtyLoaded(Version),
}

struct Row {
    lru: Lru,
    data: Box<[LargeTableEntry]>,
}

pub(crate) struct LargeTableSnapshot {
    data: Box<[LargeTableSnapshotEntry]>,
    last_added_position: Option<WalPosition>,
}

pub(crate) enum LargeTableSnapshotEntry {
    Empty,
    Clean(WalPosition),
    Dirty(Version, Arc<IndexTable>),
    DirtyUnloaded(WalPosition, Version, Arc<IndexTable>),
}

impl LargeTable {
    pub fn from_unloaded(
        snapshot: &[WalPosition],
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let size = snapshot.len();
        assert_eq!(
            size,
            config.large_table_size(),
            "Configured large table size does not match loaded snapshot size"
        );
        let it = snapshot
            .into_iter()
            .map(LargeTableEntry::from_snapshot_position);
        Self::from_iterator_size(it, config, metrics)
    }

    fn from_iterator_size(
        mut it: impl Iterator<Item = LargeTableEntry>,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
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
        Self {
            data,
            config,
            metrics,
        }
    }

    pub fn insert<L: Loader>(&self, k: Bytes, v: WalPosition, _loader: &L) -> Result<(), L::Error> {
        let mut entry = self.entry(&k);
        entry.insert(k, v);
        let index_size = entry.data.len();
        self.metrics
            .max_index_size
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                if index_size > old {
                    Some(index_size)
                } else {
                    None
                }
            })
            .ok();
        Ok(())
    }

    pub fn remove<L: Loader>(&self, k: Bytes, v: WalPosition, _loader: &L) -> Result<(), L::Error> {
        let mut entry = self.entry(&k);
        Ok(entry.remove(k, v))
    }

    pub fn get<L: Loader>(&self, k: &[u8], loader: &L) -> Result<Option<WalPosition>, L::Error> {
        let (row, offset) = self.row(k);
        let entry = row.entry(offset);
        if matches!(entry.state, LargeTableEntryState::DirtyUnloaded(_, _)) {
            // optimization: in dirty unloaded state we might not need to load entry
            if let Some(found) = entry.get(k) {
                return Ok(found.valid());
            }
        }
        let entry = self.load_entry(row, offset, loader)?;
        Ok(entry.get(k))
    }

    fn entry(&self, k: &[u8]) -> MappedMutexGuard<'_, LargeTableEntry> {
        let (row, offset) = self.row(k);
        MutexGuard::map(row, |l| l.entry_mut(offset))
    }

    fn load_entry<'a, L: Loader>(
        &self,
        mut row: MutexGuard<'a, Row>,
        offset: usize,
        loader: &L,
    ) -> Result<MappedMutexGuard<'a, LargeTableEntry>, L::Error> {
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
        let mut entry = MutexGuard::map(row, |l| l.entry_mut(offset));
        entry.maybe_load(loader)?;
        Ok(entry)
    }

    fn row(&self, k: &[u8]) -> (MutexGuard<'_, Row>, usize) {
        assert!(k.len() >= 4);
        let mut p = [0u8; 4];
        p.copy_from_slice(&k[..4]);
        let pos = u32::from_le_bytes(p) as usize;
        let pos = pos % self.config.large_table_size();
        let (mutex, offset) = Self::locate(pos);
        let row = self.data.lock(mutex);
        (row, offset)
    }

    /// Provides a snapshot of this large table.
    /// Takes &mut reference to ensure consistency of last_added_position.
    pub fn snapshot(&mut self) -> LargeTableSnapshot {
        let mut data = Vec::with_capacity(self.config.large_table_size());
        let mut last_added_position = None;
        for mutex in self.data.as_ref().as_ref() {
            let mut lock = mutex.lock();
            for entry in lock.data.iter_mut() {
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
            entry.maybe_set_to_clean(version, position);
        }
    }

    fn locate(pos: usize) -> (usize, usize) {
        let mutex = pos % LARGE_TABLE_MUTEXES;
        let offset = pos / LARGE_TABLE_MUTEXES;
        (mutex, offset)
    }
}

impl Row {
    pub fn entry(&self, offset: usize) -> &LargeTableEntry {
        &self.data[offset]
    }
    pub fn entry_mut(&mut self, offset: usize) -> &mut LargeTableEntry {
        &mut self.data[offset]
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
            data: Default::default(),
            dirty_keys: Default::default(),
            last_added_position: None,
            state: LargeTableEntryState::Unloaded(position),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            data: Default::default(),
            dirty_keys: Default::default(),
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
        if matches!(self.state, LargeTableEntryState::DirtyUnloaded(_, _)) {
            self.dirty_keys.insert(k.clone());
        }
        self.data.make_mut().insert(k, v);
        self.last_added_position = Some(v);
    }

    pub fn remove(&mut self, k: Bytes, v: WalPosition) {
        let dirty_state = self.prepare_for_mutation();
        match dirty_state {
            DirtyState::Loaded => self.data.make_mut().remove(&k),
            DirtyState::Unloaded => {
                self.dirty_keys.insert(k.clone());
                self.data.make_mut().insert(k, WalPosition::INVALID)
            }
        }
        self.last_added_position = Some(v);
    }

    pub fn get(&self, k: &[u8]) -> Option<WalPosition> {
        if matches!(&self.state, LargeTableEntryState::Unloaded(_)) {
            panic!("Can't get in unloaded state");
        }
        self.data.get(k)
    }

    pub fn maybe_load<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
        let Some((state, position)) = self.unloaded_state() else {
            return Ok(());
        };
        let mut data = loader.load(position)?;
        match state {
            UnloadedState::Dirty => data.merge_dirty(&self.data),
            UnloadedState::Clean => {}
        };
        self.data = ArcCow::new_owned(data);
        self.state = LargeTableEntryState::Loaded(position);
        Ok(())
    }

    pub fn snapshot(&mut self) -> LargeTableSnapshotEntry {
        match self.state {
            LargeTableEntryState::Empty => LargeTableSnapshotEntry::Empty,
            LargeTableEntryState::Unloaded(pos) => LargeTableSnapshotEntry::Clean(pos),
            LargeTableEntryState::Loaded(pos) => LargeTableSnapshotEntry::Clean(pos),
            LargeTableEntryState::DirtyLoaded(version) => {
                LargeTableSnapshotEntry::Dirty(version, self.data.clone_shared())
            }
            LargeTableEntryState::DirtyUnloaded(pos, version) => {
                LargeTableSnapshotEntry::DirtyUnloaded(pos, version, self.data.clone_shared())
            }
        }
    }

    /// Updates dirty state to clean state if entry was not updated since the snapshot was taken
    pub fn maybe_set_to_clean(&mut self, version: Version, position: WalPosition) {
        // todo use Arc pointer comparison instead of version
        if self.state == LargeTableEntryState::DirtyLoaded(version) {
            self.state = LargeTableEntryState::Loaded(position)
        } else if matches!(
            self.state,
            LargeTableEntryState::DirtyUnloaded(_, v) if v == version
        ) {
            self.dirty_keys = Default::default();
            self.data = Default::default();
            self.state = LargeTableEntryState::Unloaded(position)
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

    fn prepare_for_mutation(&mut self) -> DirtyState {
        match &mut self.state {
            LargeTableEntryState::Empty => {
                self.state = LargeTableEntryState::DirtyLoaded(Version::ZERO)
            }
            LargeTableEntryState::Loaded(_) => {
                self.state = LargeTableEntryState::DirtyLoaded(Version::ZERO)
            }
            LargeTableEntryState::DirtyLoaded(version) => version.wrapping_increment(),
            LargeTableEntryState::Unloaded(pos) => {
                self.state = LargeTableEntryState::DirtyUnloaded(*pos, Version::ZERO)
            }
            LargeTableEntryState::DirtyUnloaded(_, version) => version.wrapping_increment(),
        }
        self.dirty_state()
            .expect("prepare_for_mutation sets state to one of dirty states")
    }

    fn dirty_state(&self) -> Option<DirtyState> {
        match self.state {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(_) => None,
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(_, _) => Some(DirtyState::Unloaded),
            LargeTableEntryState::DirtyLoaded(_) => Some(DirtyState::Loaded),
        }
    }

    fn unloaded_state(&self) -> Option<(UnloadedState, WalPosition)> {
        match self.state {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(pos) => Some((UnloadedState::Clean, pos)),
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(pos, _) => Some((UnloadedState::Dirty, pos)),
            LargeTableEntryState::DirtyLoaded(_) => None,
        }
    }
}

enum DirtyState {
    Loaded,
    Unloaded,
}

enum UnloadedState {
    Dirty,
    Clean,
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
