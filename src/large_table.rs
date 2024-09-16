use crate::config::Config;
use crate::index_table::IndexTable;
use crate::metrics::Metrics;
use crate::primitives::arc_cow::ArcCow;
use crate::primitives::lru::Lru;
use crate::primitives::sharded_mutex::ShardedMutex;
use crate::wal::WalPosition;
use minibytes::Bytes;
use parking_lot::{MappedMutexGuard, MutexGuard};
use std::cmp;
use std::collections::HashSet;
use std::ops::Range;
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

#[derive(Default)]
pub struct LargeTableEntry {
    data: ArcCow<IndexTable>,
    last_added_position: Option<WalPosition>,
    state: LargeTableEntryState,
}

enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    DirtyUnloaded(WalPosition, HashSet<Bytes>),
    DirtyLoaded(HashSet<Bytes>),
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
    Dirty(Arc<IndexTable>),
    DirtyUnloaded(WalPosition, Arc<IndexTable>),
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
        let (mut row, offset) = self.row(&k);
        let entry = row.entry_mut(offset);
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
        let (mut row, offset) = self.row(&k);
        let entry = row.entry_mut(offset);
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

    pub fn is_empty(&self) -> bool {
        self.data
            .mutexes()
            .iter()
            .all(|m| m.lock().data.iter().all(LargeTableEntry::is_empty))
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
        let cell = self.cell_by_prefix(Self::cell_prefix(k));
        let (mutex, offset) = Self::locate(cell);
        let row = self.data.lock(mutex);
        (row, offset)
    }

    fn cell_prefix(k: &[u8]) -> u32 {
        let copy = cmp::min(k.len(), 4);
        let mut p = [0u8; 4];
        p[..copy].copy_from_slice(&k[..copy]);
        u32::from_le_bytes(p)
    }

    fn cell_by_prefix(&self, prefix: u32) -> usize {
        (prefix as usize) % self.config.large_table_size()
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
    pub fn maybe_update_entries(&self, updates: Vec<(usize, Arc<IndexTable>, WalPosition)>) {
        // todo - it is possible to optimize this to minimize number of mutex locks
        for (index, data, position) in updates {
            let (mutex, offset) = Self::locate(index);
            let mut lock = self.data.lock(mutex);
            let entry = &mut lock.data[offset];
            entry.maybe_set_to_clean(&data, position);
        }
    }

    /// Takes a next entry in the large table.
    ///
    /// See Db::next_entry for documentation.
    pub fn next_entry<L: Loader>(
        &self,
        mut cell: usize,
        mut next_key: Option<Bytes>,
        loader: &L,
    ) -> Result<
        Option<(
            Option<usize>, /*next cell*/
            Option<Bytes>, /*next key*/
            Bytes,         /*fetched key*/
            WalPosition,   /*fetched value*/
        )>,
        L::Error,
    > {
        loop {
            let (row, offset) = Self::locate(cell);
            let mut row = self.data.lock(row);
            let entry = row.entry_mut(offset);
            // todo lru logic
            entry.maybe_load(loader)?;
            if let Some((key, value, next_key)) = entry.next_entry(next_key) {
                let next_cell = if next_key.is_none() {
                    self.next_cell(cell)
                } else {
                    Some(cell)
                };
                return Ok(Some((next_cell, next_key, key, value)));
            } else {
                next_key = None;
                let Some(next_cell) = self.next_cell(cell) else {
                    return Ok(None);
                };
                cell = next_cell;
            }
        }
    }

    /// Returns the cell containing the range.
    /// Right now, this only works if the entire range "fits" single cell.
    pub fn range_cell(&self, range: &Range<Bytes>) -> usize {
        let start_prefix = Self::cell_prefix(&range.start);
        let end_prefix = Self::cell_prefix(&range.end);
        if start_prefix == end_prefix {
            self.cell_by_prefix(start_prefix)
        } else {
            panic!("Can't have ordered iterator over key range that does not fit same large table cell");
        }
    }

    fn next_cell(&self, cell: usize) -> Option<usize> {
        if cell >= self.config.large_table_size() - 1 {
            None
        } else {
            Some(cell + 1)
        }
    }

    fn locate(cell: usize) -> (usize, usize) {
        let mutex = cell % LARGE_TABLE_MUTEXES;
        let offset = cell / LARGE_TABLE_MUTEXES;
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
            state: LargeTableEntryState::Unloaded(position),
            ..Default::default()
        }
    }

    pub fn new_empty() -> Self {
        Self::default()
    }

    pub fn from_snapshot_position(position: &WalPosition) -> Self {
        if position == &WalPosition::INVALID {
            LargeTableEntry::new_empty()
        } else {
            LargeTableEntry::new_unloaded(*position)
        }
    }

    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        let dirty_state = self.state.mark_dirty();
        dirty_state.into_dirty_keys().insert(k.clone());
        self.data.make_mut().insert(k, v);
        self.last_added_position = Some(v);
    }

    pub fn remove(&mut self, k: Bytes, v: WalPosition) {
        let dirty_state = self.state.mark_dirty();
        match dirty_state {
            DirtyState::Loaded(dirty_keys) => {
                self.data.make_mut().remove(&k);
                dirty_keys.insert(k);
            }
            DirtyState::Unloaded(dirty_keys) => {
                // We could just use dirty_keys and not use WalPosition::INVALID as a marker.
                // In that case, however, we would need to clone and pass dirty_keys to a snapshot.
                self.data.make_mut().insert(k.clone(), WalPosition::INVALID);
                dirty_keys.insert(k);
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

    /// See IndexTable::next_entry for documentation.
    pub fn next_entry(
        &self,
        next_key: Option<Bytes>,
    ) -> Option<(Bytes, WalPosition, Option<Bytes>)> {
        if matches!(&self.state, LargeTableEntryState::Unloaded(_)) {
            panic!("Can't next_entry in unloaded state");
        }
        self.data.next_entry(next_key)
    }

    pub fn maybe_load<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
        let Some((state, position)) = self.state.as_unloaded_state() else {
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
            LargeTableEntryState::DirtyLoaded(_) => {
                LargeTableSnapshotEntry::Dirty(self.data.clone_shared())
            }
            LargeTableEntryState::DirtyUnloaded(pos, _) => {
                LargeTableSnapshotEntry::DirtyUnloaded(pos, self.data.clone_shared())
            }
        }
    }

    /// Updates dirty state to clean state if entry was not updated since the snapshot was taken
    pub fn maybe_set_to_clean(&mut self, expected: &Arc<IndexTable>, position: WalPosition) {
        if !self.data.same_shared(expected) {
            // The entry has changed since the snapshot was taken
            return;
        }
        match self.state.as_dirty_state() {
            None => {}
            // DirtyLoaded changes to Loaded
            Some(DirtyState::Loaded(_)) => self.state = LargeTableEntryState::Loaded(position),
            // DirtyUnloaded changes to Unloaded, data is purged
            // We can also change it to Loaded,
            // if we send merged Index from the snapshot
            Some(DirtyState::Unloaded(_)) => {
                self.data = Default::default();
                self.state = LargeTableEntryState::Unloaded(position)
            }
        }
    }

    pub fn unload<L: Loader>(&mut self, _loader: &L) -> Result<(), L::Error> {
        match &self.state {
            LargeTableEntryState::Empty => {}
            LargeTableEntryState::Unloaded(_) => {}
            LargeTableEntryState::Loaded(pos) => {
                self.state = LargeTableEntryState::Unloaded(*pos);
                self.data = Default::default();
            }
            LargeTableEntryState::DirtyUnloaded(_pos, _dirty_keys) => {
                // load, merge, flush and unload -> Unloaded(..)
            }
            LargeTableEntryState::DirtyLoaded(_dirty_key) => {
                // either (a) flush and unload -> Unloaded(..)
                // or     (b) unmerge and unload -> DirtyUnloaded(..)
            }
        }
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

    pub fn is_empty(&self) -> bool {
        matches!(self.state, LargeTableEntryState::Empty)
    }
}

impl LargeTableEntryState {
    pub fn mark_dirty(&mut self) -> DirtyState {
        match self {
            LargeTableEntryState::Empty => {
                *self = LargeTableEntryState::DirtyLoaded(Default::default())
            }
            LargeTableEntryState::Loaded(_) => {
                *self = LargeTableEntryState::DirtyLoaded(Default::default())
            }
            LargeTableEntryState::DirtyLoaded(_) => {}
            LargeTableEntryState::Unloaded(pos) => {
                *self = LargeTableEntryState::DirtyUnloaded(*pos, HashSet::default())
            }
            LargeTableEntryState::DirtyUnloaded(_, _) => {}
        }
        self.as_dirty_state()
            .expect("mark_dirty sets state to one of dirty states")
    }

    pub fn as_dirty_state(&mut self) -> Option<DirtyState> {
        match self {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(_) => None,
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(_, dirty_keys) => {
                Some(DirtyState::Unloaded(dirty_keys))
            }
            LargeTableEntryState::DirtyLoaded(dirty_keys) => Some(DirtyState::Loaded(dirty_keys)),
        }
    }

    pub fn as_unloaded_state(&self) -> Option<(UnloadedState, WalPosition)> {
        match self {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(pos) => Some((UnloadedState::Clean, *pos)),
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(pos, _) => Some((UnloadedState::Dirty, *pos)),
            LargeTableEntryState::DirtyLoaded(_) => None,
        }
    }

    pub fn dirty_keys(&mut self) -> Option<&mut HashSet<Bytes>> {
        Some(self.as_dirty_state()?.into_dirty_keys())
    }
}

impl<'a> DirtyState<'a> {
    pub fn into_dirty_keys(self) -> &'a mut HashSet<Bytes> {
        match self {
            DirtyState::Loaded(dirty_keys) => dirty_keys,
            DirtyState::Unloaded(dirty_keys) => dirty_keys,
        }
    }
}

enum DirtyState<'a> {
    Loaded(&'a mut HashSet<Bytes>),
    Unloaded(&'a mut HashSet<Bytes>),
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

    pub fn checked_increment(&mut self) {
        self.0 = self
            .0
            .checked_add(1)
            .expect("Can not increment id: too large");
    }
}

impl Default for LargeTableEntryState {
    fn default() -> Self {
        Self::Empty
    }
}
