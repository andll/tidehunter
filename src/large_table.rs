use crate::config::Config;
use crate::index_table::IndexTable;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc};
use crate::metrics::Metrics;
use crate::primitives::arc_cow::ArcCow;
use crate::primitives::lru::Lru;
use crate::primitives::sharded_mutex::ShardedMutex;
use crate::wal::{WalPosition, WalRandomRead};
use minibytes::Bytes;
use parking_lot::{MappedMutexGuard, MutexGuard};
use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

pub struct LargeTable {
    table: Vec<ShardedMutex<Row>>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Version(pub u64);

pub struct LargeTableEntry {
    ks: KeySpaceDesc,
    data: ArcCow<IndexTable>,
    last_added_position: Option<WalPosition>,
    state: LargeTableEntryState,
    metrics: Arc<Metrics>,
}

enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    DirtyUnloaded(WalPosition, HashSet<Bytes>),
    DirtyLoaded(WalPosition, HashSet<Bytes>),
}

struct Row {
    lru: Lru,
    data: Box<[LargeTableEntry]>,
}

/// ks -> row -> cell
pub(crate) struct LargeTableContainer<T>(pub Vec<Vec<Vec<T>>>);

pub(crate) struct LargeTableSnapshot {
    data: LargeTableContainer<LargeTableSnapshotEntry>,
    last_added_position: Option<WalPosition>,
}

/// ks -> row -> cell

pub(crate) enum LargeTableSnapshotEntry {
    Empty,
    Clean(WalPosition),
    Dirty(KeySpace, Arc<IndexTable>),
    DirtyUnloaded(KeySpace, WalPosition, Arc<IndexTable>),
}

impl LargeTable {
    pub fn from_unloaded(
        key_shape: &KeyShape,
        snapshot: &LargeTableContainer<WalPosition>,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> Self {
        assert_eq!(
            snapshot.0.len(),
            key_shape.num_ks(),
            "Snapshot has different number of key spaces"
        );
        let table = key_shape
            .iter_ks()
            .zip(snapshot.0.iter())
            .map(|(ks, ks_snapshot)| {
                if ks_snapshot.len() != ks.num_mutexes() {
                    panic!(
                        "Invalid snapshot for ks {}: {} rows, expected {} rows",
                        ks.id().as_usize(),
                        ks_snapshot.len(),
                        ks.num_mutexes()
                    );
                }
                let rows = ks_snapshot.iter().map(|row_snapshot| {
                    let data = row_snapshot
                        .iter()
                        .map(|position| {
                            LargeTableEntry::from_snapshot_position(
                                ks.clone(),
                                position,
                                metrics.clone(),
                            )
                        })
                        .collect();
                    Row {
                        data,
                        lru: Lru::default(),
                    }
                });
                ShardedMutex::from_iterator(rows)
            })
            .collect();
        Self {
            table,
            config,
            metrics,
        }
    }

    pub fn insert<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: Bytes,
        v: WalPosition,
        loader: &L,
    ) -> Result<(), L::Error> {
        let (mut row, offset) = self.row(ks, &k);
        let entry = row.entry_mut(offset);
        entry.insert(k, v);
        let index_size = entry.data.len();
        if loader.unload_supported() && self.too_many_dirty(entry) {
            entry.unload(loader, &self.config)?;
            row.lru.remove(offset as u64);
        }
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

        let max_index_size = self.metrics.max_index_size.load(Ordering::Relaxed);
        self.metrics
            .max_index_size_metric
            .set(max_index_size as i64);
        self.metrics.index_size.observe(index_size as f64);
        Ok(())
    }

    pub fn remove<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: Bytes,
        v: WalPosition,
        _loader: &L,
    ) -> Result<(), L::Error> {
        let (mut row, offset) = self.row(ks, &k);
        let entry = row.entry_mut(offset);
        entry.remove(k, v);
        if self.count_as_loaded(entry) {
            // todo unload
            row.lru.insert(offset as u64);
        }
        Ok(())
    }

    pub fn get<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: &[u8],
        loader: &L,
    ) -> Result<Option<WalPosition>, L::Error> {
        let (mut row, offset) = self.row(ks, k);
        let entry = row.entry_mut(offset);
        let index_position = match entry.state {
            LargeTableEntryState::Empty => return Ok(None),
            LargeTableEntryState::Loaded(_) => return Ok(entry.get(k)),
            LargeTableEntryState::DirtyLoaded(_, _) => return Ok(entry.get(k)),
            LargeTableEntryState::DirtyUnloaded(position, _) => {
                // optimization: in dirty unloaded state we might not need to load entry
                if let Some(found) = entry.get(k) {
                    if self.count_as_loaded(entry) {
                        row.lru.insert(offset as u64);
                    }
                    return Ok(found.valid());
                }
                position
            }
            LargeTableEntryState::Unloaded(position) => position,
        };
        // todo move tokio dep under a feature
        let now = Instant::now();
        let index_reader = loader.index_reader(index_position)?;
        // todo - consider only doing block_in_place for the syscall random reader
        let result =
            tokio::task::block_in_place(|| IndexTable::lookup_unloaded(ks, &index_reader, k));
        self.metrics
            .lookup_mcs
            .with_label_values(&[index_reader.kind_str(), entry.ks.name()])
            .observe(now.elapsed().as_micros() as f64);
        Ok(result)
    }

    pub fn is_empty(&self) -> bool {
        self.table.iter().all(|m| {
            m.mutexes()
                .iter()
                .all(|m| m.lock().data.iter().all(LargeTableEntry::is_empty))
        })
    }

    fn count_as_loaded(&self, entry: &mut LargeTableEntry) -> bool {
        if let Some((u, _)) = entry.state.as_unloaded_state() {
            self.config.excess_dirty_keys(u.dirty_keys_count())
        } else {
            true
        }
    }

    fn too_many_dirty(&self, entry: &mut LargeTableEntry) -> bool {
        if let Some(dk) = entry.state.dirty_keys() {
            self.config.excess_dirty_keys(dk.len())
        } else {
            false
        }
    }

    fn load_entry<'a, L: Loader>(
        &self,
        mut row: MutexGuard<'a, Row>,
        offset: usize,
        loader: &L,
    ) -> Result<MappedMutexGuard<'a, LargeTableEntry>, L::Error> {
        row.lru.insert(offset as u64); // entry will be loaded so no need to check count_as_loaded

        if loader.unload_supported() && row.lru.len() > self.config.max_loaded_entries() {
            // todo - try to unload Loaded entry even if unload is not supported
            let unload = row.lru.pop().expect("Lru is not empty");
            assert_ne!(
                unload, offset as u64,
                "Attempting unload entry we are just trying to load"
            );
            // todo - we can try different approaches,
            // for example prioritize unloading Loaded entries over Dirty entries
            row.data[unload as usize].unload(loader, &self.config)?;
        }
        let mut entry = MutexGuard::map(row, |l| &mut l.data[offset]);
        entry.maybe_load(loader)?;
        Ok(entry)
    }

    fn row(&self, ks: &KeySpaceDesc, k: &[u8]) -> (MutexGuard<'_, Row>, usize) {
        let ks_table = self.ks_table(ks);
        let (mutex, offset) = ks.locate(k);
        let row = ks_table.lock(
            mutex,
            &self
                .metrics
                .large_table_contention
                .with_label_values(&[ks.name()]),
        );
        (row, offset)
    }

    fn ks_table(&self, ks: &KeySpaceDesc) -> &ShardedMutex<Row> {
        self.table
            .get(ks.id().as_usize())
            .expect("Table not found for ks")
    }

    /// Provides a snapshot of this large table.
    /// Takes &mut reference to ensure consistency of last_added_position.
    pub fn snapshot(&mut self) -> LargeTableSnapshot {
        let mut last_added_position = None;
        let data = self
            .table
            .iter()
            .map(|ks_table| {
                ks_table
                    .mutexes()
                    .iter()
                    .map(|mutex| {
                        let mut lock = mutex.lock();
                        lock.data
                            .iter_mut()
                            .map(|entry| {
                                let snapshot = entry.snapshot();
                                LargeTableSnapshot::update_last_added_position(
                                    &mut last_added_position,
                                    entry.last_added_position,
                                );
                                snapshot
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect();
        let data = LargeTableContainer(data);
        LargeTableSnapshot {
            data,
            last_added_position,
        }
    }

    /// Update dirty entries to 'Loaded', if they have not changed since the time snapshot was taken.
    pub fn maybe_update_entries(
        &self,
        updates: LargeTableContainer<Option<(Arc<IndexTable>, WalPosition)>>,
    ) {
        for (ks_table, ks_updates) in self.table.iter().zip(updates.0.into_iter()) {
            for (mutex, row_updates) in ks_table.mutexes().iter().zip(ks_updates.into_iter()) {
                let mut lock = mutex.lock();
                for (entry, update) in lock.data.iter_mut().zip(row_updates.into_iter()) {
                    let Some((data, position)) = update else {
                        continue;
                    };
                    entry.maybe_set_to_clean(&data, position);
                }
            }
        }
    }

    /// Takes a next entry in the large table.
    ///
    /// See Db::next_entry for documentation.
    pub fn next_entry<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        mut cell: usize,
        mut next_key: Option<Bytes>,
        loader: &L,
        max_cell_exclusive: Option<usize>,
    ) -> Result<
        Option<(
            Option<usize>, /*next cell*/
            Option<Bytes>, /*next key*/
            Bytes,         /*fetched key*/
            WalPosition,   /*fetched value*/
        )>,
        L::Error,
    > {
        let ks_table = self.ks_table(ks);
        loop {
            let (row, offset) = ks.locate_cell(cell);
            let mut row = ks_table.lock(
                row,
                &self
                    .metrics
                    .large_table_contention
                    .with_label_values(&[ks.name()]),
            );
            let entry = row.entry_mut(offset);
            // todo lru logic
            entry.maybe_load(loader)?;
            if let Some((key, value, next_key)) = entry.next_entry(next_key) {
                let next_cell = if next_key.is_none() {
                    ks.next_cell(cell)
                } else {
                    Some(cell)
                };
                return Ok(Some((next_cell, next_key, key, value)));
            } else {
                next_key = None;
                let Some(next_cell) = ks.next_cell(cell) else {
                    return Ok(None);
                };
                if let Some(max_cell_exclusive) = max_cell_exclusive {
                    if next_cell >= max_cell_exclusive {
                        return Ok(None);
                    }
                }
                cell = next_cell;
            }
        }
    }

    /// See Db::last_in_range for documentation.
    pub fn last_in_range<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        cell: usize,
        from_included: &Bytes,
        to_included: &Bytes,
        loader: &L,
    ) -> Result<Option<(Bytes, WalPosition)>, L::Error> {
        // todo duplicate code with next_entry(...)
        let (row, offset) = ks.locate_cell(cell);
        let ks_table = self.ks_table(ks);
        let mut row = ks_table.lock(
            row,
            &self
                .metrics
                .large_table_contention
                .with_label_values(&[ks.name()]),
        );
        let entry = row.entry_mut(offset);
        // todo lru logic
        entry.maybe_load(loader)?;
        // todo make sure can't have dirty markers in index in this state
        Ok(entry.data.last_in_range(from_included, to_included))
    }

    pub fn report_entries_state(&self) {
        let mut states: HashMap<_, i64> = HashMap::new();
        for ks_table in &self.table {
            for mutex in ks_table.mutexes() {
                let lock = mutex.lock();
                for entry in lock.data.iter() {
                    *states.entry(entry.state.name()).or_default() += 1;
                }
            }
        }
        for (label, value) in states {
            self.metrics
                .entry_state
                .with_label_values(&[label])
                .set(value);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_all_clean(&self) -> bool {
        for ks_table in &self.table {
            for mutex in ks_table.mutexes() {
                let mut lock = mutex.lock();
                for entry in lock.data.iter_mut() {
                    if entry.state.as_dirty_state().is_some() {
                        return false;
                    }
                }
            }
        }
        true
    }
}

impl Row {
    pub fn entry_mut(&mut self, offset: usize) -> &mut LargeTableEntry {
        &mut self.data[offset]
    }
}

pub trait Loader {
    type Error;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> Result<IndexTable, Self::Error>;

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error>;

    fn unload_supported(&self) -> bool;

    fn unload(&self, ks: KeySpace, data: &IndexTable) -> Result<WalPosition, Self::Error>;
}

impl LargeTableEntry {
    pub fn new_unloaded(ks: KeySpaceDesc, position: WalPosition, metrics: Arc<Metrics>) -> Self {
        Self::new_with_state(ks, LargeTableEntryState::Unloaded(position), metrics)
    }

    pub fn new_empty(ks: KeySpaceDesc, metrics: Arc<Metrics>) -> Self {
        Self::new_with_state(ks, LargeTableEntryState::Empty, metrics)
    }

    fn new_with_state(
        ks: KeySpaceDesc,
        state: LargeTableEntryState,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            ks,
            state,
            data: Default::default(),
            last_added_position: Default::default(),
            metrics,
        }
    }

    pub fn from_snapshot_position(
        ks: KeySpaceDesc,
        position: &WalPosition,
        metrics: Arc<Metrics>,
    ) -> Self {
        if position == &WalPosition::INVALID {
            LargeTableEntry::new_empty(ks, metrics)
        } else {
            LargeTableEntry::new_unloaded(ks, *position, metrics)
        }
    }

    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        let dirty_state = self.state.mark_dirty();
        dirty_state.into_dirty_keys().insert(k.clone());
        let previous = self.data.make_mut().insert(k, v);
        self.report_loaded_keys_change(previous, Some(v));
        self.last_added_position = Some(v);
    }

    pub fn remove(&mut self, k: Bytes, v: WalPosition) {
        let dirty_state = self.state.mark_dirty();
        let (previous, new) = match dirty_state {
            DirtyState::Loaded(dirty_keys) => {
                let previous = self.data.make_mut().remove(&k);
                dirty_keys.insert(k);
                (previous, None)
            }
            DirtyState::Unloaded(dirty_keys) => {
                // We could just use dirty_keys and not use WalPosition::INVALID as a marker.
                // In that case, however, we would need to clone and pass dirty_keys to a snapshot.
                let previous = self.data.make_mut().insert(k.clone(), WalPosition::INVALID);
                dirty_keys.insert(k);
                (previous, Some(WalPosition::INVALID))
            }
        };
        self.report_loaded_keys_change(previous, new);
        self.last_added_position = Some(v);
    }

    fn report_loaded_keys_delta(&self, delta: i64) {
        self.metrics
            .loaded_keys
            .with_label_values(&[self.ks.name()])
            .add(delta);
    }

    fn report_loaded_keys_change(&self, old: Option<WalPosition>, new: Option<WalPosition>) {
        let delta = match (old, new) {
            (None, None) => return,
            (Some(_), Some(_)) => return,
            (Some(_), None) => -1,
            (None, Some(_)) => 1,
        };
        self.report_loaded_keys_delta(delta);
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
        let mut data = loader.load(&self.ks, position)?;
        let dirty_keys = match state {
            UnloadedState::Dirty(dirty_keys) => {
                data.merge_dirty(&self.data);
                Some(mem::take(dirty_keys))
            }
            UnloadedState::Clean => None,
        };
        self.report_loaded_keys_delta(data.len() as i64 - self.data.len() as i64);
        self.data = ArcCow::new_owned(data);
        if let Some(dirty_keys) = dirty_keys {
            self.state = LargeTableEntryState::DirtyLoaded(position, dirty_keys);
        } else {
            self.state = LargeTableEntryState::Loaded(position);
        }
        Ok(())
    }

    pub fn snapshot(&mut self) -> LargeTableSnapshotEntry {
        match self.state {
            LargeTableEntryState::Empty => LargeTableSnapshotEntry::Empty,
            LargeTableEntryState::Unloaded(pos) => LargeTableSnapshotEntry::Clean(pos),
            LargeTableEntryState::Loaded(pos) => LargeTableSnapshotEntry::Clean(pos),
            LargeTableEntryState::DirtyLoaded(_, _) => {
                LargeTableSnapshotEntry::Dirty(self.ks.id(), self.data.clone_shared())
            }
            LargeTableEntryState::DirtyUnloaded(pos, _) => {
                LargeTableSnapshotEntry::DirtyUnloaded(self.ks.id(), pos, self.data.clone_shared())
            }
        }
    }

    /// Updates dirty state to clean state if entry was not updated since the snapshot was taken
    pub fn maybe_set_to_clean(&mut self, expected: &Arc<IndexTable>, position: WalPosition) {
        // todo - write amplification can be reduced here:
        // even when we see that entry has changed,
        // we still can update Unloaded dirty keys to keep fewer keys in memory
        // this can reduce write amplification and it will trigger unload less frequently.
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
                self.report_loaded_keys_delta(-(self.data.len() as i64));
                self.data = Default::default();
                self.state = LargeTableEntryState::Unloaded(position)
            }
        }
    }

    pub fn unload<L: Loader>(&mut self, loader: &L, config: &Config) -> Result<(), L::Error> {
        match &self.state {
            LargeTableEntryState::Empty => {}
            LargeTableEntryState::Unloaded(_) => {}
            LargeTableEntryState::Loaded(pos) => {
                self.metrics.unload.with_label_values(&["clean"]).inc();
                self.state = LargeTableEntryState::Unloaded(*pos);
                self.report_loaded_keys_delta(-(self.data.len() as i64));
                self.data = Default::default();
            }
            LargeTableEntryState::DirtyUnloaded(_pos, _dirty_keys) => {
                // load, merge, flush and unload -> Unloaded(..)
                self.metrics
                    .unload
                    .with_label_values(&["merge_flush"])
                    .inc();
                self.maybe_load(loader)?;
                assert!(matches!(
                    self.state,
                    LargeTableEntryState::DirtyLoaded(_, _)
                ));
                self.unload_dirty_loaded(loader)?;
            }
            LargeTableEntryState::DirtyLoaded(position, dirty_keys) => {
                // todo - this position can be invalid
                if config.excess_dirty_keys(dirty_keys.len()) {
                    self.metrics.unload.with_label_values(&["flush"]).inc();
                    // either (a) flush and unload -> Unloaded(..)
                    // small code duplicate between here and unload_dirty_unloaded
                    self.unload_dirty_loaded(loader)?;
                } else {
                    self.metrics.unload.with_label_values(&["unmerge"]).inc();
                    // or (b) unmerge and unload -> DirtyUnloaded(..)
                    /*todo - avoid cloning dirty_keys, especially twice*/
                    let delta = self.data.make_mut().make_dirty(dirty_keys.clone());
                    self.report_loaded_keys_delta(delta);
                    self.state = LargeTableEntryState::DirtyUnloaded(*position, dirty_keys.clone());
                }
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

    fn unload_dirty_loaded<L: Loader>(&mut self, loader: &L) -> Result<(), L::Error> {
        self.run_compactor();
        let position = loader.unload(self.ks.id(), &self.data)?;
        self.state = LargeTableEntryState::Unloaded(position);
        self.report_loaded_keys_delta(-(self.data.len() as i64));
        self.data = Default::default();
        Ok(())
    }

    fn run_compactor(&mut self) {
        // todo run compactor during snapshot
        if let Some(compactor) = self.ks.compactor() {
            let index = self.data.make_mut();
            let pre_compact_len = index.len();
            compactor(&mut index.data);
            let compacted = pre_compact_len.saturating_sub(index.len());
            self.metrics
                .compacted_keys
                .with_label_values(&[self.ks.name()])
                .inc_by(compacted as u64);
            self.report_loaded_keys_delta(-(compacted as i64));
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.state, LargeTableEntryState::Empty)
    }
}

impl LargeTableEntryState {
    pub fn mark_dirty(&mut self) -> DirtyState {
        match self {
            LargeTableEntryState::Empty => {
                *self = LargeTableEntryState::DirtyLoaded(WalPosition::INVALID, Default::default())
            }
            LargeTableEntryState::Loaded(position) => {
                *self = LargeTableEntryState::DirtyLoaded(*position, Default::default())
            }
            LargeTableEntryState::DirtyLoaded(_, _) => {}
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
            LargeTableEntryState::DirtyLoaded(_, dirty_keys) => {
                Some(DirtyState::Loaded(dirty_keys))
            }
        }
    }

    pub fn as_unloaded_state(&mut self) -> Option<(UnloadedState, WalPosition)> {
        match self {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(pos) => Some((UnloadedState::Clean, *pos)),
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(pos, dirty_keys) => {
                Some((UnloadedState::Dirty(dirty_keys), *pos))
            }
            LargeTableEntryState::DirtyLoaded(_, _) => None,
        }
    }

    pub fn dirty_keys(&mut self) -> Option<&mut HashSet<Bytes>> {
        Some(self.as_dirty_state()?.into_dirty_keys())
    }

    #[allow(dead_code)]
    pub fn name(&self) -> &'static str {
        match self {
            LargeTableEntryState::Empty => "empty",
            LargeTableEntryState::Unloaded(_) => "unloaded",
            LargeTableEntryState::Loaded(_) => "loaded",
            LargeTableEntryState::DirtyUnloaded(_, _) => "dirty_unloaded",
            LargeTableEntryState::DirtyLoaded(_, _) => "dirty_loaded",
        }
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

impl<'a> UnloadedState<'a> {
    pub fn dirty_keys_count(&self) -> usize {
        match self {
            UnloadedState::Dirty(dirty_keys) => dirty_keys.len(),
            UnloadedState::Clean => 0,
        }
    }
}

enum DirtyState<'a> {
    Loaded(&'a mut HashSet<Bytes>),
    Unloaded(&'a mut HashSet<Bytes>),
}

enum UnloadedState<'a> {
    Dirty(&'a mut HashSet<Bytes>),
    Clean,
}

impl LargeTableSnapshot {
    pub fn into_entries(self) -> LargeTableContainer<LargeTableSnapshotEntry> {
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

impl<T> LargeTableContainer<T> {
    pub fn try_map<P, E>(self, f: impl Fn(T) -> Result<P, E>) -> Result<LargeTableContainer<P>, E> {
        let mut new_container = Vec::with_capacity(self.0.len());
        for ks_table in self.0.into_iter() {
            let mut new_ks_table = Vec::with_capacity(ks_table.len());
            for row in ks_table.into_iter() {
                let mut new_row = Vec::with_capacity(row.len());
                for element in row {
                    let new_element = f(element)?;
                    new_row.push(new_element);
                }
                new_ks_table.push(new_row);
            }
            new_container.push(new_ks_table);
        }
        Ok(LargeTableContainer(new_container))
    }
}

impl<A, B> LargeTableContainer<(A, B)> {
    pub fn unzip(self) -> (LargeTableContainer<A>, LargeTableContainer<B>) {
        let (a, b) = self
            .0
            .into_iter()
            .map(|ks_table| {
                ks_table
                    .into_iter()
                    .map(|row| row.into_iter().unzip())
                    .unzip()
            })
            .unzip();
        (LargeTableContainer(a), LargeTableContainer(b))
    }
}

impl<T: Copy> LargeTableContainer<T> {
    /// Creates a new container with the given shape and filled with copy of passed value
    pub fn new_from_key_shape(key_shape: &KeyShape, value: T) -> Self {
        Self(
            key_shape
                .iter_ks()
                .map(|ks| {
                    (0..ks.num_mutexes())
                        .map(|_| (0..ks.cells_per_mutex()).map(|_| value).collect())
                        .collect()
                })
                .collect(),
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_shape::KeyShapeBuilder;

    #[test]
    fn test_ks_allocation() {
        let config = Config::small();
        let mut ks = KeyShapeBuilder::new();
        let a = ks.add_key_space("a", 0, 1, 1);
        let b = ks.add_key_space("b", 0, 1, 1);
        ks.add_key_space("c", 0, 1, 1);
        let ks = ks.build();
        let l = LargeTable::from_unloaded(
            &ks,
            &LargeTableContainer::new_from_key_shape(&ks, WalPosition::INVALID),
            Arc::new(config),
            Metrics::new(),
        );
        let (mut row, offset) = l.row(ks.ks(a), &[]);
        assert_eq!(row.entry_mut(offset).ks.name(), "a");
        let (mut row, offset) = l.row(ks.ks(b), &[5, 2, 3, 4]);
        assert_eq!(row.entry_mut(offset).ks.name(), "b");
    }
}
