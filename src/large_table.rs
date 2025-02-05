use crate::config::Config;
use crate::flusher::{FlushKind, IndexFlusher};
use crate::index_table::IndexTable;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc};
use crate::metrics::Metrics;
use crate::primitives::arc_cow::ArcCow;
use crate::primitives::sharded_mutex::ShardedMutex;
use crate::wal::{WalPosition, WalRandomRead};
use bloom::{BloomFilter, ASMS};
use lru::LruCache;
use minibytes::Bytes;
use parking_lot::MutexGuard;
use rand::rngs::ThreadRng;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};

pub struct LargeTable {
    table: Vec<KsTable>,
    config: Arc<Config>,
    flusher: IndexFlusher,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct Version(pub u64);

pub struct LargeTableEntry {
    ks: KeySpaceDesc,
    cell: usize,
    data: ArcCow<IndexTable>,
    last_added_position: Option<WalPosition>,
    state: LargeTableEntryState,
    metrics: Arc<Metrics>,
    bloom_filter: Option<BloomFilter>,
    unload_jitter: usize,
    flush_pending: bool,
}

enum LargeTableEntryState {
    Empty,
    Unloaded(WalPosition),
    Loaded(WalPosition),
    DirtyUnloaded(WalPosition, HashSet<Bytes>),
    DirtyLoaded(WalPosition, HashSet<Bytes>),
}

struct KsTable {
    ks: KeySpaceDesc,
    rows: ShardedMutex<Row>,
}

struct Row {
    value_lru: Option<LruCache<Bytes, Bytes>>,
    data: Box<[LargeTableEntry]>,
}

/// ks -> row -> cell
pub(crate) struct LargeTableContainer<T>(pub Vec<Vec<Vec<T>>>);

pub(crate) struct LargeTableSnapshot {
    pub data: LargeTableContainer<WalPosition>,
    pub last_added_position: WalPosition,
}

impl LargeTable {
    pub fn from_unloaded(
        key_shape: &KeyShape,
        snapshot: &LargeTableContainer<WalPosition>,
        config: Arc<Config>,
        flusher: IndexFlusher,
        metrics: Arc<Metrics>,
    ) -> Self {
        assert_eq!(
            snapshot.0.len(),
            key_shape.num_ks(),
            "Snapshot has different number of key spaces"
        );
        let mut rng = ThreadRng::default();
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
                let rows = ks_snapshot
                    .iter()
                    .enumerate()
                    .map(|(row_index, row_snapshot)| {
                        let data = row_snapshot
                            .iter()
                            .enumerate()
                            .map(|(row_offset, position)| {
                                let cell = ks.cell_by_location(row_index, row_offset);
                                let unload_jitter = config.gen_dirty_keys_jitter(&mut rng);
                                LargeTableEntry::from_snapshot_position(
                                    ks.clone(),
                                    cell,
                                    position,
                                    metrics.clone(),
                                    unload_jitter,
                                )
                            })
                            .collect();
                        let value_lru = ks.value_cache_size().map(LruCache::new);
                        Row { data, value_lru }
                    });
                let rows = ShardedMutex::from_iterator(rows);
                KsTable {
                    ks: ks.clone(),
                    rows,
                }
            })
            .collect();
        Self {
            table,
            config,
            flusher,
            metrics,
        }
    }

    pub fn insert<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: Bytes,
        v: WalPosition,
        value: &Bytes,
        loader: &L,
    ) -> Result<(), L::Error> {
        let (mut row, offset) = self.row(ks, &k);
        if let Some(value_lru) = &mut row.value_lru {
            value_lru.push(k.clone(), value.clone());
        }
        let entry = row.entry_mut(offset);
        entry.insert(k, v);
        let index_size = entry.data.len();
        if loader.flush_supported() && self.too_many_dirty(entry) {
            entry.unload_if_ks_enabled(&self.flusher);
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
        if let Some(value_lru) = &mut row.value_lru {
            value_lru.pop(&k);
        }
        let entry = row.entry_mut(offset);
        entry.remove(k, v);
        Ok(())
    }

    pub fn update_lru(&self, ks: &KeySpaceDesc, key: Bytes, value: Bytes) {
        if ks.value_cache_size().is_none() {
            return;
        }
        let (mut row, _offset) = self.row(ks, &key);
        let Some(value_lru) = &mut row.value_lru else {
            unreachable!()
        };
        let mut delta: i64 = (key.len() + value.len()) as i64;
        let previous = value_lru.push(key, value);
        if let Some((p_key, p_value)) = previous {
            delta -= (p_key.len() + p_value.len()) as i64;
        }
        self.metrics
            .value_cache_size
            .with_label_values(&[&ks.name()])
            .add(delta);
    }

    pub fn get<L: Loader>(
        &self,
        ks: &KeySpaceDesc,
        k: &[u8],
        loader: &L,
    ) -> Result<GetResult, L::Error> {
        let (mut row, offset) = self.row(ks, k);
        if let Some(value_lru) = &mut row.value_lru {
            if let Some(value) = value_lru.get(k) {
                self.metrics
                    .lookup_result
                    .with_label_values(&[ks.name(), "found", "lru"])
                    .inc();
                return Ok(GetResult::Value(value.clone()));
            }
        }
        let entry = row.entry_mut(offset);
        if entry.bloom_filter_not_found(k) {
            return Ok(self.report_lookup_result(ks, None, "bloom"));
        }
        let index_position = match entry.state {
            LargeTableEntryState::Empty => return Ok(self.report_lookup_result(ks, None, "cache")),
            LargeTableEntryState::Loaded(_) => {
                return Ok(self.report_lookup_result(ks, entry.get(k), "cache"))
            }
            LargeTableEntryState::DirtyLoaded(_, _) => {
                return Ok(self.report_lookup_result(ks, entry.get(k), "cache"))
            }
            LargeTableEntryState::DirtyUnloaded(position, _) => {
                // optimization: in dirty unloaded state we might not need to load entry
                if let Some(found) = entry.get(k) {
                    return Ok(self.report_lookup_result(ks, found.valid(), "cache"));
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
        Ok(self.report_lookup_result(ks, result, "lookup"))
    }

    fn report_lookup_result(
        &self,
        ks: &KeySpaceDesc,
        v: Option<WalPosition>,
        source: &str,
    ) -> GetResult {
        let found = if v.is_some() { "found" } else { "not_found" };
        self.metrics
            .lookup_result
            .with_label_values(&[ks.name(), found, source])
            .inc();
        match v {
            None => GetResult::NotFound,
            Some(w) => GetResult::WalPosition(w),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.table.iter().all(|m| {
            m.rows
                .mutexes()
                .iter()
                .all(|m| m.lock().data.iter().all(LargeTableEntry::is_empty))
        })
    }

    fn too_many_dirty(&self, entry: &mut LargeTableEntry) -> bool {
        if let Some(dk) = entry.state.dirty_keys() {
            self.config
                .excess_dirty_keys(dk.len().saturating_sub(entry.unload_jitter))
        } else {
            false
        }
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
        &self
            .table
            .get(ks.id().as_usize())
            .expect("Table not found for ks")
            .rows
    }

    /// Provides a snapshot of this large table along with replay position in the wal for the snapshot.
    pub fn snapshot<L: Loader>(
        &self,
        tail_position: u64,
        loader: &L,
    ) -> Result<LargeTableSnapshot, L::Error> {
        assert!(loader.flush_supported());
        // See ks_snapshot documentation for details
        // on how snapshot replay position is determined.
        let mut replay_from = None;
        let mut max_position: Option<WalPosition> = None;
        let mut data = Vec::with_capacity(self.table.len());
        for ks_table in self.table.iter() {
            let (ks_data, ks_replay_from, ks_max_position) =
                self.ks_snapshot(ks_table, tail_position, loader)?;
            data.push(ks_data);
            if let Some(ks_replay_from) = ks_replay_from {
                replay_from = Some(cmp::min(
                    replay_from.unwrap_or(WalPosition::MAX),
                    ks_replay_from,
                ));
            }
            if let Some(ks_max_position) = ks_max_position {
                max_position = Some(if let Some(max_position) = max_position {
                    cmp::max(ks_max_position, max_position)
                } else {
                    ks_max_position
                });
            }
        }

        let replay_from =
            replay_from.unwrap_or_else(|| max_position.unwrap_or(WalPosition::INVALID));

        let data = LargeTableContainer(data);
        Ok(LargeTableSnapshot {
            data,
            last_added_position: replay_from,
        })
    }

    /// Takes snapshot of a given key space.
    /// Returns (Snapshot, ReplayFrom, MaximumValidEntryWalPosition).
    ///
    /// Replay from is calculated as the lowest wal position across all dirty entries
    /// Replay from is None if all entries in ks are clean entries.
    ///
    /// Maximum valid entry wal position is a maximum wal position of all entries persisted to disk
    /// This position is None if no entries are persisted to disk (all entries are Empty).
    ///
    /// The combined snapshot replay position across all key spaces is determined as following:
    ///
    /// * If at least one replay_from for ks table is not None, the replay_from for entire snapshot
    ///   is a minimum across all key spaces where replay_from is not None.
    ///   Reasoning here is that if some key space has some dirty entry,
    ///   the entire snapshot needs to be replayed from the position of that dirty entry.
    ///
    /// * If all the ReplayFrom are None, the replay position for snapshot
    ///   is a maximum of all non-None MaximumValidEntryWalPosition across all key spaces.
    ///   The Reasoning is that if all entries in all key spaces are clean, it is safe to replay
    ///   wal from the highest written index entry.
    ///   As more entries could be added to the wal while snapshot is created,
    ///   we cannot simply take the current wal writer position here as a snapshot replay position.
    ///
    /// * If all ReplayFrom are None and all MaximumValidEntryWalPosition are None,
    ///   then the database is empty at the time of a snapshot, and the replay_position
    ///   for snapshot is set to WalPosition::INVALID to indicate wal needs to be replayed
    ///   from the beginning.
    fn ks_snapshot<L: Loader>(
        &self,
        ks_table: &KsTable,
        tail_position: u64,
        loader: &L,
    ) -> Result<
        (
            Vec<Vec<WalPosition>>,
            Option<WalPosition>,
            Option<WalPosition>,
        ),
        L::Error,
    > {
        let mut replay_from = None;
        let mut max_wal_position: Option<WalPosition> = None;
        let mut ks_data = Vec::with_capacity(ks_table.rows.mutexes().len());
        for mutex in ks_table.rows.mutexes() {
            let mut row = mutex.lock();
            let mut row_data = Vec::with_capacity(row.data.len());
            for entry in &mut row.data {
                entry.maybe_unload_for_snapshot(loader, &self.config, tail_position)?;
                let position = entry.state.wal_position();
                row_data.push(position);
                if let Some(valid_position) = position.valid() {
                    max_wal_position = if let Some(max_wal_position) = max_wal_position {
                        Some(cmp::max(max_wal_position, valid_position))
                    } else {
                        Some(valid_position)
                    };
                }
                if entry.state.is_dirty() {
                    replay_from = Some(cmp::min(replay_from.unwrap_or(WalPosition::MAX), position));
                }
            }
            ks_data.push(row_data);
        }
        let metric = self
            .metrics
            .index_distance_from_tail
            .with_label_values(&[&ks_table.ks.name()]);
        match replay_from {
            None => metric.set(0),
            Some(WalPosition::INVALID) => metric.set(-1),
            Some(position) => {
                // This can go below 0 because tail_position is not synchronized
                // and index position can be higher than the tail_position in rare cases
                metric.set(tail_position.saturating_sub(position.as_u64()) as i64)
            }
        }
        Ok((ks_data, replay_from, max_wal_position))
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
            // todo read from disk instead of loading
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
        // todo read from disk instead of loading
        entry.maybe_load(loader)?;
        // todo make sure can't have dirty markers in index in this state
        Ok(entry.data.last_in_range(from_included, to_included))
    }

    pub fn report_entries_state(&self) {
        let mut states: HashMap<_, i64> = HashMap::new();
        for ks_table in &self.table {
            for mutex in ks_table.rows.mutexes() {
                let lock = mutex.lock();
                for entry in lock.data.iter() {
                    *states
                        .entry((entry.ks.name().to_string(), entry.state.name()))
                        .or_default() += 1;
                }
            }
        }
        for ((ks, state), value) in states {
            self.metrics
                .entry_state
                .with_label_values(&[&ks, state])
                .set(value);
        }
    }

    pub fn update_flushed_index(
        &self,
        ks: &KeySpaceDesc,
        cell: usize,
        original_index: Arc<IndexTable>,
        position: WalPosition,
    ) {
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
        entry.update_flushed_index(original_index, position);
    }

    #[cfg(test)]
    pub(crate) fn is_all_clean(&self) -> bool {
        for ks_table in &self.table {
            for mutex in ks_table.rows.mutexes() {
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

    fn flush_supported(&self) -> bool;

    fn flush(&self, ks: KeySpace, data: &IndexTable) -> Result<WalPosition, Self::Error>;
}

impl LargeTableEntry {
    pub fn new_unloaded(
        ks: KeySpaceDesc,
        cell: usize,
        position: WalPosition,
        metrics: Arc<Metrics>,
        unload_jitter: usize,
    ) -> Self {
        Self::new_with_state(
            ks,
            cell,
            LargeTableEntryState::Unloaded(position),
            metrics,
            unload_jitter,
        )
    }

    pub fn new_empty(
        ks: KeySpaceDesc,
        cell: usize,
        metrics: Arc<Metrics>,
        unload_jitter: usize,
    ) -> Self {
        Self::new_with_state(
            ks,
            cell,
            LargeTableEntryState::Empty,
            metrics,
            unload_jitter,
        )
    }

    fn new_with_state(
        ks: KeySpaceDesc,
        cell: usize,
        state: LargeTableEntryState,
        metrics: Arc<Metrics>,
        unload_jitter: usize,
    ) -> Self {
        let bloom_filter = ks
            .bloom_filter()
            .map(|params| BloomFilter::with_rate(params.rate, params.count));
        Self {
            ks,
            cell,
            state,
            data: Default::default(),
            last_added_position: Default::default(),
            metrics,
            bloom_filter,
            unload_jitter,
            flush_pending: false,
        }
    }

    pub fn from_snapshot_position(
        ks: KeySpaceDesc,
        cell: usize,
        position: &WalPosition,
        metrics: Arc<Metrics>,
        unload_jitter: usize,
    ) -> Self {
        if position == &WalPosition::INVALID {
            LargeTableEntry::new_empty(ks, cell, metrics, unload_jitter)
        } else {
            LargeTableEntry::new_unloaded(ks, cell, *position, metrics, unload_jitter)
        }
    }

    pub fn insert(&mut self, k: Bytes, v: WalPosition) {
        let dirty_state = self.state.mark_dirty();
        dirty_state.into_dirty_keys().insert(k.clone());
        self.insert_bloom_filter(&k);
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

    fn insert_bloom_filter(&mut self, key: &[u8]) {
        let Some(bloom_filter) = &mut self.bloom_filter else {
            return;
        };
        // todo - rebuild bloom filter from time to time?
        bloom_filter.insert(&key);
    }

    /// Returns true if key is definitely not in the cell
    fn bloom_filter_not_found(&self, key: &[u8]) -> bool {
        if let Some(bloom_filter) = &self.bloom_filter {
            !bloom_filter.contains(&key)
        } else {
            false
        }
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

    pub fn maybe_unload_for_snapshot<L: Loader>(
        &mut self,
        loader: &L,
        config: &Config,
        tail_position: u64,
    ) -> Result<(), L::Error> {
        if !self.state.is_dirty() {
            return Ok(());
        }
        if self.flush_pending {
            // todo metric / log?
            return Ok(());
        }
        let position = self.state.wal_position();
        // position can actually be great then tail_position due to concurrency
        let distance = tail_position.saturating_sub(position.as_u64());
        if distance >= config.snapshot_unload_threshold {
            self.metrics
                .snapshot_force_unload
                .with_label_values(&[self.ks.name()])
                .inc();
            // todo - we don't need to unload here, only persist the entry,
            // Just reusing unload logic for now.
            self.unload(loader, config, true)?;
        }
        Ok(())
    }

    pub fn unload_if_ks_enabled(&mut self, flusher: &IndexFlusher) {
        if self.ks.unloading_disabled() {
            return;
        }
        if !self.flush_pending {
            self.flush_pending = true;
            let flush_kind = self
                .flush_kind()
                .expect("unload_if_ks_enabled is called in clean state");
            flusher.request_flush(self.ks.id(), self.cell, flush_kind);
        }
    }

    pub fn update_flushed_index(&mut self, original_index: Arc<IndexTable>, position: WalPosition) {
        assert!(
            self.flush_pending,
            "update_merged_index called while flush_pending is not set"
        );
        match self.state {
            LargeTableEntryState::DirtyUnloaded(_, _) => {}
            LargeTableEntryState::DirtyLoaded(_, _) => {}
            LargeTableEntryState::Empty => panic!("update_merged_index in Empty state"),
            LargeTableEntryState::Unloaded(_) => panic!("update_merged_index in Unloaded state"),
            LargeTableEntryState::Loaded(_) => panic!("update_merged_index in Loaded state"),
        }
        self.flush_pending = false;
        if self.data.same_shared(&original_index) {
            self.report_loaded_keys_delta(-(self.data.len() as i64));
            self.data = Default::default();
            self.state = LargeTableEntryState::Unloaded(position);
            self.metrics
                .flush_update
                .with_label_values(&["clear"])
                .inc();
        } else {
            let delta = self.data.make_mut().unmerge_flushed(&original_index);
            self.report_loaded_keys_delta(delta);
            // todo remove dirty_keys from DirtyUnloaded
            let dirty_keys = self.data.data.keys().cloned().collect();
            self.state = LargeTableEntryState::DirtyUnloaded(position, dirty_keys);
            self.metrics
                .flush_update
                .with_label_values(&["unmerge"])
                .inc();
        }
    }

    fn unload<L: Loader>(
        &mut self,
        loader: &L,
        config: &Config,
        force_clean: bool,
    ) -> Result<(), L::Error> {
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
                if force_clean || config.excess_dirty_keys(dirty_keys.len()) {
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
        let position = loader.flush(self.ks.id(), &self.data)?;
        self.state = LargeTableEntryState::Unloaded(position);
        self.report_loaded_keys_delta(-(self.data.len() as i64));
        self.data = Default::default();
        Ok(())
    }

    fn run_compactor(&mut self) {
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

    pub fn flush_kind(&mut self) -> Option<FlushKind> {
        match self.state {
            LargeTableEntryState::Empty => None,
            LargeTableEntryState::Unloaded(_) => None,
            LargeTableEntryState::Loaded(_) => None,
            LargeTableEntryState::DirtyUnloaded(position, _) => {
                Some(FlushKind::MergeUnloaded(position, self.data.clone_shared()))
            }
            LargeTableEntryState::DirtyLoaded(_, _) => {
                Some(FlushKind::FlushLoaded(self.data.clone_shared()))
            }
        }
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

    /// Wal position of the persisted index entry.
    pub fn wal_position(&self) -> WalPosition {
        match self {
            LargeTableEntryState::Empty => WalPosition::INVALID,
            LargeTableEntryState::Unloaded(w) => *w,
            LargeTableEntryState::Loaded(w) => *w,
            LargeTableEntryState::DirtyUnloaded(w, _) => *w,
            LargeTableEntryState::DirtyLoaded(w, _) => *w,
        }
    }

    /// Returns whether wal needs to be replayed from the position returned by wal_position()
    /// to restore large table entry.
    ///
    /// This is the same as as_dirty_state().is_some()
    pub fn is_dirty(&self) -> bool {
        match self {
            LargeTableEntryState::Empty => false,
            LargeTableEntryState::Unloaded(_) => false,
            LargeTableEntryState::Loaded(_) => false,
            LargeTableEntryState::DirtyUnloaded(_, _) => true,
            LargeTableEntryState::DirtyLoaded(_, _) => true,
        }
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

pub enum GetResult {
    Value(Bytes),
    WalPosition(WalPosition),
    NotFound,
}

impl GetResult {
    pub fn is_found(&self) -> bool {
        match self {
            GetResult::Value(_) => true,
            GetResult::WalPosition(_) => true,
            GetResult::NotFound => false,
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

enum DirtyState<'a> {
    Loaded(&'a mut HashSet<Bytes>),
    Unloaded(&'a mut HashSet<Bytes>),
}

enum UnloadedState<'a> {
    Dirty(&'a mut HashSet<Bytes>),
    Clean,
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
            IndexFlusher::new_unstarted_for_test(),
            Metrics::new(),
        );
        let (mut row, offset) = l.row(ks.ks(a), &[]);
        assert_eq!(row.entry_mut(offset).ks.name(), "a");
        let (mut row, offset) = l.row(ks.ks(b), &[5, 2, 3, 4]);
        assert_eq!(row.entry_mut(offset).ks.name(), "b");
    }

    #[test]
    fn test_bloom_size() {
        let f = BloomFilter::with_rate(0.01, 8000);
        println!("hashes: {}, bytes: {}", f.num_hashes(), f.num_bits() / 8);
    }
}
