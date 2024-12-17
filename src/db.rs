use crate::batch::WriteBatch;
use crate::config::Config;
use crate::control::ControlRegion;
use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::index_table::IndexTable;
use crate::iterators::range_ordered::RangeOrderedIterator;
use crate::iterators::unordered::UnorderedIterator;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc};
use crate::large_table::{
    LargeTable, LargeTableContainer, LargeTableSnapshot, LargeTableSnapshotEntry, Loader, Version,
};
use crate::metrics::Metrics;
use crate::wal::{
    PreparedWalWrite, Wal, WalError, WalIterator, WalPosition, WalRandomRead, WalWriter,
};
use bytes::{Buf, BufMut, BytesMut};
use memmap2::{MmapMut, MmapOptions};
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::{io, thread};

pub struct Db {
    // todo - avoid read lock on reads?
    large_table: RwLock<LargeTable>,
    wal: Arc<Wal>,
    wal_writer: WalWriter,
    control_region_store: Mutex<ControlRegionStore>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    key_shape: KeyShape,
}

pub type DbResult<T> = Result<T, DbError>;

pub const MAX_KEY_LEN: usize = u16::MAX as usize;

impl Db {
    pub fn open(
        path: &Path,
        key_shape: KeyShape,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> DbResult<Self> {
        let cr = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("cr"))?;
        let (control_region_store, control_region) =
            Self::read_or_create_control_region(&cr, &key_shape)?;
        let large_table = LargeTable::from_unloaded(
            &key_shape,
            control_region.snapshot(),
            config.clone(),
            metrics.clone(),
        );
        let wal = Wal::open(&path.join("wal"), config.wal_layout())?;
        let wal_iterator = wal.wal_iterator(control_region.last_position())?;
        let wal_writer = Self::replay_wal(&key_shape, &large_table, wal_iterator, &metrics)?;
        let large_table = RwLock::new(large_table);
        let control_region_store = Mutex::new(control_region_store);
        Ok(Self {
            large_table,
            wal_writer,
            wal,
            control_region_store,
            config,
            metrics,
            key_shape,
        })
    }

    pub fn start_periodic_snapshot(self: &Arc<Self>) {
        // todo account number of bytes read during wal replay
        let position = self.wal_writer.position();
        let weak = Arc::downgrade(self);
        thread::Builder::new()
            .name("snapshot".to_string())
            .spawn(move || Self::periodic_snapshot_thread(weak, position))
            .unwrap();
    }

    fn periodic_snapshot_thread(weak: Weak<Db>, mut position: u64) -> Option<()> {
        loop {
            thread::sleep(Duration::from_secs(30));
            let db = weak.upgrade()?;
            db.large_table.read().report_entries_state();
            // todo when we get to wal position wrapping around this will need to be fixed
            let current_position = db.wal_writer.position();
            let written = current_position.checked_sub(position).unwrap();
            if written > db.config.snapshot_written_bytes() {
                // todo taint db instance on failure?
                db.rebuild_control_region()
                    .expect("Failed to rebuild control region");
                position = db.wal_writer.position();
            }
        }
    }

    fn read_or_create_control_region(
        cr: &File,
        key_shape: &KeyShape,
    ) -> Result<(ControlRegionStore, ControlRegion), DbError> {
        let file_len = cr.metadata()?.len() as usize;
        let cr_len = key_shape.cr_len();
        println!("cr_len {cr_len}");
        let mut cr_map = unsafe { MmapOptions::new().len(cr_len * 2).map_mut(cr)? };
        let (last_written_left, control_region) = if file_len != cr_len * 2 {
            cr.set_len((cr_len * 2) as u64)?;
            let skip_marker = CrcFrame::skip_marker();
            cr_map[0..skip_marker.len_with_header()].copy_from_slice(skip_marker.as_ref());
            cr_map.flush()?;
            (false, ControlRegion::new_empty(key_shape))
        } else {
            Self::read_control_region(&cr_map, key_shape)?
        };
        let control_region_store = ControlRegionStore {
            cr_map,
            last_written_left,
            last_version: control_region.version(),
        };
        Ok((control_region_store, control_region))
    }

    fn read_control_region(
        cr_map: &MmapMut,
        key_shape: &KeyShape,
    ) -> Result<(bool, ControlRegion), DbError> {
        let cr_len = key_shape.cr_len();
        assert_eq!(cr_map.len(), cr_len * 2);
        let cr1 = CrcFrame::read_from_slice(&cr_map, 0);
        let cr2 = CrcFrame::read_from_slice(&cr_map, cr_len);
        let (last_written_left, cr) = match (cr1, cr2) {
            (Ok(cr1), Err(_)) => (true, cr1),
            (Err(_), Ok(cr2)) => (false, cr2),
            (Ok(cr1), Ok(cr2)) => {
                let version1 = ControlRegion::version_from_bytes(cr1);
                let version2 = ControlRegion::version_from_bytes(cr2);
                if version1 > version2 {
                    (true, cr1)
                } else {
                    (false, cr2)
                }
            }
            // Cr region is valid but empty
            (Err(CrcReadError::SkipMarker), Err(_)) => {
                return Ok((false, ControlRegion::new_empty(key_shape)))
            }
            (Err(_), Err(_)) => return Err(DbError::CrCorrupted),
        };
        let control_region = ControlRegion::from_slice(&cr, key_shape);
        Ok((last_written_left, control_region))
    }

    pub fn insert(&self, ks: KeySpace, k: impl Into<Bytes>, v: impl Into<Bytes>) -> DbResult<()> {
        let k = k.into();
        let v = v.into();
        let ks = self.key_shape.ks(ks);
        ks.check_key(&k);
        let w = PreparedWalWrite::new(&WalEntry::Record(ks.id(), k.clone(), v));
        self.metrics
            .wal_written_bytes_type
            .with_label_values(&["record"])
            .inc_by(w.len() as u64);
        let position = self.wal_writer.write(&w)?;
        self.metrics.wal_written_bytes.set(position.as_u64() as i64);
        self.large_table.read().insert(ks, k, position, self)?;
        Ok(())
    }

    pub fn remove(&self, ks: KeySpace, k: impl Into<Bytes>) -> DbResult<()> {
        let k = k.into();
        self.key_shape.ks(ks).check_key(&k);
        let w = PreparedWalWrite::new(&WalEntry::Remove(ks, k.clone()));
        self.metrics
            .wal_written_bytes_type
            .with_label_values(&["tombstone"])
            .inc_by(w.len() as u64);
        let position = self.wal_writer.write(&w)?;
        Ok(self
            .large_table
            .read()
            .remove(self.key_shape.ks(ks), k, position, self)?)
    }

    pub fn get(&self, ks: KeySpace, k: &[u8]) -> DbResult<Option<Bytes>> {
        let ks = self.key_shape.ks(ks);
        let Some(position) = self.large_table.read().get(ks, k, self)? else {
            return Ok(None);
        };
        let value = self.read_record(k, position)?;
        Ok(Some(value))
    }

    pub fn exists(&self, ks: KeySpace, k: &[u8]) -> DbResult<bool> {
        let ks = self.key_shape.ks(ks);
        Ok(self.large_table.read().get(ks, k, self)?.is_some())
    }

    pub fn write_batch(&self, batch: WriteBatch) -> DbResult<()> {
        // todo implement atomic durability
        let lock = self.large_table.read();
        let WriteBatch { writes, deletes } = batch;
        let mut last_position = WalPosition::INVALID;
        for (ks, k, w) in writes {
            self.metrics
                .wal_written_bytes_type
                .with_label_values(&["record"])
                .inc_by(w.len() as u64);
            let position = self.wal_writer.write(&w)?;
            let ks = self.key_shape.ks(ks);
            ks.check_key(&k);
            lock.insert(ks, k, position, self)?;
            last_position = position;
        }

        for (ks, k, w) in deletes {
            self.metrics
                .wal_written_bytes_type
                .with_label_values(&["tombstone"])
                .inc_by(w.len() as u64);
            let position = self.wal_writer.write(&w)?;
            self.key_shape.ks(ks).check_key(&k);
            lock.remove(self.key_shape.ks(ks), k, position, self)?;
            last_position = position;
        }
        if last_position != WalPosition::INVALID {
            self.metrics
                .wal_written_bytes
                .set(last_position.as_u64() as i64);
        }

        Ok(())
    }

    /// Unordered iterator over entire key space
    pub fn unordered_iterator(self: &Arc<Self>, ks: KeySpace) -> UnorderedIterator {
        UnorderedIterator::new(self.clone(), ks)
    }

    /// Ordered iterator over a pre-defined range of keys.
    ///
    /// Both start and end of the range should have the same first 4 bytes,
    /// otherwise this function panics.
    pub fn range_ordered_iterator(
        self: &Arc<Self>,
        ks: KeySpace,
        range: Range<Bytes>,
    ) -> RangeOrderedIterator {
        // todo (fix) technically with Range<Bytes> end of the range is exclusive
        let cell = self.key_shape.range_cell(ks, &range.start, &range.end);
        RangeOrderedIterator::new(self.clone(), ks, cell, range)
    }

    /// Returns last key-value pair in the given range, where both ends of the range are included
    ///
    /// Both start and end of the range should have the same first 4 bytes,
    /// otherwise this function panics.
    pub fn last_in_range(
        &self,
        ks: KeySpace,
        from_included: &Bytes,
        to_included: &Bytes,
    ) -> DbResult<Option<(Bytes, Bytes)>> {
        let cell = self.key_shape.range_cell(ks, from_included, to_included);
        let ks = self.key_shape.ks(ks);
        let Some((key, position)) =
            self.large_table
                .read()
                .last_in_range(ks, cell, from_included, to_included, self)?
        else {
            return Ok(None);
        };
        let value = self.read_record(&key, position)?;
        Ok(Some((key, value)))
    }

    /// Returns true if this db is empty.
    ///
    /// (warn) Right now it returns true if db was never inserted true,
    /// but may return false if entry was inserted and then deleted.
    pub fn is_empty(&self) -> bool {
        self.large_table.read().is_empty()
    }

    /// Returns the next entry in the database.
    /// Iterator must specify the cell to inspect and the (Optional) next key.
    ///
    /// If the next_key is set to None, the first key in the cell is returned.
    ///
    /// When iterating the entire DB, the iterator starts with cell=0 and next_key=None.
    ///
    /// The returned values:
    /// (1) Next cell to read, None if iterator has reached the end of the DB.
    /// (2) Next key to read.
    ///     This value should be passed as is to next call of next_entry.
    ///     None here **does not** mean iteration has ended.
    /// (3) the key fetched by the iterator
    /// (4) the value fetched by the iterator
    ///
    /// This function allows concurrent modification of the database.
    /// If next_key is deleted,
    /// the function will return the key-value pair next after the deleted key.
    ///
    /// As such, the returned key might not match the value passed in the next_key.
    pub(crate) fn next_entry(
        &self,
        ks: KeySpace,
        cell: usize,
        next_key: Option<Bytes>,
        max_cell_exclusive: Option<usize>,
    ) -> DbResult<
        Option<(
            Option<usize>, /*next cell*/
            Option<Bytes>, /*next key*/
            Bytes,         /*fetched key*/
            Bytes,         /*fetched value*/
        )>,
    > {
        let ks = self.key_shape.ks(ks);
        let Some((next_cell, next_key, key, wal_position)) =
            self.large_table
                .read()
                .next_entry(ks, cell, next_key, self, max_cell_exclusive)?
        else {
            return Ok(None);
        };
        let value = self.read_record(&key, wal_position)?;
        Ok(Some((next_cell, next_key, key, value)))
    }

    fn read_record(&self, k: &[u8], position: WalPosition) -> DbResult<Bytes> {
        let entry = self.read_report_entry(position)?;
        if let WalEntry::Record(KeySpace(_), wal_key, v) = entry {
            debug_assert_eq!(wal_key.as_ref(), k);
            Ok(v)
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }

    fn report_read(&self, entry: &WalEntry, mapped: bool) {
        let (kind, ks) = match entry {
            WalEntry::Record(ks, _, _) => ("record", self.key_shape.ks(*ks).name()),
            WalEntry::Index(ks, _) => ("index", self.key_shape.ks(*ks).name()),
            WalEntry::Remove(ks, _) => ("tombstone", self.key_shape.ks(*ks).name()),
        };
        let mapped = if mapped { "mapped" } else { "unmapped" };
        self.metrics
            .read
            .with_label_values(&[ks, kind, mapped])
            .inc();
        self.metrics
            .read_bytes
            .with_label_values(&[ks, kind, mapped])
            .inc_by(entry.len() as u64);
    }

    fn replay_wal(
        key_shape: &KeyShape,
        large_table: &LargeTable,
        mut wal_iterator: WalIterator,
        metrics: &Metrics,
    ) -> DbResult<WalWriter> {
        loop {
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_))) {
                break Ok(wal_iterator.into_writer());
            }
            let (position, entry) = entry?;
            let entry = WalEntry::from_bytes(entry);
            match entry {
                WalEntry::Record(ks, k, _v) => {
                    metrics.replayed_wal_records.inc();
                    let ks = key_shape.ks(ks);
                    large_table.insert(ks, k, position, wal_iterator.wal())?;
                }
                WalEntry::Index(_ks, _bytes) => {
                    // todo - handle this by updating large table to Loaded()
                }
                WalEntry::Remove(ks, k) => {
                    metrics.replayed_wal_records.inc();
                    large_table.remove(key_shape.ks(ks), k, position, wal_iterator.wal())?;
                }
            }
        }
    }

    fn rebuild_control_region(&self) -> DbResult<()> {
        let mut crs = self.control_region_store.lock();
        // drop large_table lock asap
        // todo (critical) read lock need to cover wal allocation and insert to large table!!
        let snapshot = self.large_table.write().snapshot();
        let last_added_position = snapshot.last_added_position();
        let last_added_position = last_added_position.unwrap_or(WalPosition::INVALID);
        let snapshot = self.write_snapshot(snapshot)?;
        // todo fsync wal first
        crs.store(snapshot, last_added_position)
    }

    fn write_snapshot(
        &self,
        snapshot: LargeTableSnapshot,
    ) -> DbResult<LargeTableContainer<WalPosition>> {
        let entries = snapshot.into_entries();
        let snapshot_and_updates = entries.try_map(|entry| self.write_entry_snapshot(entry))?;
        let (snapshot, index_updates) = snapshot_and_updates.unzip();
        self.large_table.read().maybe_update_entries(index_updates);
        Ok(snapshot)
    }

    fn write_entry_snapshot(
        &self,
        entry: LargeTableSnapshotEntry,
    ) -> DbResult<(WalPosition, Option<(Arc<IndexTable>, WalPosition)>)> {
        match entry {
            LargeTableSnapshotEntry::Empty => Ok((WalPosition::INVALID, None)),
            LargeTableSnapshotEntry::Clean(pos) => Ok((pos, None)),
            LargeTableSnapshotEntry::Dirty(ks, index) => {
                let position = self.write_index(ks, &index)?;
                Ok((position, Some((index, position))))
            }
            LargeTableSnapshotEntry::DirtyUnloaded(ks, pos, index) => {
                let mut clean = self.load(self.key_shape.ks(ks), pos)?;
                clean.merge_dirty(&index);
                let position = self.write_index(ks, &clean)?;
                Ok((position, Some((index, position))))
            }
        }
    }

    fn write_index(&self, ks: KeySpace, index: &IndexTable) -> DbResult<WalPosition> {
        let index = index.to_bytes(self.key_shape.ks(ks));
        let w = PreparedWalWrite::new(&WalEntry::Index(ks, index));
        self.metrics
            .wal_written_bytes_type
            .with_label_values(&["index"])
            .inc_by(w.len() as u64);
        Ok(self.wal_writer.write(&w)?)
    }

    fn read_entry(wal: &Wal, position: WalPosition) -> DbResult<(bool, WalEntry)> {
        let (mapped, entry) = wal.read_unmapped(position)?;
        Ok((mapped, WalEntry::from_bytes(entry)))
    }

    fn read_report_entry(&self, position: WalPosition) -> DbResult<WalEntry> {
        let (mapped, entry) = Self::read_entry(&self.wal, position)?;
        self.report_read(&entry, mapped);
        Ok(entry)
    }

    fn read_index(ks: &KeySpaceDesc, entry: WalEntry) -> DbResult<IndexTable> {
        if let WalEntry::Index(_, bytes) = entry {
            let entry = IndexTable::from_bytes(ks, bytes);
            Ok(entry)
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }
}

struct ControlRegionStore {
    cr_map: MmapMut,
    last_version: Version,
    last_written_left: bool,
}

impl ControlRegionStore {
    pub fn store(
        &mut self,
        snapshot: LargeTableContainer<WalPosition>,
        last_position: WalPosition,
    ) -> DbResult<()> {
        let control_region = ControlRegion::new(snapshot, self.increment_version(), last_position);
        let frame = CrcFrame::new(&control_region);
        assert_eq!(frame.len_with_header() * 2, self.cr_map.len());
        let write_to = if self.last_written_left {
            // write right
            &mut self.cr_map[frame.len_with_header()..]
        } else {
            // write left
            &mut self.cr_map[..frame.len_with_header()]
        };
        write_to.copy_from_slice(frame.as_ref());
        self.cr_map.flush()?;
        self.last_written_left = !self.last_written_left;
        Ok(())
    }

    fn increment_version(&mut self) -> Version {
        self.last_version.checked_increment();
        self.last_version
    }
}

impl Loader for Wal {
    type Error = DbError;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> DbResult<IndexTable> {
        let (_, entry) = Db::read_entry(self, position)?;
        Db::read_index(ks, entry)
    }

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error> {
        Ok(self.random_reader_at(position, WalEntry::INDEX_PREFIX_SIZE)?)
    }

    fn unload_supported(&self) -> bool {
        false
    }

    fn unload(&self, _ks: KeySpace, _data: &IndexTable) -> DbResult<WalPosition> {
        unimplemented!()
    }
}

impl Loader for Db {
    type Error = DbError;

    fn load(&self, ks: &KeySpaceDesc, position: WalPosition) -> DbResult<IndexTable> {
        let entry = self.read_report_entry(position)?;
        Self::read_index(ks, entry)
    }

    fn index_reader(&self, position: WalPosition) -> Result<WalRandomRead, Self::Error> {
        Ok(self
            .wal
            .random_reader_at(position, WalEntry::INDEX_PREFIX_SIZE)?)
    }

    fn unload_supported(&self) -> bool {
        true
    }

    fn unload(&self, ks: KeySpace, data: &IndexTable) -> DbResult<WalPosition> {
        self.write_index(ks, data)
    }
}

pub(crate) enum WalEntry {
    Record(KeySpace, Bytes, Bytes),
    Index(KeySpace, Bytes),
    Remove(KeySpace, Bytes),
}

#[derive(Debug)]
pub enum DbError {
    Io(io::Error),
    CrCorrupted,
    WalError(WalError),
    CorruptedIndexEntry(bincode::Error),
}

impl WalEntry {
    const WAL_ENTRY_RECORD: u8 = 1;
    const WAL_ENTRY_INDEX: u8 = 2;
    const WAL_ENTRY_REMOVE: u8 = 3;
    pub const INDEX_PREFIX_SIZE: usize = 2;

    pub fn from_bytes(bytes: Bytes) -> Self {
        let mut b = &bytes[..];
        let entry_type = b.get_u8();
        match entry_type {
            WalEntry::WAL_ENTRY_RECORD => {
                let ks = KeySpace(b.get_u8());
                let key_len = b.get_u16() as usize;
                let k = bytes.slice(4..4 + key_len);
                let v = bytes.slice(4 + key_len..);
                WalEntry::Record(ks, k, v)
            }
            WalEntry::WAL_ENTRY_INDEX => {
                let ks = KeySpace(b.get_u8());
                WalEntry::Index(ks, bytes.slice(2..))
            }
            WalEntry::WAL_ENTRY_REMOVE => {
                let ks = KeySpace(b.get_u8());
                WalEntry::Remove(ks, bytes.slice(2..))
            }
            _ => panic!("Unknown wal entry type {entry_type}"),
        }
    }
}

impl IntoBytesFixed for WalEntry {
    fn len(&self) -> usize {
        match self {
            WalEntry::Record(KeySpace(_), k, v) => 1 + 1 + 2 + k.len() + v.len(),
            WalEntry::Index(KeySpace(_), index) => 1 + 1 + index.len(),
            WalEntry::Remove(KeySpace(_), k) => 1 + 1 + k.len(),
        }
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        // todo avoid copy here
        match self {
            WalEntry::Record(ks, k, v) => {
                buf.put_u8(Self::WAL_ENTRY_RECORD);
                buf.put_u8(ks.0);
                buf.put_u16(k.len() as u16);
                buf.put_slice(&k);
                buf.put_slice(&v);
            }
            WalEntry::Index(ks, bytes) => {
                buf.put_u8(Self::WAL_ENTRY_INDEX);
                buf.put_u8(ks.0);
                buf.put_slice(&bytes);
            }
            WalEntry::Remove(ks, k) => {
                buf.put_u8(Self::WAL_ENTRY_REMOVE);
                buf.put_u8(ks.0);
                buf.put_slice(&k)
            }
        }
    }
}

impl From<io::Error> for DbError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<WalError> for DbError {
    fn from(value: WalError) -> Self {
        Self::WalError(value)
    }
}

impl From<bincode::Error> for DbError {
    fn from(value: bincode::Error) -> Self {
        Self::CorruptedIndexEntry(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::key_shape::KeyShapeBuilder;
    use rand::rngs::ThreadRng;
    use rand::RngCore;
    use std::collections::HashSet;

    #[test]
    fn db_test() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(4, 12, 12);
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();
            db.insert(ks, vec![3, 4, 5, 6], vec![7]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
            db.rebuild_control_region().unwrap();
            assert!(
                db.large_table.read().is_all_clean(),
                "Some entries are not clean after snapshot"
            );
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                metrics.clone(),
            )
            .unwrap();
            // nothing replayed from wal since we just rebuilt the control region
            assert_eq!(metrics.replayed_wal_records.get(), 0);
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
            db.insert(ks, vec![3, 4, 5, 6], vec![8]).unwrap();
            assert_eq!(Some(vec![8].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                metrics.clone(),
            )
            .unwrap();
            assert_eq!(metrics.replayed_wal_records.get(), 1);
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![8].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new().clone(),
            )
            .unwrap();
            db.insert(ks, vec![3, 4, 5, 6], vec![9]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![9].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
    }

    #[test]
    fn test_batch() {
        let dir = tempdir::TempDir::new("test-batch").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(4, 12, 12);
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let mut batch = WriteBatch::new();
        batch.write(ks, vec![5, 6, 7, 8], vec![15]);
        batch.write(ks, vec![6, 7, 8, 9], vec![17]);
        db.write_batch(batch).unwrap();
        assert_eq!(Some(vec![15].into()), db.get(ks, &[5, 6, 7, 8]).unwrap());
        assert_eq!(Some(vec![17].into()), db.get(ks, &[6, 7, 8, 9]).unwrap());
    }

    #[test]
    fn test_remove() {
        let dir = tempdir::TempDir::new("test-remove").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(4, 12, 12);
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();
            db.insert(ks, vec![3, 4, 5, 6], vec![7]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
            db.remove(ks, vec![1, 2, 3, 4]).unwrap();
            assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
            db.remove(ks, vec![1, 2, 3, 4]).unwrap();
            assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
        }
        {
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap();
            assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
            db.insert(ks, vec![1, 2, 3, 4], vec![9, 10]).unwrap();
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
            assert_eq!(Some(vec![9, 10].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
            db.rebuild_control_region().unwrap();
            db.remove(ks, vec![1, 2, 3, 4]).unwrap();
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                metrics.clone(),
            )
            .unwrap();
            assert_eq!(metrics.replayed_wal_records.get(), 1);
            assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        }
    }

    #[test]
    fn test_unordered_iterator() {
        let dir = tempdir::TempDir::new("test-unordered-iterator").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(4, 12, 12);
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            let mut it = db.unordered_iterator(ks);
            assert!(it.next().is_none());
            db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();
            db.insert(ks, vec![3, 4, 5, 6], vec![7]).unwrap();
            let it = db.unordered_iterator(ks);
            let s: DbResult<HashSet<_>> = it.collect();
            let s = s.unwrap();
            assert_eq!(s.len(), 2);
            assert!(s.contains(&(vec![1, 2, 3, 4].into(), vec![5, 6].into())));
            assert!(s.contains(&(vec![3, 4, 5, 6].into(), vec![7].into())));
        }
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            let it = db.unordered_iterator(ks);
            let s: DbResult<HashSet<_>> = it.collect();
            let s = s.unwrap();
            assert_eq!(s.len(), 2);
            assert!(s.contains(&(vec![1, 2, 3, 4].into(), vec![5, 6].into())));
            assert!(s.contains(&(vec![3, 4, 5, 6].into(), vec![7].into())));
        }
    }

    #[test]
    fn test_ordered_iterator() {
        let dir = tempdir::TempDir::new("test-ordered-iterator").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(5, 12, 12);
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            let mut it = db.unordered_iterator(ks);
            assert!(it.next().is_none());
            db.insert(ks, vec![1, 2, 3, 4, 6], vec![1]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 5], vec![2]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 10], vec![3]).unwrap();
            db.insert(ks, vec![3, 4, 5, 6, 11], vec![7]).unwrap();
            let it = db.range_ordered_iterator(
                ks,
                vec![1, 2, 3, 4, 0].into()..vec![1, 2, 3, 4, 10].into(),
            );
            let v: DbResult<Vec<_>> = it.collect();
            let v = v.unwrap();
            assert_eq!(v.len(), 2);
            assert_eq!(
                v.get(0).unwrap(),
                &(vec![1, 2, 3, 4, 5].into(), vec![2].into())
            );
            assert_eq!(
                v.get(1).unwrap(),
                &(vec![1, 2, 3, 4, 6].into(), vec![1].into())
            );
        }
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            let it = db.range_ordered_iterator(
                ks,
                vec![1, 2, 3, 4, 0].into()..vec![1, 2, 3, 4, 10].into(),
            );
            let v: DbResult<Vec<_>> = it.collect();
            let v = v.unwrap();
            assert_eq!(v.len(), 2);
            assert_eq!(
                v.get(0).unwrap(),
                &(vec![1, 2, 3, 4, 5].into(), vec![2].into())
            );
            assert_eq!(
                v.get(1).unwrap(),
                &(vec![1, 2, 3, 4, 6].into(), vec![1].into())
            );
        }
    }

    #[test]
    fn test_empty() {
        let dir = tempdir::TempDir::new("test-empty").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(5, 12, 12);
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            assert!(db.is_empty());
            db.insert(ks, vec![1, 2, 3, 4, 0], vec![1]).unwrap();
            assert!(!db.is_empty());
        }
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            assert!(!db.is_empty());
        }
    }

    #[test]
    fn test_small_keys() {
        let dir = tempdir::TempDir::new("test-small-keys").unwrap();
        let config = Arc::new(Config::small());
        let mut ksb = KeyShapeBuilder::new();
        let ks0 = ksb.add_key_space("a", 0, 12, 12);
        let ks1 = ksb.add_key_space("b", 1, 12, 12);
        let ks2 = ksb.add_key_space("c", 2, 12, 12);
        let _ks3 = ksb.add_key_space("d", 3, 12, 12);
        let key_shape = ksb.build();
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            db.insert(ks0, vec![], vec![1]).unwrap();
            db.insert(ks1, vec![1], vec![2]).unwrap();
            db.insert(ks2, vec![1, 2], vec![3]).unwrap();
            assert_eq!(db.get(ks0, &[]).unwrap(), Some(vec![1].into()));
            assert_eq!(db.get(ks1, &[1]).unwrap(), Some(vec![2].into()));
            assert_eq!(db.get(ks2, &[1, 2]).unwrap(), Some(vec![3].into()));
        }
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            assert_eq!(db.get(ks0, &[]).unwrap(), Some(vec![1].into()));
            assert_eq!(db.get(ks1, &[1]).unwrap(), Some(vec![2].into()));
            assert_eq!(db.get(ks2, &[1, 2]).unwrap(), Some(vec![3].into()));
        }
    }

    #[test]
    fn test_last_in_range() {
        let dir = tempdir::TempDir::new("test-last-in-range").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(5, 12, 12);
        let db = Arc::new(
            Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                Metrics::new(),
            )
            .unwrap(),
        );
        db.insert(ks, vec![1, 2, 3, 4, 6], vec![1]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 5], vec![2]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 10], vec![3]).unwrap();
        assert_eq!(
            db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 8].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 6].into(), vec![1].into()))
        );
        assert_eq!(
            db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 6].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 6].into(), vec![1].into()))
        );
        assert_eq!(
            db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 5].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 5].into(), vec![2].into()))
        );
        assert_eq!(
            db.last_in_range(ks, &vec![1, 2, 3, 4, 4].into(), &vec![1, 2, 3, 4, 4].into())
                .unwrap(),
            None
        );
    }

    #[test]
    // #[ignore]
    // this is an expensive test, use it if strategy around narrow lookup changes
    pub fn test_narrow_lookup() {
        let dir = tempdir::TempDir::new("test-narrow-lookup").unwrap();
        let config = Config::small();
        const BUCKETS: usize = 256;
        const PER_BUCKET: usize = 1024;
        const KEY_SIZE: usize = 4;
        let config = Arc::new(config);
        let mut ksb = KeyShapeBuilder::new();
        let ks = ksb.add_key_space("a", KEY_SIZE, 16, BUCKETS / 16);
        let key_shape = ksb.build();
        let mut rng = ThreadRng::default();
        let mut keys = Vec::with_capacity(BUCKETS * PER_BUCKET);
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            for _ in 0..BUCKETS * PER_BUCKET {
                let mut k = vec![0u8; KEY_SIZE];
                rng.fill_bytes(&mut k);
                keys.push(k.clone());
                db.insert(ks, k, vec![]).unwrap();
            }
            db.rebuild_control_region().unwrap();
        }
        {
            let metrics = Metrics::new();
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    metrics.clone(),
                )
                .unwrap(),
            );
            for key in keys {
                db.get(ks, &key).unwrap().unwrap();
            }
            let lookups = metrics.lookup.with_label_values(&["a"]).get();
            let success = metrics
                .narrow_lookup_success
                .with_label_values(&["a"])
                .get();
            let rate_pct = (success as f64 / lookups as f64) * 100.;
            println!("L {} S{}, success rate {:.0}%", lookups, success, rate_pct);
            assert!(
                rate_pct > 99.,
                "Narrow lookup success rate must be at least 99% if everything setup correctly"
            );
        }
    }

    #[test]
    pub fn test_dirty_unloading() {
        let dir = tempdir::TempDir::new("test-dirty-unloading").unwrap();
        let mut config = Config::small();
        config.max_dirty_keys = 2;
        config.max_loaded_entries = 1;
        let config = Arc::new(config);
        let (key_shape, ks) = KeyShape::new_single(5, 2, 1024);
        #[track_caller]
        fn check_all(db: &Db, ks: KeySpace, last: u8) {
            for i in 5u8..=last {
                assert_eq!(db.get(ks, &[1, 2, 3, 4, i]).unwrap(), Some(vec![i].into()));
            }
        }
        #[track_caller]
        fn check_metrics(
            metrics: &Metrics,
            unmerge: u64,
            flush: u64,
            merge_flush: u64,
            clean: u64,
        ) {
            assert_eq!(
                metrics
                    .unload
                    .get_metric_with_label_values(&["unmerge"])
                    .unwrap()
                    .get(),
                unmerge,
                "unmerge metric does not match"
            );
            assert_eq!(
                metrics
                    .unload
                    .get_metric_with_label_values(&["flush"])
                    .unwrap()
                    .get(),
                flush,
                "flush metric does not match"
            );
            assert_eq!(
                metrics
                    .unload
                    .get_metric_with_label_values(&["merge_flush"])
                    .unwrap()
                    .get(),
                merge_flush,
                "merge_flush metric does not match"
            );
            assert_eq!(
                metrics
                    .unload
                    .get_metric_with_label_values(&["clean"])
                    .unwrap()
                    .get(),
                clean,
                "clean metric does not match"
            );
        }
        let other_key = vec![129, 2, 3, 4, 5];
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            {
                // todo this code predates KS api so we calculate cells manually
                // todo rewrite with using ks API instead
                let ksd = db.key_shape.ks(ks);
                let (mutex1, cell1) = ksd.locate(&other_key);
                let (mutex2, cell2) = ksd.locate(&[1, 2, 3, 4, 5]);
                assert_eq!(mutex1, mutex2);
                assert_ne!(cell1, cell2);
                // code below is needed to search for other_key
                // if layout of large table in test changes
                // other_key should be from the same mutex but different cell
                // This way we trigger unloading on the
                // cell containing keys prefixed by [1, 2, 3, 4, ...]
                //
                // println!("A {:?}", LargeTable::locate(db.key_shape.cell(ks,&[1, 2, 3, 4])));
                // for i in 0..255 {
                //     println!("{i} {:?}", LargeTable::locate(db.key_shape.cell(ks,&[i, 2, 3, 4])));
                // }
            }

            db.insert(ks, other_key.clone(), vec![5]).unwrap(); // fill one
            db.insert(ks, vec![1, 2, 3, 4, 5], vec![5]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 6], vec![6]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 7], vec![7]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 8], vec![8]).unwrap();
            check_metrics(&db.metrics, 0, 1, 0, 0);
            db.insert(ks, vec![1, 2, 3, 4, 9], vec![9]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 10], vec![10]).unwrap();
            check_all(&db, ks, 10);
            check_metrics(&db.metrics, 0, 1, 1, 0);
        }
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            check_all(&db, ks, 10);
            check_metrics(&db.metrics, 0, 0, 0, 0);
        }
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            db.insert(ks, vec![1, 2, 3, 4, 11], vec![11]).unwrap();
            db.insert(ks, vec![1, 2, 3, 4, 12], vec![12]).unwrap();
            check_metrics(&db.metrics, 0, 1, 0, 0);
            check_all(&db, ks, 12);
            db.insert(ks, vec![1, 2, 3, 4, 13], vec![13]).unwrap();
            db.get(ks, &other_key).unwrap().unwrap();
            check_metrics(&db.metrics, 0, 1, 0, 0);
        }
        {
            let db = Arc::new(
                Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap(),
            );
            check_all(&db, ks, 13);
            check_metrics(&db.metrics, 0, 0, 0, 0);
            db.rebuild_control_region().unwrap(); // this puts all entries into clean state
            assert!(
                db.large_table.read().is_all_clean(),
                "Some entries are not clean after snapshot"
            );
            db.get(ks, &other_key).unwrap().unwrap();
            check_metrics(&db.metrics, 0, 0, 0, 0);
        }
    }
}
