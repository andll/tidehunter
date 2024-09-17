use crate::batch::WriteBatch;
use crate::config::Config;
use crate::control::ControlRegion;
use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::index_table::IndexTable;
use crate::iterators::range_ordered::RangeOrderedIterator;
use crate::iterators::unordered::UnorderedIterator;
use crate::large_table::{
    LargeTable, LargeTableSnapshot, LargeTableSnapshotEntry, Loader, Version,
};
use crate::metrics::Metrics;
use crate::wal::{PreparedWalWrite, Wal, WalError, WalIterator, WalPosition, WalWriter};
use bytes::{Buf, BufMut, BytesMut};
use memmap2::{MmapMut, MmapOptions};
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::Ordering;
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
}

pub type DbResult<T> = Result<T, DbError>;

pub const MAX_KEY_LEN: usize = u16::MAX as usize;

impl Db {
    pub fn open(path: &Path, config: Arc<Config>, metrics: Arc<Metrics>) -> DbResult<Self> {
        let cr = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("cr"))?;
        let (control_region_store, control_region) =
            Self::read_or_create_control_region(&cr, &config)?;
        let large_table =
            LargeTable::from_unloaded(control_region.snapshot(), config.clone(), metrics.clone());
        let wal = Wal::open(&path.join("wal"), config.wal_layout())?;
        let wal_iterator = wal.wal_iterator(control_region.last_position())?;
        let wal_writer = Self::replay_wal(&large_table, wal_iterator, &metrics)?;
        let large_table = RwLock::new(large_table);
        let control_region_store = Mutex::new(control_region_store);
        Ok(Self {
            large_table,
            wal_writer,
            wal,
            control_region_store,
            config,
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
            thread::sleep(Duration::from_secs(15));
            let db = weak.upgrade()?;
            // todo when we get to wal position wrapping around this will need to be fixed
            let current_position = db.wal_writer.position();
            let written = current_position.checked_sub(position).unwrap();
            if written > db.config.snapshot_written_bytes() {
                // todo taint db instance on failure?
                db.rebuild_control_region()
                    .expect("Failed to rebuild control region");
                position = current_position;
            }
        }
    }

    fn read_or_create_control_region(
        cr: &File,
        config: &Config,
    ) -> Result<(ControlRegionStore, ControlRegion), DbError> {
        let file_len = cr.metadata()?.len() as usize;
        let cr_len = config.cr_len();
        let mut cr_map = unsafe { MmapOptions::new().len(cr_len * 2).map_mut(cr)? };
        let (last_written_left, control_region) = if file_len != cr_len * 2 {
            cr.set_len((cr_len * 2) as u64)?;
            let skip_marker = CrcFrame::skip_marker();
            cr_map[0..skip_marker.len_with_header()].copy_from_slice(skip_marker.as_ref());
            cr_map.flush()?;
            (false, ControlRegion::new_empty(config.large_table_size()))
        } else {
            Self::read_control_region(&cr_map, config)?
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
        config: &Config,
    ) -> Result<(bool, ControlRegion), DbError> {
        let cr_len = config.cr_len();
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
                return Ok((false, ControlRegion::new_empty(config.large_table_size())))
            }
            (Err(_), Err(_)) => return Err(DbError::CrCorrupted),
        };
        let control_region = ControlRegion::from_slice(&cr, config.large_table_size());
        Ok((last_written_left, control_region))
    }

    pub fn insert(&self, k: impl Into<Bytes>, v: impl Into<Bytes>) -> DbResult<()> {
        let k = k.into();
        let v = v.into();
        assert!(k.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let w = PreparedWalWrite::new(&WalEntry::Record(k.clone(), v));
        let position = self.wal_writer.write(&w)?;
        self.large_table.read().insert(k, position, self)?;
        Ok(())
    }

    pub fn remove(&self, k: impl Into<Bytes>) -> DbResult<()> {
        let k = k.into();
        assert!(k.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let w = PreparedWalWrite::new(&WalEntry::Remove(k.clone()));
        let position = self.wal_writer.write(&w)?;
        Ok(self.large_table.read().remove(k, position, self)?)
    }

    pub fn get(&self, k: &[u8]) -> DbResult<Option<Bytes>> {
        let Some(position) = self.large_table.read().get(k, self)? else {
            return Ok(None);
        };
        let value = self.read_record(k, position)?;
        Ok(Some(value))
    }

    pub fn exists(&self, k: &[u8]) -> DbResult<bool> {
        Ok(self.large_table.read().get(k, self)?.is_some())
    }

    pub fn write_batch(&self, batch: WriteBatch) -> DbResult<()> {
        // todo implement atomic durability
        let lock = self.large_table.read();
        let WriteBatch { writes, deletes } = batch;

        for (k, w) in writes {
            let position = self.wal_writer.write(&w)?;
            lock.insert(k, position, self)?;
        }

        for (k, w) in deletes {
            let position = self.wal_writer.write(&w)?;
            lock.remove(k, position, self)?;
        }

        Ok(())
    }

    /// Unordered iterator over entire database
    pub fn unordered_iterator(self: &Arc<Self>) -> UnorderedIterator {
        UnorderedIterator::new(self.clone())
    }

    /// Ordered iterator over a pre-defined range of keys.
    ///
    /// Both start and end of the range should have the same first 4 bytes,
    /// otherwise this function panics.
    pub fn range_ordered_iterator(self: &Arc<Self>, range: Range<Bytes>) -> RangeOrderedIterator {
        // todo (fix) technically with Range<Bytes> end of the range is exclusive
        let cell = self.large_table.read().range_cell(&range.start, &range.end);
        RangeOrderedIterator::new(self.clone(), cell, range)
    }

    /// Returns last key-value pair in the given range, where both ends of the range are included
    ///
    /// Both start and end of the range should have the same first 4 bytes,
    /// otherwise this function panics.
    pub fn last_in_range(
        &self,
        from_included: &Bytes,
        to_included: &Bytes,
    ) -> DbResult<Option<(Bytes, Bytes)>> {
        let Some((key, position)) =
            self.large_table
                .read()
                .last_in_range(from_included, to_included, self)?
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
        cell: usize,
        next_key: Option<Bytes>,
    ) -> DbResult<
        Option<(
            Option<usize>, /*next cell*/
            Option<Bytes>, /*next key*/
            Bytes,         /*fetched key*/
            Bytes,         /*fetched value*/
        )>,
    > {
        let Some((next_cell, next_key, key, wal_position)) =
            self.large_table.read().next_entry(cell, next_key, self)?
        else {
            return Ok(None);
        };
        let value = self.read_record(&key, wal_position)?;
        Ok(Some((next_cell, next_key, key, value)))
    }

    fn read_record(&self, k: &[u8], position: WalPosition) -> DbResult<Bytes> {
        let entry = Self::read_entry_unmapped(&self.wal, position)?;
        if let WalEntry::Record(wal_key, v) = entry {
            debug_assert_eq!(wal_key.as_ref(), k);
            Ok(v)
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }

    fn replay_wal(
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
                WalEntry::Record(k, _v) => {
                    metrics.replayed_wal_records.inc();
                    large_table.insert(k, position, wal_iterator.wal())?;
                }
                WalEntry::Index(_bytes) => {
                    // todo - handle this by updating large table to Loaded()
                }
                WalEntry::Remove(k) => {
                    metrics.replayed_wal_records.inc();
                    large_table.remove(k, position, wal_iterator.wal())?;
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

    fn write_snapshot(&self, snapshot: LargeTableSnapshot) -> DbResult<Box<[WalPosition]>> {
        let iter = Box::into_iter(snapshot.into_entries());
        let mut index_updates = vec![];
        let snapshot = iter
            .enumerate()
            .map(|(i, entry)| match entry {
                LargeTableSnapshotEntry::Empty => Ok(WalPosition::INVALID),
                LargeTableSnapshotEntry::Clean(pos) => Ok(pos),
                LargeTableSnapshotEntry::Dirty(index) => {
                    let position = self.write_index(&index)?;
                    index_updates.push((i, index, position));
                    Ok(position)
                }
                LargeTableSnapshotEntry::DirtyUnloaded(pos, index) => {
                    let mut clean = self.load(pos)?;
                    clean.merge_dirty(&index);
                    let position = self.write_index(&clean)?;
                    index_updates.push((i, index, position));
                    Ok(position)
                }
            })
            .collect::<DbResult<Box<[WalPosition]>>>()?;
        self.large_table.read().maybe_update_entries(index_updates);
        Ok(snapshot)
    }

    fn write_index(&self, index: &IndexTable) -> DbResult<WalPosition> {
        let index = bincode::serialize(index)?;
        let index = index.into();
        let w = PreparedWalWrite::new(&WalEntry::Index(index));
        Ok(self.wal_writer.write(&w)?)
    }

    fn read_entry_mapped(wal: &Wal, position: WalPosition) -> DbResult<WalEntry> {
        let entry = wal.read(position)?;
        Ok(WalEntry::from_bytes(entry))
    }

    fn read_entry_unmapped(wal: &Wal, position: WalPosition) -> DbResult<WalEntry> {
        let entry = wal.read_unmapped(position)?;
        Ok(WalEntry::from_bytes(entry))
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
        snapshot: Box<[WalPosition]>,
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

    fn load(&self, position: WalPosition) -> DbResult<IndexTable> {
        let entry = Db::read_entry_mapped(self, position)?;
        if let WalEntry::Index(bytes) = entry {
            let entry = bincode::deserialize(&bytes)?;
            Ok(entry)
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }

    fn unload_supported(&self) -> bool {
        false
    }

    fn unload(&self, _data: &IndexTable) -> DbResult<WalPosition> {
        unimplemented!()
    }
}

impl Loader for Db {
    type Error = DbError;

    fn load(&self, position: WalPosition) -> DbResult<IndexTable> {
        Loader::load(&*self.wal, position)
    }

    fn unload_supported(&self) -> bool {
        true
    }

    fn unload(&self, data: &IndexTable) -> DbResult<WalPosition> {
        self.write_index(data)
    }
}

pub(crate) enum WalEntry {
    Record(Bytes, Bytes),
    Index(Bytes),
    Remove(Bytes),
}

#[derive(Debug)]
pub enum DbError {
    Io(io::Error),
    CrCorrupted,
    WalError(WalError),
    CorruptedIndexEntry(bincode::Error),
}

impl WalEntry {
    const WAL_ENTRY_RECORD: u16 = 1;
    const WAL_ENTRY_INDEX: u16 = 2;
    const WAL_ENTRY_REMOVE: u16 = 3;

    pub fn from_bytes(bytes: Bytes) -> Self {
        let mut b = &bytes[..];
        let entry_type = b.get_u16();
        match entry_type {
            WalEntry::WAL_ENTRY_RECORD => {
                let key_len = b.get_u16() as usize;
                let k = bytes.slice(4..4 + key_len);
                let v = bytes.slice(4 + key_len..);
                WalEntry::Record(k, v)
            }
            WalEntry::WAL_ENTRY_INDEX => WalEntry::Index(bytes.slice(2..)),
            WalEntry::WAL_ENTRY_REMOVE => WalEntry::Remove(bytes.slice(2..)),
            _ => panic!("Unknown wal entry type {entry_type}"),
        }
    }
}

impl IntoBytesFixed for WalEntry {
    fn len(&self) -> usize {
        match self {
            WalEntry::Record(k, v) => 4 + k.len() + v.len(),
            WalEntry::Index(index) => 2 + index.len(),
            WalEntry::Remove(k) => 2 + k.len(),
        }
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        // todo avoid copy here
        match self {
            WalEntry::Record(k, v) => {
                buf.put_u16(Self::WAL_ENTRY_RECORD);
                buf.put_u16(k.len() as u16);
                buf.put_slice(&k);
                buf.put_slice(&v);
            }
            WalEntry::Index(bytes) => {
                buf.put_u16(Self::WAL_ENTRY_INDEX);
                buf.put_slice(&bytes);
            }
            WalEntry::Remove(k) => {
                buf.put_u16(Self::WAL_ENTRY_REMOVE);
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
    use std::collections::HashSet;

    #[test]
    fn db_test() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let config = Arc::new(Config::small());
        {
            let db = Db::open(dir.path(), config.clone(), Metrics::new()).unwrap();
            db.insert(vec![1, 2, 3, 4], vec![5, 6]).unwrap();
            db.insert(vec![3, 4, 5, 6], vec![7]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
        {
            let db = Db::open(dir.path(), config.clone(), Metrics::new()).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
            db.rebuild_control_region().unwrap();
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(dir.path(), config.clone(), metrics.clone()).unwrap();
            // nothing replayed from wal since we just rebuilt the control region
            assert_eq!(metrics.replayed_wal_records.get(), 0);
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
            db.insert(vec![3, 4, 5, 6], vec![8]).unwrap();
            assert_eq!(Some(vec![8].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(dir.path(), config.clone(), metrics.clone()).unwrap();
            assert_eq!(metrics.replayed_wal_records.get(), 1);
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![8].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
        {
            let db = Db::open(dir.path(), config.clone(), Metrics::new().clone()).unwrap();
            db.insert(vec![3, 4, 5, 6], vec![9]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![9].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
    }

    #[test]
    fn test_batch() {
        let dir = tempdir::TempDir::new("test-batch").unwrap();
        let config = Arc::new(Config::small());
        let db = Db::open(dir.path(), config, Metrics::new()).unwrap();
        let mut batch = WriteBatch::new();
        batch.write(vec![5, 6, 7, 8], vec![15]);
        batch.write(vec![6, 7, 8, 9], vec![17]);
        db.write_batch(batch).unwrap();
        assert_eq!(Some(vec![15].into()), db.get(&[5, 6, 7, 8]).unwrap());
        assert_eq!(Some(vec![17].into()), db.get(&[6, 7, 8, 9]).unwrap());
    }

    #[test]
    fn test_remove() {
        let dir = tempdir::TempDir::new("test-remove").unwrap();
        let config = Arc::new(Config::small());
        {
            let db = Db::open(dir.path(), config.clone(), Metrics::new()).unwrap();
            db.insert(vec![1, 2, 3, 4], vec![5, 6]).unwrap();
            db.insert(vec![3, 4, 5, 6], vec![7]).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
            db.remove(vec![1, 2, 3, 4]).unwrap();
            assert_eq!(None, db.get(&[1, 2, 3, 4]).unwrap());
            db.remove(vec![1, 2, 3, 4]).unwrap();
            assert_eq!(None, db.get(&[1, 2, 3, 4]).unwrap());
        }
        {
            let db = Db::open(dir.path(), config.clone(), Metrics::new()).unwrap();
            assert_eq!(None, db.get(&[1, 2, 3, 4]).unwrap());
            db.insert(vec![1, 2, 3, 4], vec![9, 10]).unwrap();
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
            assert_eq!(Some(vec![9, 10].into()), db.get(&[1, 2, 3, 4]).unwrap());
            db.rebuild_control_region().unwrap();
            db.remove(vec![1, 2, 3, 4]).unwrap();
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(dir.path(), config.clone(), metrics.clone()).unwrap();
            assert_eq!(metrics.replayed_wal_records.get(), 1);
            assert_eq!(None, db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
    }

    #[test]
    fn test_unordered_iterator() {
        let dir = tempdir::TempDir::new("test-unordered-iterator").unwrap();
        let config = Arc::new(Config::small());
        {
            let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
            let mut it = db.unordered_iterator();
            assert!(it.next().is_none());
            db.insert(vec![1, 2, 3, 4], vec![5, 6]).unwrap();
            db.insert(vec![3, 4, 5, 6], vec![7]).unwrap();
            let it = db.unordered_iterator();
            let s: DbResult<HashSet<_>> = it.collect();
            let s = s.unwrap();
            assert_eq!(s.len(), 2);
            assert!(s.contains(&(vec![1, 2, 3, 4].into(), vec![5, 6].into())));
            assert!(s.contains(&(vec![3, 4, 5, 6].into(), vec![7].into())));
        }
        {
            let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
            let it = db.unordered_iterator();
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
        {
            let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
            let mut it = db.unordered_iterator();
            assert!(it.next().is_none());
            db.insert(vec![1, 2, 3, 4, 6], vec![1]).unwrap();
            db.insert(vec![1, 2, 3, 4, 5], vec![2]).unwrap();
            db.insert(vec![1, 2, 3, 4, 10], vec![3]).unwrap();
            db.insert(vec![3, 4, 5, 6], vec![7]).unwrap();
            let it =
                db.range_ordered_iterator(vec![1, 2, 3, 4, 0].into()..vec![1, 2, 3, 4, 10].into());
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
            let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
            let it =
                db.range_ordered_iterator(vec![1, 2, 3, 4, 0].into()..vec![1, 2, 3, 4, 10].into());
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
        {
            let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
            assert!(db.is_empty());
            db.insert(vec![1, 2, 3, 4, 0], vec![1]).unwrap();
            assert!(!db.is_empty());
        }
        {
            let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
            assert!(!db.is_empty());
        }
    }

    #[test]
    fn test_small_keys() {
        let dir = tempdir::TempDir::new("test-small-keys").unwrap();
        let config = Arc::new(Config::small());
        {
            let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
            db.insert(vec![], vec![1]).unwrap();
            db.insert(vec![1], vec![2]).unwrap();
            db.insert(vec![1, 2], vec![3]).unwrap();
            assert_eq!(db.get(&[]).unwrap(), Some(vec![1].into()));
            assert_eq!(db.get(&[1]).unwrap(), Some(vec![2].into()));
            assert_eq!(db.get(&[1, 2]).unwrap(), Some(vec![3].into()));
        }
        {
            let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
            assert_eq!(db.get(&[]).unwrap(), Some(vec![1].into()));
            assert_eq!(db.get(&[1]).unwrap(), Some(vec![2].into()));
            assert_eq!(db.get(&[1, 2]).unwrap(), Some(vec![3].into()));
        }
    }

    #[test]
    fn test_last_in_range() {
        let dir = tempdir::TempDir::new("test-last-in-range").unwrap();
        let config = Arc::new(Config::small());
        let db = Arc::new(Db::open(dir.path(), config.clone(), Metrics::new()).unwrap());
        db.insert(vec![1, 2, 3, 4, 6], vec![1]).unwrap();
        db.insert(vec![1, 2, 3, 4, 5], vec![2]).unwrap();
        db.insert(vec![1, 2, 3, 4, 10], vec![3]).unwrap();
        assert_eq!(
            db.last_in_range(&vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 8].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 6].into(), vec![1].into()))
        );
        assert_eq!(
            db.last_in_range(&vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 6].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 6].into(), vec![1].into()))
        );
        assert_eq!(
            db.last_in_range(&vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 5].into())
                .unwrap(),
            Some((vec![1, 2, 3, 4, 5].into(), vec![2].into()))
        );
        assert_eq!(
            db.last_in_range(&vec![1, 2, 3, 4, 4].into(), &vec![1, 2, 3, 4, 4].into())
                .unwrap(),
            None
        );
    }
}
