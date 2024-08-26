use crate::batch::WriteBatch;
use crate::config::Config;
use crate::control::ControlRegion;
use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::large_table::{
    IndexTable, LargeTable, LargeTableSnapshot, LargeTableSnapshotEntry, Loader, Version,
};
use crate::metrics::Metrics;
use crate::wal::{PreparedWalWrite, Wal, WalError, WalIterator, WalPosition, WalWriter};
use bytes::{Buf, BufMut, BytesMut};
use memmap2::{MmapMut, MmapOptions};
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct Db {
    // todo - avoid read lock on reads?
    large_table: RwLock<LargeTable>,
    wal: Arc<Wal>,
    wal_writer: WalWriter,
    control_region_store: Mutex<ControlRegionStore>,
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
        })
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

    pub fn remove(&self, k: impl Into<Bytes>) -> DbResult<bool> {
        let k = k.into();
        assert!(k.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let w = PreparedWalWrite::new(&WalEntry::Remove(k.clone()));
        let position = self.wal_writer.write(&w)?;
        Ok(self.large_table.read().remove(&k, position, self)?)
    }

    pub fn get(&self, k: &[u8]) -> DbResult<Option<Bytes>> {
        let Some(position) = self.large_table.read().get(k, self)? else {
            return Ok(None);
        };
        let entry = Self::read_entry_unmapped(&self.wal, position)?;
        let value = if let WalEntry::Record(wal_key, v) = entry {
            debug_assert_eq!(wal_key.as_ref(), k);
            v
        } else {
            panic!("Unexpected wal entry where expected record");
        };
        Ok(Some(value))
    }

    pub fn write_batch(&self, batch: WriteBatch) -> DbResult<()> {
        // todo implement atomic durability
        let lock = self.large_table.read();
        for (k, w) in batch.into_writes() {
            let position = self.wal_writer.write(&w)?;
            lock.insert(k, position, self)?;
        }
        Ok(())
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
                    metrics.replayed_wal_records.fetch_add(1, Ordering::Relaxed);
                    large_table.insert(k, position, wal_iterator.wal())?;
                }
                WalEntry::Index(_bytes) => {
                    // todo - handle this by updating large table to Loaded()
                }
                WalEntry::Remove(k) => {
                    metrics.replayed_wal_records.fetch_add(1, Ordering::Relaxed);
                    large_table.remove(&k, position, wal_iterator.wal())?;
                }
            }
        }
    }

    fn rebuild_control_region(&self) -> DbResult<()> {
        let mut crs = self.control_region_store.lock();
        // drop large_table lock asap
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
                LargeTableSnapshotEntry::Dirty(version, index) => {
                    let position = self.write_index(&index)?;
                    index_updates.push((i, version, position));
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
            assert_eq!(metrics.replayed_wal_records.load(Ordering::Relaxed), 0);
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
            db.insert(vec![3, 4, 5, 6], vec![8]).unwrap();
            assert_eq!(Some(vec![8].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(dir.path(), config.clone(), metrics.clone()).unwrap();
            assert_eq!(metrics.replayed_wal_records.load(Ordering::Relaxed), 1);
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
            assert!(db.remove(vec![1, 2, 3, 4]).unwrap());
            assert_eq!(None, db.get(&[1, 2, 3, 4]).unwrap());
            assert!(!db.remove(vec![1, 2, 3, 4]).unwrap());
            assert_eq!(None, db.get(&[1, 2, 3, 4]).unwrap());
        }
        {
            let db = Db::open(dir.path(), config.clone(), Metrics::new()).unwrap();
            assert_eq!(None, db.get(&[1, 2, 3, 4]).unwrap());
            db.insert(vec![1, 2, 3, 4], vec![9, 10]).unwrap();
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
            assert_eq!(Some(vec![9, 10].into()), db.get(&[1, 2, 3, 4]).unwrap());
            db.rebuild_control_region().unwrap();
            assert!(db.remove(vec![1, 2, 3, 4]).unwrap());
        }
        {
            let metrics = Metrics::new();
            let db = Db::open(dir.path(), config.clone(), metrics.clone()).unwrap();
            assert_eq!(metrics.replayed_wal_records.load(Ordering::Relaxed), 1);
            assert_eq!(None, db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
    }
}
