use crate::config::Config;
use crate::control::ControlRegion;
use crate::crc::{CrcFrame, IntoBytesFixed};
use crate::large_table::{
    IndexTable, LargeTable, LargeTableSnapshot, LargeTableSnapshotEntry, Loader,
};
use crate::wal::{PreparedWalWrite, Wal, WalError, WalIterator, WalPosition, WalWriter};
use bytes::{Buf, BufMut, BytesMut};
use memmap2::{MmapMut, MmapOptions};
use minibytes::Bytes;
use parking_lot::RwLock;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;

pub struct Db {
    large_table: LargeTable,
    wal: Arc<Wal>,
    wal_writer: WalWriter,
    cr_map: MmapMut,
}

pub type DbResult<T> = Result<T, DbError>;

pub const MAX_KEY_LEN: usize = u16::MAX as usize;

impl Db {
    pub fn open(path: &Path, config: &Config) -> DbResult<Self> {
        let cr = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("cr"))?;
        let file_len = cr.metadata()?.len() as usize;
        let cr_len = ControlRegion::len_from_large_table_size(config.large_table_size());
        let cr_map = unsafe { MmapOptions::new().len(cr_len * 2).map_mut(&cr)? };
        let control_region = if file_len != cr_len * 2 {
            // cr.set_len((cr_len * 2) as u64)?;
            ControlRegion::new_empty(config.large_table_size())
        } else {
            Self::read_control_region(&cr_map, config)?
        };
        let large_table = LargeTable::from_unloaded(control_region.snapshot());
        let wal = Wal::open(&path.join("wal"), config.wal_layout())?;
        let wal_iterator = wal.wal_iterator(control_region.replay_from())?;
        let wal_writer = Self::replay_wal(&large_table, wal_iterator)?;
        Ok(Self {
            cr_map,
            large_table,
            wal_writer,
            wal,
        })
    }

    fn read_control_region(cr_map: &MmapMut, config: &Config) -> Result<ControlRegion, DbError> {
        let cr_len = ControlRegion::len_from_large_table_size(config.large_table_size());
        assert_eq!(cr_map.len(), cr_len * 2);
        let cr1 = CrcFrame::read_from_checked_no_len(&cr_map[..cr_len]);
        let cr2 = CrcFrame::read_from_checked_no_len(&cr_map[cr_len..]);
        let cr = match (cr1, cr2) {
            (Ok(cr1), Err(_)) => cr1,
            (Err(_), Ok(cr2)) => cr2,
            (Ok(cr1), Ok(cr2)) => {
                let version1 = ControlRegion::version_from_bytes(cr1);
                let version2 = ControlRegion::version_from_bytes(cr2);
                if version1 > version2 {
                    cr1
                } else {
                    cr2
                }
            }
            (Err(_), Err(_)) => return Err(DbError::CrCorrupted),
        };
        Ok(ControlRegion::from_bytes(&cr, config.large_table_size()))
    }

    pub fn insert(&self, k: Bytes, v: Bytes) -> DbResult<()> {
        assert!(k.len() <= MAX_KEY_LEN, "Key exceeding max key length");
        let w = PreparedWalWrite::new(&WalEntry::Record(k.clone(), v));
        let position = self.wal_writer.write(&w)?;
        self.large_table.insert(k, position, &*self.wal)?;
        Ok(())
    }

    pub fn get(&self, k: &[u8]) -> DbResult<Option<Bytes>> {
        let Some(position) = self.large_table.get(k, &*self.wal)? else {
            return Ok(None);
        };
        let entry = Self::read_entry(&self.wal, position)?;
        let value = if let WalEntry::Record(wal_key, v) = entry {
            debug_assert_eq!(wal_key.as_ref(), k);
            v
        } else {
            panic!("Unexpected wal entry where expected record");
        };
        Ok(Some(value))
    }

    fn replay_wal(large_table: &LargeTable, mut wal_iterator: WalIterator) -> DbResult<WalWriter> {
        loop {
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_))) {
                break Ok(wal_iterator.into_writer());
            }
            let (position, entry) = entry?;
            let entry = WalEntry::from_bytes(entry);
            match entry {
                WalEntry::Record(k, _v) => {
                    large_table.insert(k, position, wal_iterator.wal())?;
                }
                WalEntry::Index(_bytes) => {
                    // todo - handle this by updating large table to Loaded()
                }
            }
        }
    }

    fn rebuild_cr(&self) -> DbResult<()> {
        let snapshot = self.large_table.snapshot();
        let snapshot = self.write_snapshot(snapshot)?;

        Ok(())
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
                    let index = bincode::serialize(&index)?;
                    let index = index.into();
                    let w = PreparedWalWrite::new(&WalEntry::Index(index));
                    let position = self.wal_writer.write(&w)?;
                    index_updates.push((i, version, position));
                    Ok(position)
                }
            })
            .collect::<DbResult<Box<[WalPosition]>>>()?;
        self.large_table.maybe_update_entries(index_updates);
        Ok(snapshot)
    }

    fn read_entry(wal: &Wal, position: WalPosition) -> DbResult<WalEntry> {
        let entry = wal.read(position)?;
        Ok(WalEntry::from_bytes(entry))
    }
}

impl Loader for &Wal {
    type Error = DbError;

    fn load(self, position: WalPosition) -> DbResult<IndexTable> {
        let entry = Db::read_entry(self, position)?;
        if let WalEntry::Index(bytes) = entry {
            let entry = bincode::deserialize(&bytes)?;
            Ok(entry)
        } else {
            panic!("Unexpected wal entry where expected record");
        }
    }
}

enum WalEntry {
    Record(Bytes, Bytes),
    Index(Bytes),
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
            _ => panic!("Unknown wal entry type {entry_type}"),
        }
    }
}

impl IntoBytesFixed for WalEntry {
    fn len(&self) -> usize {
        match self {
            WalEntry::Record(k, v) => 4 + k.len() + v.len(),
            WalEntry::Index(index) => 2 + index.len(),
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
        let config = Config::small();
        {
            let db = Db::open(dir.path(), &config).unwrap();
            db.insert(vec![1, 2, 3, 4].into(), vec![5, 6].into())
                .unwrap();
            db.insert(vec![3, 4, 5, 6].into(), vec![7].into()).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
        {
            let db = Db::open(dir.path(), &config).unwrap();
            assert_eq!(Some(vec![5, 6].into()), db.get(&[1, 2, 3, 4]).unwrap());
            assert_eq!(Some(vec![7].into()), db.get(&[3, 4, 5, 6]).unwrap());
        }
    }
}
