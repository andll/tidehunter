use crate::config::Config;
use crate::control::ControlRegion;
use crate::crc::CrcFrame;
use crate::large_table::LargeTable;
use crate::wal::Wal;
use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;

pub struct Db {
    large_table: LargeTable,
    wal: Arc<Wal>,
    cr_map: MmapMut,
}

impl Db {
    pub fn open(path: &Path, config: &Config) -> Result<Self, DbOpenError> {
        let cr = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("cr"))?;
        let file_len = cr.metadata()?.len() as usize;
        let cr_len = ControlRegion::len_from_large_table_size(config.large_table_size());
        let cr_map = unsafe { MmapOptions::new().len(cr_len * 2).map_mut(&cr)? };
        let control_region = if file_len != cr_len * 2 {
            cr.set_len((cr_len * 2) as u64)?;
            ControlRegion::new(config.large_table_size())
        } else {
            Self::read_control_region(&cr_map, config)?
        };
        let large_table = LargeTable::from_unloaded(control_region.snapshot());
        let wal = Wal::open(&path.join("wal"), config.frag_layout())?;
        Ok(Self {
            cr_map,
            large_table,
            wal,
        })
    }

    fn read_control_region(
        cr_map: &MmapMut,
        config: &Config,
    ) -> Result<ControlRegion, DbOpenError> {
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
            (Err(_), Err(_)) => return Err(DbOpenError::CrCorrupted),
        };
        Ok(ControlRegion::from_bytes(&cr, config.large_table_size()))
    }
}

pub enum DbOpenError {
    Io(io::Error),
    CrCorrupted,
}

impl From<io::Error> for DbOpenError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn db_test() {}
}
