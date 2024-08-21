use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use bytes::{Buf, BufMut};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{io, mem};

#[derive(Clone)]
pub struct WalWriter {
    wal: Arc<Wal>,
    map: Map,
    position: AtomicFragPosition,
}

struct Wal {
    file: File,
    layout: FragLayout,
    maps: Mutex<BTreeMap<u64, Bytes>>,
}

pub struct WalIterator {
    wal: Arc<Wal>,
    map: Map,
    position: u64,
}

#[derive(Clone)]
struct Map {
    id: u64,
    data: Bytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FragPosition(u64);

impl WalWriter {
    pub fn write(&mut self, w: &PreparedWalWrite) -> Result<FragPosition, WalFullError> {
        let len = w.frame.len_with_header() as u64;
        let len_aligned = align(len);
        let pos = self.position.allocate_position(len_aligned);
        // todo duplicated code
        let (map_id, offset) = self.wal.layout.locate(pos);
        if self.map.id != map_id {
            // todo put skip marker
            self.map = self.wal.map(map_id).unwrap(); // todo fix unwrap
        }
        // safety: pos calculation logic guarantees non-overlapping writes
        // position only available after write here completes
        let buf = unsafe {
            let offset = offset as usize;
            let len = len as usize;
            #[allow(mutable_transmutes)] // is there a better way?
            mem::transmute::<&[u8], &mut [u8]>(&self.map.data[offset..offset + len])
        };
        buf.copy_from_slice(w.frame.as_ref());
        // conversion to u32 is safe - pos is less than self.frag_size,
        // and self.frag_size is asserted less than u32::MAX
        Ok(FragPosition(pos))
    }
}

#[derive(Clone)]
struct AtomicFragPosition {
    position: Arc<AtomicU64>,
    layout: FragLayout,
}

impl AtomicFragPosition {
    /// Allocate new position according to layout or None if ran out of space
    pub fn allocate_position(&self, len_aligned: u64) -> u64 {
        assert!(len_aligned > 0);
        let mut position: Option<u64> = None;
        // todo aggressive multi-thread test for this
        self.position
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |pos| {
                let pos = self.layout.next_position(pos, len_aligned);
                position = Some(pos);
                Some(pos + len_aligned)
            })
            .ok();
        position.unwrap()
    }
}

#[derive(Clone)]
pub struct FragLayout {
    pub(crate) frag_size: u64,
}

impl FragLayout {
    fn assert_layout(&self) {
        assert!(self.frag_size <= u32::MAX as u64, "Frag size too large");
        assert_eq!(
            self.frag_size,
            align(self.frag_size),
            "Frag size not aligned"
        );
    }

    /// Allocate the next position.
    /// Block should not cross the map boundary defined by the self.frag_size
    fn next_position(&self, mut pos: u64, len_aligned: u64) -> u64 {
        assert!(
            len_aligned <= self.frag_size,
            "Entry({len_aligned}) is larger then frag_size({})",
            self.frag_size
        );
        let map_start = self.locate(pos).0;
        let map_end = self.locate(pos + len_aligned - 1).0;
        if map_start != map_end {
            pos = (map_start + 1) * self.frag_size;
        }
        pos
    }

    /// Return number of a mapping and offset inside the mapping for given position
    #[inline]
    fn locate(&self, pos: u64) -> (u64, u64) {
        (pos / self.frag_size, pos % self.frag_size)
    }

    /// Return range of a particular mapping
    fn map_range(&self, map: u64) -> Range<u64> {
        let start = self.frag_size * map;
        let end = self.frag_size * (map + 1);
        start..end
    }
}

const fn align(l: u64) -> u64 {
    const ALIGN: u64 = 8;
    (l + ALIGN - 1) / ALIGN * ALIGN
}

#[derive(Debug)]
pub struct WalFullError;

impl Wal {
    pub fn open(p: &Path, layout: FragLayout) -> io::Result<Arc<Self>> {
        layout.assert_layout();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(p)?;
        file.set_len(layout.frag_size)?;
        Ok(Self::from_file(file, layout))
    }

    fn from_file(file: File, layout: FragLayout) -> Arc<Self> {
        let reader = Wal {
            file,
            layout,
            maps: Default::default(),
        };
        Arc::new(reader)
    }

    pub fn read(&self, pos: FragPosition) -> Result<Bytes, WalReadError> {
        let (map, offset) = self.layout.locate(pos.0);
        let map = self.map(map)?;
        // todo avoid clone, introduce Bytes::slice_in_place
        Ok(CrcFrame::read_from_checked_with_len(
            &map.data,
            offset as usize,
        )?)
    }

    fn map(&self, id: u64) -> io::Result<Map> {
        let mut maps = self.maps.lock();
        let b = match maps.entry(id) {
            Entry::Vacant(va) => {
                let range = self.layout.map_range(id);
                let mmap = unsafe {
                    // todo - some mappings can be read-only
                    memmap2::MmapOptions::new()
                        .offset(range.start)
                        .len(self.layout.frag_size as usize)
                        .map_mut(&self.file)?
                };
                va.insert(mmap.into())
            }
            Entry::Occupied(oc) => oc.into_mut(),
        };
        let map = Map {
            id,
            data: b.clone(),
        };
        Ok(map)
    }

    pub fn wal_iterator(self: &Arc<Self>) -> WalIterator {
        let map = self.map(0).unwrap(); // todo fix unwrap
        WalIterator {
            wal: self.clone(),
            position: 0,
            map,
        }
    }

    pub fn writer(self: &Arc<Self>) -> WalWriter {
        self.wal_iterator().into_writer()
    }
}

impl WalIterator {
    pub fn next(&mut self) -> Result<(FragPosition, Bytes), CrcReadError> {
        // todo duplicated code
        let (map_id, offset) = self.wal.layout.locate(self.position);
        if self.map.id != map_id {
            // todo handle skip marker
            self.map = self.wal.map(map_id).unwrap(); // todo fix unwrap
        }
        let frame = CrcFrame::read_from_checked_with_len(&self.map.data, offset as usize)?;
        let position = FragPosition(self.position);
        self.position += align((frame.len() + CrcFrame::CRC_LEN_HEADER_LENGTH) as u64);
        Ok((position, frame))
    }

    pub fn into_writer(self) -> WalWriter {
        let position = AtomicFragPosition {
            position: Arc::new(AtomicU64::new(self.position)),
            layout: self.wal.layout.clone(),
        };
        WalWriter {
            wal: self.wal,
            map: self.map,
            position,
        }
    }
}

pub struct PreparedWalWrite {
    frame: CrcFrame,
}

impl PreparedWalWrite {
    pub fn new(t: &impl IntoBytesFixed) -> Self {
        let frame = CrcFrame::from_bytes_fixed_with_len(t);
        Self { frame }
    }
}

impl FragPosition {
    pub const INVALID: FragPosition = FragPosition(u64::MAX);
    #[cfg(test)]
    pub const TEST: FragPosition = FragPosition(3311);

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.0);
    }

    pub fn read_from_buf(buf: &mut impl Buf) -> Self {
        Self(buf.get_u64())
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum WalReadError {
    Io(io::Error),
    Crc(CrcReadError),
}

impl From<CrcReadError> for WalReadError {
    fn from(value: CrcReadError) -> Self {
        Self::Crc(value)
    }
}

impl From<io::Error> for WalReadError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let file = dir.path().join("wal");
        let layout = FragLayout { frag_size: 1024 };
        let wal = Wal::open(&file, layout.clone()).unwrap();
        let mut writer = wal.writer();
        let pos = writer
            .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
            .unwrap();
        let data = wal.read(pos).unwrap();
        assert_eq!(&[1, 2, 3], data.as_ref());
        let pos = writer.write(&PreparedWalWrite::new(&vec![])).unwrap();
        let data = wal.read(pos).unwrap();
        assert_eq!(&[] as &[u8], data.as_ref());
        let large = vec![1u8; 1024 - 8 - CrcFrame::CRC_LEN_HEADER_LENGTH * 3];
        let pos = writer.write(&PreparedWalWrite::new(&large)).unwrap();
        let data = wal.read(pos).unwrap();
        assert_eq!(&large, data.as_ref());
        drop(writer);
        drop(wal);
        let wal = Wal::open(&file, layout.clone()).unwrap();
        let mut wal_iterator = wal.wal_iterator();
        assert_bytes(&[1, 2, 3], wal_iterator.next());
        assert_bytes(&[], wal_iterator.next());
        assert_bytes(&large, wal_iterator.next());
        wal_iterator.next().expect_err("Error expected");
        let mut writer = wal_iterator.into_writer();
        let pos = writer
            .write(&PreparedWalWrite::new(&vec![91, 92, 93]))
            .unwrap();
        let data = wal.read(pos).unwrap();
        assert_eq!(&[91, 92, 93], data.as_ref());
        drop(writer);
        drop(wal);
        let wal = Wal::open(&file, layout.clone()).unwrap();
        let mut wal_iterator = wal.wal_iterator();
        let p1 = assert_bytes(&[1, 2, 3], wal_iterator.next());
        let p2 = assert_bytes(&[], wal_iterator.next());
        let p3 = assert_bytes(&large, wal_iterator.next());
        let p4 = assert_bytes(&[91, 92, 93], wal_iterator.next());
        wal_iterator.next().expect_err("Error expected");
        drop(wal_iterator);
        let wal = Wal::open(&file, layout.clone()).unwrap();
        assert_eq!(&[1, 2, 3], wal.read(p1).unwrap().as_ref());
        assert_eq!(&[] as &[u8], wal.read(p2).unwrap().as_ref());
        assert_eq!(&large, wal.read(p3).unwrap().as_ref());
        assert_eq!(&[91, 92, 93], wal.read(p4).unwrap().as_ref());
    }

    #[test]
    fn test_atomic_frag_position() {
        let layout = FragLayout { frag_size: 512 };
        let position = AtomicFragPosition {
            layout,
            position: Arc::new(AtomicU64::new(0)),
        };
        assert_eq!(0, position.allocate_position(16));
        assert_eq!(16, position.allocate_position(8));
        assert_eq!(24, position.allocate_position(8));
        assert_eq!(32, position.allocate_position(104));
        assert_eq!(136, position.allocate_position(128));
        assert_eq!(264, position.allocate_position(240));
        assert_eq!(512, position.allocate_position(16));
        assert_eq!(512 + 16, position.allocate_position(32));
    }

    #[test]
    fn test_align() {
        assert_eq!(align(1), 8);
        assert_eq!(align(4), 8);
        assert_eq!(align(7), 8);
        assert_eq!(align(0), 0);
        assert_eq!(align(8), 8);
        assert_eq!(align(15), 16);
        assert_eq!(align(16), 16);
    }

    #[track_caller]
    fn assert_bytes(e: &[u8], v: Result<(FragPosition, Bytes), CrcReadError>) -> FragPosition {
        let v = v.expect("Expected value, bot nothing");
        assert_eq!(e, v.1.as_ref());
        v.0
    }
}
