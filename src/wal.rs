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
    map: Bytes,
    position: AtomicFragPosition,
}

struct Wal {
    file: File,
    layout: FragLayout,
    maps: Mutex<BTreeMap<u64, Bytes>>,
}

pub struct WalIterator {
    wal: Arc<Wal>,
    map: Bytes,
    position: u64,
    layout: FragLayout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FragPosition(u32);

impl WalWriter {
    pub fn open(p: &impl AsRef<Path>, layout: FragLayout) -> io::Result<WalIterator> {
        layout.assert_layout();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(p)?;
        file.set_len(layout.frag_size)?;
        let map = unsafe {
            memmap2::MmapOptions::new()
                .len(layout.frag_size as usize)
                .map_mut(&file)?
        };
        let map = map.into();
        let reader = Wal::from_file(file, layout.clone());
        let iterator = WalIterator {
            wal: reader,
            map,
            position: 0,
            layout,
        };
        Ok(iterator)
    }

    pub fn write(&self, w: &PreparedWalWrite) -> Result<FragPosition, WalFullError> {
        let len = w.frame.len_with_header() as u64;
        let len_aligned = align(len);
        let Some(pos) = self.position.allocate_position(len_aligned) else {
            return Err(WalFullError);
        };
        // safety: pos calculation logic guarantees non-overlapping writes
        // position only available after write here completes
        let buf = unsafe {
            let pos = pos as usize;
            let len = len as usize;
            #[allow(mutable_transmutes)] // is there a better way?
            mem::transmute::<&[u8], &mut [u8]>(&self.map[pos..pos + len])
        };
        buf.copy_from_slice(w.frame.as_ref());
        // conversion to u32 is safe - pos is less than self.frag_size,
        // and self.frag_size is asserted less than u32::MAX
        Ok(FragPosition(pos as u32))
    }

    pub fn reader(&self) -> Arc<Wal> {
        self.wal.clone()
    }
}

#[derive(Clone)]
struct AtomicFragPosition {
    position: Arc<AtomicU64>,
    layout: FragLayout,
}

impl AtomicFragPosition {
    /// Allocate new position according to layout or None if ran out of space
    pub fn allocate_position(&self, len_aligned: u64) -> Option<u64> {
        assert!(len_aligned > 0);
        let mut position: Option<u64> = None;
        // todo aggressive multi-thread test for this
        self.position
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |pos| {
                let pos = self.layout.next_position(pos, len_aligned);
                if let Some(pos) = pos {
                    position = Some(pos);
                    Some(pos + len_aligned)
                } else {
                    position = None;
                    None
                }
            })
            .ok();
        position
    }
}

#[derive(Clone)]
pub struct FragLayout {
    pub(crate) frag_size: u64,
    pub(crate) map_size: u64,
}

impl FragLayout {
    fn assert_layout(&self) {
        assert!(self.frag_size <= u32::MAX as u64, "Frag size too large");
        assert!(
            self.map_size <= self.frag_size,
            "Map size larger then frag size"
        );
        assert_eq!(
            (self.frag_size / self.map_size) * self.map_size,
            self.frag_size,
            "Frag size should be divisible by map size"
        );
        assert_eq!(
            self.frag_size,
            align(self.frag_size),
            "Frag size not aligned"
        );
        assert_eq!(self.map_size, align(self.map_size), "Map size not aligned")
    }

    /// Allocate the next position.
    /// Block should not cross the map boundary defined by the self.map_size
    /// End of the block should not exceed self.frag_size (returns None otherwise)
    fn next_position(&self, mut pos: u64, len_aligned: u64) -> Option<u64> {
        assert!(
            len_aligned <= self.map_size,
            "Entry({len_aligned}) is larger then map_size({})",
            self.map_size
        );
        let map_start = self.locate(pos).0;
        let map_end = self.locate(pos + len_aligned - 1).0;
        if map_start != map_end {
            pos = (map_start + 1) * self.map_size;
        }
        if pos + len_aligned > self.frag_size {
            None
        } else {
            Some(pos)
        }
    }

    /// Return number of a mapping and offset inside the mapping for given position
    #[inline]
    fn locate(&self, pos: u64) -> (u64, u64) {
        (pos / self.map_size, pos % self.map_size)
    }

    /// Return range of a particular mapping
    fn map_range(&self, map: u64) -> Range<u64> {
        let start = self.map_size * map;
        let end = self.map_size * (map + 1);
        assert!(end <= self.frag_size);
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
        let (map, offset) = self.layout.locate(pos.0 as u64);
        let mut maps = self.maps.lock();
        let b = match maps.entry(map) {
            Entry::Vacant(va) => {
                let range = self.layout.map_range(map);
                let mmap = unsafe {
                    memmap2::MmapOptions::new()
                        .offset(range.start)
                        .len(self.layout.map_size as usize)
                        .map(&self.file)?
                };
                va.insert(mmap.into())
            }
            Entry::Occupied(oc) => oc.into_mut(),
        };
        Ok(CrcFrame::read_from_checked_with_len(b, offset as usize)?)
    }
}

impl WalIterator {
    pub fn next(&mut self) -> Result<(FragPosition, Bytes), CrcReadError> {
        let position = FragPosition(self.position as u32);
        let frame = CrcFrame::read_from_checked_with_len(&self.map, self.position as usize)?;
        self.position += align((frame.len() + CrcFrame::CRC_LEN_HEADER_LENGTH) as u64);
        Ok((position, frame))
    }

    pub fn into_writer(self) -> WalWriter {
        let position = AtomicFragPosition {
            position: Arc::new(AtomicU64::new(self.position)),
            layout: self.layout,
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
    pub const INVALID: FragPosition = FragPosition(u32::MAX);
    #[cfg(test)]
    pub const TEST: FragPosition = FragPosition(3311);

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.0);
    }

    pub fn read_from_buf(buf: &mut impl Buf) -> Self {
        Self(buf.get_u32())
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
        let layout = FragLayout {
            frag_size: 1024,
            map_size: 1024,
        };
        let wal = WalWriter::open(&file, layout).unwrap().into_writer();
        let reader = wal.reader();
        let pos = wal.write(&PreparedWalWrite::new(&vec![1, 2, 3])).unwrap();
        let data = reader.read(pos).unwrap();
        assert_eq!(&[1, 2, 3], data.as_ref());
        let pos = wal.write(&PreparedWalWrite::new(&vec![])).unwrap();
        let data = reader.read(pos).unwrap();
        assert_eq!(&[] as &[u8], data.as_ref());
        let large = vec![1u8; 1024 - 8 - CrcFrame::CRC_LEN_HEADER_LENGTH * 3];
        let pos = wal.write(&PreparedWalWrite::new(&large)).unwrap();
        let data = reader.read(pos).unwrap();
        assert_eq!(&large, data.as_ref());
        let err = wal.write(&PreparedWalWrite::new(&vec![]));
        err.expect_err("Must fail");
        drop(wal);
        drop(reader);
        let layout = FragLayout {
            frag_size: 2048,
            map_size: 1024,
        };
        let mut wal = WalWriter::open(&file, layout.clone()).unwrap();
        assert_bytes(&[1, 2, 3], wal.next());
        assert_bytes(&[], wal.next());
        assert_bytes(&large, wal.next());
        wal.next().expect_err("Error expected");
        let wal = wal.into_writer();
        let reader = wal.reader();
        let pos = wal
            .write(&PreparedWalWrite::new(&vec![91, 92, 93]))
            .unwrap();
        let data = reader.read(pos).unwrap();
        assert_eq!(&[91, 92, 93], data.as_ref());
        drop(wal);
        drop(reader);
        let mut wal = WalWriter::open(&file, layout.clone()).unwrap();
        assert_bytes(&[1, 2, 3], wal.next());
        assert_bytes(&[], wal.next());
        assert_bytes(&large, wal.next());
        assert_bytes(&[91, 92, 93], wal.next());
        wal.next().expect_err("Error expected");
        drop(wal);
        let mut wal = WalWriter::open(&file, layout.clone()).unwrap();
        let p1 = wal.next().unwrap().0;
        let p2 = wal.next().unwrap().0;
        let p3 = wal.next().unwrap().0;
        let p4 = wal.next().unwrap().0;
        wal.next().expect_err("Error expected");
        drop(wal);
        let reader = Wal::open(&file, layout.clone()).unwrap();
        assert_eq!(&[1, 2, 3], reader.read(p1).unwrap().as_ref());
        assert_eq!(&[] as &[u8], reader.read(p2).unwrap().as_ref());
        assert_eq!(&large, reader.read(p3).unwrap().as_ref());
        assert_eq!(&[91, 92, 93], reader.read(p4).unwrap().as_ref());
    }

    #[test]
    fn test_atomic_frag_position() {
        let layout = FragLayout {
            frag_size: 512,
            map_size: 128,
        };
        let position = AtomicFragPosition {
            layout,
            position: Arc::new(AtomicU64::new(0)),
        };
        assert_eq!(Some(0), position.allocate_position(16));
        assert_eq!(Some(16), position.allocate_position(8));
        assert_eq!(Some(24), position.allocate_position(8));
        assert_eq!(Some(128), position.allocate_position(100));
        assert_eq!(Some(256), position.allocate_position(128));
        assert_eq!(Some(256 + 128), position.allocate_position(120));
        assert_eq!(None, position.allocate_position(9)); // just one over 8 bytes left
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
    fn assert_bytes(e: &[u8], v: Result<(FragPosition, Bytes), CrcReadError>) {
        let v = v.expect("Expected value, bot nothing");
        assert_eq!(e, v.1.as_ref());
    }
}
