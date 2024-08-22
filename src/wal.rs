use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use bytes::{Buf, BufMut};
use memmap2::{Mmap, MmapMut};
use minibytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
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
    map: Arc<Mutex<Map>>,
    position: AtomicWalPosition,
}

// todo periodically clear maps
pub struct Wal {
    file: File,
    layout: WalLayout,
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
    writeable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct WalPosition(u64);

impl WalWriter {
    pub fn write(&self, w: &PreparedWalWrite) -> Result<WalPosition, WalError> {
        let len = w.frame.len_with_header() as u64;
        let len_aligned = align(len);
        let (pos, prev_block_end) = self.position.allocate_position(len_aligned);
        // todo duplicated code
        let (map_id, offset) = self.wal.layout.locate(pos);
        let mut map = self.map.lock();
        // todo - decide whether map is covered by mutex or we want concurrent writes
        if map.id != map_id {
            if pos != prev_block_end {
                let (prev_map, prev_offset) = self.wal.layout.locate(prev_block_end);
                assert_eq!(prev_map, map.id);
                let skip_marker = CrcFrame::skip_marker();
                let buf = map.write_buf_at(prev_offset as usize, skip_marker.as_ref().len());
                buf.copy_from_slice(skip_marker.as_ref());
            }
            // todo put skip marker
            self.wal.extend_to_map(map_id)?;
            *map = self.wal.map(map_id, true)?;
        } else {
            assert_eq!(pos, prev_block_end);
        }
        // safety: pos calculation logic guarantees non-overlapping writes
        // position only available after write here completes
        let buf = map.write_buf_at(offset as usize, len as usize);
        buf.copy_from_slice(w.frame.as_ref());
        // conversion to u32 is safe - pos is less than self.frag_size,
        // and self.frag_size is asserted less than u32::MAX
        Ok(WalPosition(pos))
    }
}

#[derive(Clone)]
struct AtomicWalPosition {
    position: Arc<AtomicU64>,
    layout: WalLayout,
}

impl AtomicWalPosition {
    /// Allocate new position according to layout
    ///
    /// Returns new position and then end of previous block
    pub fn allocate_position(&self, len_aligned: u64) -> (u64, u64) {
        assert!(len_aligned > 0);
        let mut position: Option<(u64, u64)> = None;
        // todo aggressive multi-thread test for this
        self.position
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev_pos| {
                let pos = self.layout.next_position(prev_pos, len_aligned);
                position = Some((pos, prev_pos));
                Some(pos + len_aligned)
            })
            .ok();
        position.unwrap()
    }
}

#[derive(Clone)]
pub struct WalLayout {
    pub(crate) frag_size: u64,
}

impl WalLayout {
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

impl Wal {
    pub fn open(p: &Path, layout: WalLayout) -> io::Result<Arc<Self>> {
        layout.assert_layout();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(p)?;
        Ok(Self::from_file(file, layout))
    }

    fn from_file(file: File, layout: WalLayout) -> Arc<Self> {
        let reader = Wal {
            file,
            layout,
            maps: Default::default(),
        };
        Arc::new(reader)
    }

    pub fn read(&self, pos: WalPosition) -> Result<Bytes, WalError> {
        let (map, offset) = self.layout.locate(pos.0);
        let map = self.map(map, false)?;
        // todo avoid clone, introduce Bytes::slice_in_place
        Ok(CrcFrame::read_from_bytes(&map.data, offset as usize)?)
    }

    fn map(&self, id: u64, write: bool) -> io::Result<Map> {
        let mut maps = self.maps.lock();
        let b = match maps.entry(id) {
            Entry::Vacant(va) => {
                let range = self.layout.map_range(id);
                let mmap = unsafe {
                    let mut options = memmap2::MmapOptions::new();
                    options
                        .offset(range.start)
                        .len(self.layout.frag_size as usize);
                    if write {
                        options.populate().map_mut(&self.file)?.into()
                    } else {
                        options.map(&self.file)?.into()
                    }
                };
                va.insert(mmap)
            }
            Entry::Occupied(oc) => oc.into_mut(),
        };
        let map = Map {
            id,
            data: b.clone(),
            writeable: write,
        };
        Ok(map)
    }

    /// Resize file to fit the specified map id
    fn extend_to_map(&self, id: u64) -> io::Result<()> {
        let range = self.layout.map_range(id);
        let len = self.file.metadata()?.len();
        if len < range.end {
            self.file.set_len(range.end)?;
        }
        Ok(())
    }

    pub fn wal_iterator(self: &Arc<Self>, position: WalPosition) -> io::Result<WalIterator> {
        let (map_id, _) = self.layout.locate(position.0);
        self.extend_to_map(map_id)?;
        let map = self.map(map_id, true)?;
        Ok(WalIterator {
            wal: self.clone(),
            position: position.0,
            map,
        })
    }

    // Attempts cleaning internal mem maps, returning number of retained maps
    // Map can be freed when all buffers linked to this portion of a file are dropped
    pub fn cleanup(&self) -> usize {
        let mut maps = self.maps.lock();
        maps.retain(|_k, v| {
            v.downcast_mut::<Mmap>().is_none() &&
            // todo maybe don't put writeable maps into self.maps?
            v.downcast_mut::<MmapMut>().is_none()
        });
        maps.len()
    }
}

impl WalIterator {
    pub fn next(&mut self) -> Result<(WalPosition, Bytes), WalError> {
        let frame = self.read_one();
        let frame = if matches!(frame, Err(WalError::Crc(CrcReadError::SkipMarker))) {
            // handle skip marker - jump to next frag
            let next_map = self.map.id + 1;
            self.position = self.wal.layout.map_range(next_map).start;
            self.read_one()?
        } else {
            frame?
        };
        let position = WalPosition(self.position);
        self.position += align((frame.len() + CrcFrame::CRC_HEADER_LENGTH) as u64);
        Ok((position, frame))
    }

    fn read_one(&mut self) -> Result<Bytes, WalError> {
        // todo duplicated code
        let (map_id, offset) = self.wal.layout.locate(self.position);
        if self.map.id != map_id {
            self.wal.extend_to_map(map_id)?;
            self.map = self.wal.map(map_id, true)?;
        }
        Ok(CrcFrame::read_from_bytes(&self.map.data, offset as usize)?)
    }

    pub fn into_writer(self) -> WalWriter {
        let position = AtomicWalPosition {
            position: Arc::new(AtomicU64::new(self.position)),
            layout: self.wal.layout.clone(),
        };
        WalWriter {
            wal: self.wal,
            map: Arc::new(Mutex::new(self.map)),
            position,
        }
    }

    pub fn wal(&self) -> &Wal {
        &self.wal
    }
}

impl Map {
    pub fn write_buf_at(&self, offset: usize, len: usize) -> &mut [u8] {
        assert!(self.writeable, "Attempt to write into read-only map");
        unsafe {
            #[allow(mutable_transmutes)] // is there a better way?
            mem::transmute::<&[u8], &mut [u8]>(&self.data[offset..offset + len])
        }
    }
}

pub struct PreparedWalWrite {
    frame: CrcFrame,
}

impl PreparedWalWrite {
    pub fn new(t: &impl IntoBytesFixed) -> Self {
        let frame = CrcFrame::new(t);
        Self { frame }
    }
}

impl WalPosition {
    pub const INVALID: WalPosition = WalPosition(u64::MAX);
    pub const ZERO: WalPosition = WalPosition(0);
    pub const LENGTH: usize = 8;
    #[cfg(test)]
    pub const TEST: WalPosition = WalPosition(3311);

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.0);
    }

    pub fn read_from_buf(buf: &mut impl Buf) -> Self {
        Self(buf.get_u64())
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    Crc(CrcReadError),
}

impl From<CrcReadError> for WalError {
    fn from(value: CrcReadError) -> Self {
        Self::Crc(value)
    }
}

impl From<io::Error> for WalError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::fs;

    #[test]
    fn test_wal() {
        let dir = tempdir::TempDir::new("test-wal").unwrap();
        let file = dir.path().join("wal");
        let layout = WalLayout { frag_size: 1024 };
        // todo - add second test case when there is no space for skip marker after large
        let large = vec![1u8; 1024 - 8 - CrcFrame::CRC_HEADER_LENGTH * 3 - 9];
        {
            let wal = Wal::open(&file, layout.clone()).unwrap();
            let writer = wal.wal_iterator(WalPosition::ZERO).unwrap().into_writer();
            let pos = writer
                .write(&PreparedWalWrite::new(&vec![1, 2, 3]))
                .unwrap();
            let data = wal.read(pos).unwrap();
            assert_eq!(&[1, 2, 3], data.as_ref());
            let pos = writer.write(&PreparedWalWrite::new(&vec![])).unwrap();
            let data = wal.read(pos).unwrap();
            assert_eq!(&[] as &[u8], data.as_ref());
            drop(data);
            let pos = writer.write(&PreparedWalWrite::new(&large)).unwrap();
            let data = wal.read(pos).unwrap();
            assert_eq!(&large, data.as_ref());
        }
        {
            let wal = Wal::open(&file, layout.clone()).unwrap();
            let mut wal_iterator = wal.wal_iterator(WalPosition::ZERO).unwrap();
            assert_bytes(&[1, 2, 3], wal_iterator.next());
            assert_bytes(&[], wal_iterator.next());
            assert_bytes(&large, wal_iterator.next());
            wal_iterator.next().expect_err("Error expected");
            let writer = wal_iterator.into_writer();
            let pos = writer
                .write(&PreparedWalWrite::new(&vec![91, 92, 93]))
                .unwrap();
            assert_eq!(pos.0, 1024); // assert we skipped over to next frag
            let data = wal.read(pos).unwrap();
            assert_eq!(&[91, 92, 93], data.as_ref());
        }
        {
            let wal = Wal::open(&file, layout.clone()).unwrap();
            let mut wal_iterator = wal.wal_iterator(WalPosition::ZERO).unwrap();
            let p1 = assert_bytes(&[1, 2, 3], wal_iterator.next());
            let p2 = assert_bytes(&[], wal_iterator.next());
            let p3 = assert_bytes(&large, wal_iterator.next());
            let p4 = assert_bytes(&[91, 92, 93], wal_iterator.next());
            wal_iterator.next().expect_err("Error expected");
            // wal_iterator holds the reference to mapping, so can't clean all of them
            assert_eq!(wal.cleanup(), 1);
            drop(wal_iterator);
            // after wal_iterator is dropped, cleanup should free all memory
            assert_eq!(wal.cleanup(), 0);
            drop(wal);
            let wal = Wal::open(&file, layout.clone()).unwrap();
            assert_eq!(&[1, 2, 3], wal.read(p1).unwrap().as_ref());
            assert_eq!(&[] as &[u8], wal.read(p2).unwrap().as_ref());
            assert_eq!(&large, wal.read(p3).unwrap().as_ref());
            assert_eq!(&[91, 92, 93], wal.read(p4).unwrap().as_ref());
        }
        // we wrote into two frags
        assert_eq!(2048, fs::metadata(file).unwrap().len());
    }

    #[test]
    fn test_atomic_wal_position() {
        let layout = WalLayout { frag_size: 512 };
        let position = AtomicWalPosition {
            layout,
            position: Arc::new(AtomicU64::new(0)),
        };
        assert_eq!((0, 0), position.allocate_position(16));
        assert_eq!((16, 16), position.allocate_position(8));
        assert_eq!((24, 24), position.allocate_position(8));
        assert_eq!((32, 32), position.allocate_position(104));
        assert_eq!((136, 136), position.allocate_position(128));
        assert_eq!((264, 264), position.allocate_position(240));
        // Leap over frag boundary
        assert_eq!((512, 504), position.allocate_position(16));
        assert_eq!((512 + 16, 512 + 16), position.allocate_position(32));
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

    #[test]
    fn test_position() {
        let mut buf = BytesMut::new();
        WalPosition::TEST.write_to_buf(&mut buf);
        let bytes: bytes::Bytes = buf.into();
        let mut buf = bytes.as_ref();
        let position = WalPosition::read_from_buf(&mut buf);
        assert_eq!(position, WalPosition::TEST);
    }

    #[track_caller]
    fn assert_bytes(e: &[u8], v: Result<(WalPosition, Bytes), WalError>) -> WalPosition {
        let v = v.expect("Expected value, got nothing");
        assert_eq!(e, v.1.as_ref());
        v.0
    }
}
