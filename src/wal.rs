use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use crate::lookup::{FileRange, RandomRead};
use crate::metrics::Metrics;
use bytes::{Buf, BufMut, BytesMut};
use memmap2::MmapMut;
use minibytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::os::unix::fs::FileExt;
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
    maps: RwLock<BTreeMap<u64, Map>>,
    metrics: Arc<Metrics>,
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

pub enum WalRandomRead<'a> {
    Mapped(Bytes),
    File(FileRange<'a>),
}

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
            self.wal.extend_to_map(map_id)?;
            let mmap_mut = map
                .data
                .downcast_ref::<MmapMut>()
                .expect("Can't downcast writable map to MmapMut");
            // Asynchronously flush the filled mem map
            // todo evaluate to make sure this does not hurt performance in real app
            mmap_mut.flush_async()?;
            *map = self.wal.map(map_id, true)?;
        } else {
            // todo it is possible to have a race between map mutex and pos allocation so this check may fail
            // assert_eq!(pos, align(prev_block_end));
        }
        // safety: pos calculation logic guarantees non-overlapping writes
        // position only available after write here completes
        let buf = map.write_buf_at(offset as usize, len as usize);
        buf.copy_from_slice(w.frame.as_ref());
        // conversion to u32 is safe - pos is less than self.frag_size,
        // and self.frag_size is asserted less than u32::MAX
        Ok(WalPosition(pos))
    }

    /// Current un-initialized position,
    /// not to be used as WalPosition, only as a metric to see how many bytes were written
    pub fn position(&self) -> u64 {
        self.position.position.load(Ordering::Relaxed)
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
    pub(crate) max_maps: usize,
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
    pub fn open(p: &Path, layout: WalLayout, metrics: Arc<Metrics>) -> io::Result<Arc<Self>> {
        layout.assert_layout();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(p)?;
        Ok(Self::from_file(file, layout, metrics))
    }

    fn from_file(file: File, layout: WalLayout, metrics: Arc<Metrics>) -> Arc<Self> {
        let reader = Wal {
            file,
            layout,
            maps: Default::default(),
            metrics,
        };
        Arc::new(reader)
    }

    pub fn read(&self, pos: WalPosition) -> Result<Bytes, WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.0);
        let map = self.map(map, false)?;
        // todo avoid clone, introduce Bytes::slice_in_place
        Ok(CrcFrame::read_from_bytes(&map.data, offset as usize)?)
    }

    /// Read the wal position without mapping.
    /// If mapping exists, it is still used for reading
    /// if mapping does not exist the read syscall is used instead.
    ///
    /// Returns (false, _) if read syscall was used
    /// Returns (true, _) if mapping was used
    pub fn read_unmapped(&self, pos: WalPosition) -> Result<(bool, Bytes), WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        const INITIAL_READ_SIZE: usize = 4 * 1024; // todo probably need to increase even more
        let (map, offset) = self.layout.locate(pos.0);
        if let Some(map) = self.get_map(map) {
            Ok((true, CrcFrame::read_from_bytes(&map.data, offset as usize)?))
        } else {
            let mut buf = BytesMut::zeroed(INITIAL_READ_SIZE);
            let read = self.file.read_at(&mut buf, pos.0)?;
            assert!(read > CrcFrame::CRC_HEADER_LENGTH); // todo this is not actually guaranteed
            let size = CrcFrame::read_size(&buf[..read]);
            let target_size = size + CrcFrame::CRC_HEADER_LENGTH;
            if target_size > read {
                // todo more test coverage for those cases including when read != INITIAL_READ_SIZE
                if target_size > INITIAL_READ_SIZE {
                    let more = target_size - INITIAL_READ_SIZE;
                    buf.put_bytes(0, more);
                }
                self.file
                    .read_exact_at(&mut buf[read..], pos.0 + read as u64)?;
            }
            let bytes = bytes::Bytes::from(buf).into();
            Ok((false, CrcFrame::read_from_bytes(&bytes, 0)?))
        }
    }

    pub fn random_reader_at(
        &self,
        pos: WalPosition,
        inner_offset: usize,
    ) -> Result<WalRandomRead, WalError> {
        assert_ne!(
            pos,
            WalPosition::INVALID,
            "Trying to read invalid wal position"
        );
        let (map, offset) = self.layout.locate(pos.0);
        if let Some(map) = self.get_map(map) {
            let offset = offset as usize;
            let header_end = offset + CrcFrame::CRC_HEADER_LENGTH;
            let size = CrcFrame::read_size(&map.data[offset..header_end]);
            let data = map.data.slice(header_end + inner_offset..header_end + size);
            Ok(WalRandomRead::Mapped(data))
        } else {
            let mut buf = [0; CrcFrame::CRC_HEADER_LENGTH];
            self.file.read_exact_at(&mut buf, pos.0)?;
            let size = CrcFrame::read_size(&buf);
            let header_end = pos.0 + CrcFrame::CRC_HEADER_LENGTH as u64;
            let range = (header_end + inner_offset as u64)..(header_end + size as u64);
            Ok(WalRandomRead::File(FileRange::new(&self.file, range)))
        }
    }

    fn get_map(&self, id: u64) -> Option<Map> {
        let maps = self.maps.read();
        maps.get(&id).cloned()
    }

    fn map(&self, id: u64, writeable: bool) -> io::Result<Map> {
        let mut maps = self.maps.write();
        let map = match maps.entry(id) {
            Entry::Vacant(va) => {
                let range = self.layout.map_range(id);
                let data = unsafe {
                    let mut options = memmap2::MmapOptions::new();
                    options
                        .offset(range.start)
                        .len(self.layout.frag_size as usize);
                    if writeable {
                        options.populate().map_mut(&self.file)?.into()
                    } else {
                        options.map(&self.file)?.into()
                    }
                };
                let map = Map {
                    id,
                    writeable,
                    data,
                };
                va.insert(map)
            }
            Entry::Occupied(oc) => {
                let map = oc.into_mut();
                if writeable && !map.writeable {
                    // this can be supported but not needed?
                    panic!("Requested writable mapping but it is already mapped as read-only");
                }
                map
            }
        };
        let map = map.clone();
        if maps.len() > self.layout.max_maps {
            maps.pop_first();
        }
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

    /// Iterate wal from the position after given position
    /// If WalPosition::INVALID is specified, iterate from start
    pub fn wal_iterator(self: &Arc<Self>, position: WalPosition) -> Result<WalIterator, WalError> {
        let (skip_one, position) = if position == WalPosition::INVALID {
            (false, 0)
        } else {
            (true, position.0)
        };
        let (map_id, _) = self.layout.locate(position);
        self.extend_to_map(map_id)?;
        let map = self.map(map_id, true)?;
        let mut iterator = WalIterator {
            wal: self.clone(),
            position,
            map,
        };
        if skip_one {
            iterator.next()?;
        }
        Ok(iterator)
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

impl WalRandomRead<'_> {
    pub fn kind_str(&self) -> &'static str {
        match self {
            WalRandomRead::Mapped(_) => "mapped",
            WalRandomRead::File(_) => "syscall",
        }
    }
}

impl RandomRead for WalRandomRead<'_> {
    fn read(&self, range: Range<usize>) -> Bytes {
        match self {
            WalRandomRead::Mapped(bytes) => bytes.slice(range),
            WalRandomRead::File(fr) => fr.read(range),
        }
    }

    fn len(&self) -> usize {
        match self {
            WalRandomRead::Mapped(bytes) => bytes.len(),
            WalRandomRead::File(range) => range.len(),
        }
    }

    fn prefetch_range(&mut self, _range: &Range<usize>) {
        match self {
            WalRandomRead::Mapped(_) => {}
            WalRandomRead::File(_) => { /*todo*/ }
        }
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

    pub fn len(&self) -> usize {
        self.frame.len_with_header()
    }
}

impl WalPosition {
    pub const INVALID: WalPosition = WalPosition(u64::MAX);
    pub const LENGTH: usize = 8;
    #[cfg(test)]
    pub const TEST: WalPosition = WalPosition(3311);

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.0);
    }

    pub fn read_from_buf(buf: &mut impl Buf) -> Self {
        Self(buf.get_u64())
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        Self(u64::from_be_bytes(
            slice
                .try_into()
                .expect("Invalid slice length for WalPosition::from_slice"),
        ))
    }

    pub fn valid(self) -> Option<Self> {
        if self == Self::INVALID {
            None
        } else {
            Some(self)
        }
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn test_value(v: u64) -> Self {
        Self(v)
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
        let layout = WalLayout {
            frag_size: 1024,
            max_maps: 2,
        };
        // todo - add second test case when there is no space for skip marker after large
        let large = vec![1u8; 1024 - 8 - CrcFrame::CRC_HEADER_LENGTH * 3 - 9];
        {
            let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
            let writer = wal
                .wal_iterator(WalPosition::INVALID)
                .unwrap()
                .into_writer();
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
            let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
            let mut wal_iterator = wal.wal_iterator(WalPosition::INVALID).unwrap();
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
            let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
            let mut wal_iterator = wal.wal_iterator(WalPosition::INVALID).unwrap();
            let p1 = assert_bytes(&[1, 2, 3], wal_iterator.next());
            let p2 = assert_bytes(&[], wal_iterator.next());
            let p3 = assert_bytes(&large, wal_iterator.next());
            let p4 = assert_bytes(&[91, 92, 93], wal_iterator.next());
            wal_iterator.next().expect_err("Error expected");
            let wal = Wal::open(&file, layout.clone(), Metrics::new()).unwrap();
            assert_eq!(&[1, 2, 3], wal.read(p1).unwrap().as_ref());
            assert_eq!(&[] as &[u8], wal.read(p2).unwrap().as_ref());
            assert_eq!(&large, wal.read(p3).unwrap().as_ref());
            assert_eq!(&[91, 92, 93], wal.read(p4).unwrap().as_ref());

            assert_eq!(&[1, 2, 3], wal.read_unmapped(p1).unwrap().1.as_ref());
            assert_eq!(&[] as &[u8], wal.read_unmapped(p2).unwrap().1.as_ref());
            assert_eq!(&large, wal.read_unmapped(p3).unwrap().1.as_ref());
            assert_eq!(&[91, 92, 93], wal.read_unmapped(p4).unwrap().1.as_ref());
        }
        // we wrote into two frags
        assert_eq!(2048, fs::metadata(file).unwrap().len());
    }

    #[test]
    fn test_atomic_wal_position() {
        let layout = WalLayout {
            frag_size: 512,
            max_maps: 2,
        };
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
