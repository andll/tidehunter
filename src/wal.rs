use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use bytes::{Buf, BufMut};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{io, mem};

#[derive(Clone)]
pub struct Wal {
    map: Bytes,
    position: AtomicFragPosition,
}

#[derive(Clone)]
pub enum WalReader {
    Whole(Bytes),
    Partial(Arc<PartialWalReader>),
}

struct PartialWalReader {
    file: File,
    maps: Mutex<BTreeMap<u32, Bytes>>,
}

pub struct WalIterator {
    map: Bytes,
    position: u64,
    layout: FragLayout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FragPosition(u32);

impl Wal {
    pub fn open(p: &impl AsRef<Path>, layout: FragLayout) -> io::Result<WalIterator> {
        assert!(layout.frag_size <= u32::MAX as u64, "Frag size too large");
        assert!(
            layout.map_size <= layout.frag_size,
            "Map size larger then frag size"
        );
        assert_eq!(
            (layout.frag_size / layout.map_size) * layout.map_size,
            layout.frag_size,
            "Frag size should be divisible by map size"
        );
        assert_eq!(
            layout.frag_size,
            Self::align(layout.frag_size),
            "Frag size not aligned"
        );
        assert_eq!(
            layout.map_size,
            Self::align(layout.map_size),
            "Map size not aligned"
        );
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
        let iterator = WalIterator {
            map,
            position: 0,
            layout,
        };
        Ok(iterator)
    }

    pub fn write(&self, w: &PreparedWalWrite) -> Result<FragPosition, WalFullError> {
        let len = w.frame.len_with_header() as u64;
        let len_aligned = Self::align(len);
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

    pub fn reader(&self) -> WalReader {
        let map = self.map.clone();
        WalReader::Whole(map)
    }

    fn align(l: u64) -> u64 {
        align::<8>(l)
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
    /// Allocate the next position.
    /// Block should not cross the map boundary defined by the self.map_size
    /// End of the block should not exceed self.frag_size (returns None otherwise)
    fn next_position(&self, mut pos: u64, len_aligned: u64) -> Option<u64> {
        assert!(
            len_aligned <= self.map_size,
            "Entry({len_aligned}) is larger then map_size({})",
            self.map_size
        );
        let map_start = pos / self.map_size;
        let map_end = (pos + len_aligned - 1) / self.map_size;
        if map_start != map_end {
            pos = (map_start + 1) * self.map_size;
        }
        if pos + len_aligned > self.frag_size {
            None
        } else {
            Some(pos)
        }
    }
}

const fn align<const A: u64>(l: u64) -> u64 {
    (l + A - 1) / A * A
}

#[derive(Debug)]
pub struct WalFullError;

impl WalReader {
    pub fn read(&self, pos: FragPosition) -> Result<Bytes, CrcReadError> {
        match self {
            WalReader::Whole(map) => CrcFrame::read_from_checked_with_len(map, pos.0 as usize),
            WalReader::Partial(partial) => partial.read(pos),
        }
    }
}

impl PartialWalReader {
    pub fn read(&self, pos: FragPosition) -> Result<Bytes, CrcReadError> {
        unimplemented!()
        // let maps = self.maps.lock();
        // if let Some((start, map)) = maps.range(..pos.0).last() {
        //     let start = *start as usize;
        //     let end = start + map.len();
        //     let pos = pos.0 as usize;
        //     if pos + CrcFrame::CRC_LEN_HEADER_LENGTH <= end {
        //
        //     }
        // }
    }
}

impl WalIterator {
    pub fn next(&mut self) -> Result<Bytes, CrcReadError> {
        let frame = CrcFrame::read_from_checked_with_len(&self.map, self.position as usize)?;
        self.position += Wal::align((frame.len() + CrcFrame::CRC_LEN_HEADER_LENGTH) as u64);
        Ok(frame)
    }

    pub fn into_writer(self) -> Wal {
        let position = AtomicFragPosition {
            position: Arc::new(AtomicU64::new(self.position)),
            layout: self.layout,
        };
        Wal {
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
        let wal = Wal::open(&file, layout).unwrap().into_writer();
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
            map_size: 2048,
        };
        let mut wal = Wal::open(&file, layout.clone()).unwrap();
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
        let mut wal = Wal::open(&file, layout).unwrap();
        assert_bytes(&[1, 2, 3], wal.next());
        assert_bytes(&[], wal.next());
        assert_bytes(&large, wal.next());
        assert_bytes(&[91, 92, 93], wal.next());
        wal.next().expect_err("Error expected");
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
        assert_eq!(Wal::align(1), 8);
        assert_eq!(Wal::align(4), 8);
        assert_eq!(Wal::align(7), 8);
        assert_eq!(Wal::align(0), 0);
        assert_eq!(Wal::align(8), 8);
        assert_eq!(Wal::align(15), 16);
        assert_eq!(Wal::align(16), 16);
    }

    #[track_caller]
    fn assert_bytes(e: &[u8], v: Result<Bytes, CrcReadError>) {
        let v = v.expect("Expected value, bot nothing");
        assert_eq!(e, v.as_ref());
    }
}
