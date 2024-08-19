use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use bytes::{Buf, BufMut};
use minibytes::Bytes;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{io, mem};

#[derive(Clone)]
pub struct Wal {
    map: Bytes,
    position: Arc<AtomicU64>,
    frag_size: u64,
}

#[derive(Clone)]
pub struct WalReader {
    map: Bytes,
}

pub struct WalIterator {
    map: Bytes,
    position: u64,
    frag_size: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FragPosition(u32);

impl Wal {
    pub fn open(p: &impl AsRef<Path>, frag_size: u64) -> io::Result<WalIterator> {
        assert!(frag_size <= u32::MAX as u64);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(p)?;
        file.set_len(frag_size)?;
        let map = unsafe {
            memmap2::MmapOptions::new()
                .len(frag_size as usize)
                .map_mut(&file)?
        };
        let map = map.into();
        let iterator = WalIterator {
            map,
            position: 0,
            frag_size,
        };
        Ok(iterator)
    }

    pub fn write(&self, w: &PreparedWalWrite) -> Result<FragPosition, WalFullError> {
        let len = w.frame.len() as u64;
        let len_aligned = Self::align(len);
        let pos = self.position.fetch_add(len_aligned, Ordering::AcqRel);
        if pos + len_aligned > self.frag_size {
            return Err(WalFullError);
        }
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
        WalReader { map }
    }

    fn align(l: u64) -> u64 {
        const ALIGN: u64 = 8;
        (l + ALIGN - 1) / ALIGN * ALIGN
    }
}

#[derive(Debug)]
pub struct WalFullError;

impl WalReader {
    pub fn read(&self, pos: FragPosition) -> Result<Bytes, CrcReadError> {
        CrcFrame::read_from_checked_with_len(&self.map, pos.0 as usize)
    }
}

impl WalIterator {
    pub fn next(&mut self) -> Result<Bytes, CrcReadError> {
        let frame = CrcFrame::read_from_checked_with_len(&self.map, self.position as usize)?;
        self.position += Wal::align((frame.len() + CrcFrame::CRC_LEN_HEADER_LENGTH) as u64);
        Ok(frame)
    }

    pub fn into_writer(self) -> Wal {
        Wal {
            map: self.map,
            position: Arc::new(AtomicU64::new(self.position)),
            frag_size: self.frag_size,
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
        let wal = Wal::open(&file, 1024).unwrap().into_writer();
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
        let mut wal = Wal::open(&file, 2048).unwrap();
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
        let mut wal = Wal::open(&file, 2048).unwrap();
        assert_bytes(&[1, 2, 3], wal.next());
        assert_bytes(&[], wal.next());
        assert_bytes(&large, wal.next());
        assert_bytes(&[91, 92, 93], wal.next());
        wal.next().expect_err("Error expected");
    }

    #[track_caller]
    fn assert_bytes(e: &[u8], v: Result<Bytes, CrcReadError>) {
        let v = v.expect("Expected value, bot nothing");
        assert_eq!(e, v.as_ref());
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
}
