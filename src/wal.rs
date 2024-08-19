use crate::crc::{CrcFrame, CrcReadError, IntoBytesFixed};
use memmap2::MmapMut;
use minibytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub struct Wal {
    map: MmapMut,
}

pub struct WalReader {
    map: Bytes,
}

impl Wal {
    pub fn open(p: &impl AsRef<Path>, len: u64) -> io::Result<(Self, WalReader)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(p)?;
        file.set_len(len)?;
        let map = unsafe {
            memmap2::MmapOptions::new()
                .len(len as usize)
                .map_mut(&file)?
        };
        let writer = Self { map };
        let map = unsafe { memmap2::MmapOptions::new().len(len as usize).map(&file)? };
        let map = map.into();
        let reader = WalReader { map };
        Ok((writer, reader))
    }

    pub fn write(&mut self, pos: usize, w: &PreparedWalWrite) {
        self.map[pos..].copy_from_slice(w.frame.as_ref());
    }
}

impl WalReader {
    pub fn read(&self, pos: u32) -> Result<Bytes, CrcReadError> {
        CrcFrame::read_from_checked(&self.map, pos as usize)
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
