use crate::crc::{CrcFrame, IntoBytesFixed};
use memmap2::MmapMut;
use minibytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub struct Wal {
    file: File,
    wmap: MmapMut,
    rmap: Bytes,
}

impl Wal {
    pub fn open(p: &impl AsRef<Path>, len: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(p)?;
        file.set_len(len)?;
        let wmap = unsafe {
            memmap2::MmapOptions::new()
                .len(len as usize)
                .map_mut(&file)?
        };
        let rmap = unsafe { memmap2::MmapOptions::new().len(len as usize).map(&file)? };
        let rmap = rmap.into();
        Ok(Self { file, wmap, rmap })
    }

    pub fn write(&mut self, pos: usize, w: &PreparedWalWrite) {
        self.wmap[pos..].copy_from_slice(w.frame.as_ref());
    }

    pub fn read(&self, pos: u32) -> Bytes {
        unimplemented!()
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
