use bytes::{Buf, BufMut};
use std::path::Path;
use std::{fs, io};

pub struct FragManager {
    frags: Vec<FragId>,
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct FragId(u32);

impl FragManager {
    pub fn from_dir(dir: &Path) -> io::Result<Self> {
        let dir = fs::read_dir(dir)?;
        let mut frags = vec![];
        for path in dir {
            let path = path?;
            if path.path().ends_with(".frag") {
                let file_name = path.file_name();
                let file_name = file_name
                    .to_str()
                    .expect("Failed to convert filename to string");
                let Ok(frag) = file_name.parse() else {
                    panic!("Unexpected frag name {file_name}")
                };
                frags.push(FragId(frag))
            }
        }
        Ok(Self { frags })
    }
}

impl FragId {
    pub const INVALID: FragId = FragId(u32::MAX);
    #[cfg(test)]
    pub const TEST: FragId = FragId(1155);

    pub fn write_to_buf(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.0);
    }

    pub fn read_from_buf(buf: &mut impl Buf) -> Self {
        Self(buf.get_u32())
    }
}
