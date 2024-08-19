use bytes::BytesMut;
use minibytes::Bytes;

pub struct CrcFrame {
    bytes: Bytes,
}

pub trait IntoBytesFixed {
    fn len(&self) -> usize;
    fn write_into_bytes(&self, buf: &mut [u8]);
}

impl CrcFrame {
    pub fn from_bytes_fixed(t: &impl IntoBytesFixed) -> Self {
        let mut bytes = BytesMut::with_capacity(t.len() + 4);
        t.write_into_bytes(&mut bytes[4..]);
        let crc = crc32fast::hash(&bytes[4..]);
        let crc = crc.to_le_bytes();
        bytes[..4].copy_from_slice(&crc);
        let bytes: bytes::Bytes = bytes.into();
        let bytes = bytes.into();
        Self { bytes }
    }
}
