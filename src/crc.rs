use bytes::{BufMut, BytesMut};
use minibytes::Bytes;

pub struct CrcFrame {
    bytes: Bytes,
}

pub trait IntoBytesFixed {
    fn len(&self) -> usize;
    fn write_into_bytes(&self, buf: &mut BytesMut);
}

impl CrcFrame {
    pub fn from_bytes_fixed(t: &impl IntoBytesFixed) -> Self {
        let mut bytes = BytesMut::with_capacity(t.len() + 4);
        bytes.put_u32(0);
        t.write_into_bytes(&mut bytes);
        let crc = crc32fast::hash(&bytes[4..]);
        let crc = crc.to_be_bytes();
        bytes[..4].copy_from_slice(&crc);
        let bytes: bytes::Bytes = bytes.into();
        let bytes = bytes.into();
        Self { bytes }
    }
    pub fn from_bytes_fixed_with_len(t: &impl IntoBytesFixed) -> Self {
        let mut bytes = BytesMut::with_capacity(t.len() + 8);
        bytes.put_u64(0);
        t.write_into_bytes(&mut bytes);
        let len = (t.len() as u32).to_be_bytes();
        bytes[0..4].copy_from_slice(&len);
        let crc = crc32fast::hash(&bytes[8..]);
        let crc = crc.to_be_bytes();
        bytes[4..8].copy_from_slice(&crc);
        let bytes: bytes::Bytes = bytes.into();
        let bytes = bytes.into();
        Self { bytes }
    }

    pub fn read_from_checked(b: &Bytes, pos: usize) -> Result<Bytes, CrcReadError> {
        const HEADER_LEN: usize = 8;
        if b.len() < pos + HEADER_LEN {
            return Err(CrcReadError::OutOfBounds);
        }
        let mut len = [0u8; 4];
        len.copy_from_slice(&b[pos..pos + 4]);
        let len = u32::from_be_bytes(len) as usize;
        // no overflow because len and pos are converted from u32
        if b.len() < pos + HEADER_LEN + len {
            return Err(CrcReadError::OutOfBounds);
        }

        let mut crc = [0u8; 4];
        crc.copy_from_slice(&b[pos + 4..pos + 8]);
        let crc = u32::from_be_bytes(crc);
        let data = b.slice(pos + HEADER_LEN..pos + HEADER_LEN + len);
        let actual_crc = crc32fast::hash(&data);
        if actual_crc != crc {
            return Err(CrcReadError::CrcMismatch);
        }
        Ok(data)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CrcReadError {
    OutOfBounds,
    CrcMismatch,
}

impl AsRef<[u8]> for CrcFrame {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BufMut;

    #[test]
    pub fn crc_test() {
        let data = vec![1u8, 2, 3];
        let crc = CrcFrame::from_bytes_fixed_with_len(&data);
        assert_eq!(
            &[1, 2, 3],
            &CrcFrame::read_from_checked(&crc.bytes, 0).unwrap().as_ref()
        );
        let mut bm = BytesMut::with_capacity(1024);
        bm.put_u64(u64::MAX);
        bm.put_slice(&crc.bytes);
        let bytes = bytes::Bytes::from(bm.clone()).into();
        assert_eq!(
            &[1, 2, 3],
            &CrcFrame::read_from_checked(&bytes, 8).unwrap().as_ref()
        );
        let bytes = bytes.slice(..bytes.len() - 1);
        assert_eq!(
            CrcFrame::read_from_checked(&bytes, 8),
            Err(CrcReadError::OutOfBounds)
        );
        let pos = bm.len() - 1;
        bm[pos] = 15;
        let bytes = bytes::Bytes::from(bm).into();
        assert_eq!(
            CrcFrame::read_from_checked(&bytes, 8),
            Err(CrcReadError::CrcMismatch)
        );
    }

    impl IntoBytesFixed for Vec<u8> {
        fn len(&self) -> usize {
            self.len()
        }

        fn write_into_bytes(&self, buf: &mut BytesMut) {
            buf.put_slice(&self);
        }
    }
}
