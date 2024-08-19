use crate::crc::IntoBytesFixed;
use crate::large_table::{Position, Version};
use bytes::{Buf, BufMut, BytesMut};

pub struct ControlRegion {
    version: Version,
    replay_from: Position,
    snapshot: Box<[Position]>,
}

impl IntoBytesFixed for ControlRegion {
    fn len(&self) -> usize {
        Version::LENGTH + Position::LENGTH + self.snapshot.len() * Position::LENGTH
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        buf.put_u64(self.version.0);
        buf.put_u64(self.replay_from.0);
        for i in 0..self.snapshot.len() {
            buf.put_u64(self.snapshot[i].0);
        }
    }
}

impl ControlRegion {
    const VERSION_OFFSET: usize = 0;
    const REPLAY_FROM_OFFSET: usize = Self::VERSION_OFFSET + Version::LENGTH;
    const SNAPSHOT_OFFSET: usize = Self::REPLAY_FROM_OFFSET + Position::LENGTH;

    pub fn new(large_table_size: usize) -> Self {
        let snapshot = vec![Position::ZERO; large_table_size].into_boxed_slice();
        Self {
            version: Version::ZERO,
            replay_from: Position::ZERO,
            snapshot,
        }
    }

    pub fn version_from_bytes(mut bytes: &[u8]) -> Version {
        Version(bytes.get_u64())
    }

    pub fn from_bytes(mut bytes: &[u8], large_table_size: usize) -> Self {
        assert_eq!(
            bytes.len(),
            Self::len_from_large_table_size(large_table_size)
        );
        let version = bytes.get_u64();
        let replay_from = bytes.get_u64();
        let mut snapshot = Vec::with_capacity(large_table_size);
        for _ in 0..large_table_size {
            snapshot.push(Position(bytes.get_u64()));
        }
        let snapshot = snapshot.into_boxed_slice();
        Self {
            version: Version(version),
            replay_from: Position(replay_from),
            snapshot,
        }
    }

    pub fn snapshot(&self) -> &[Position] {
        &self.snapshot
    }

    pub fn len_from_large_table_size(large_table_size: usize) -> usize {
        Version::LENGTH + Position::LENGTH + large_table_size * Position::LENGTH
    }
}
