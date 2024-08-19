use crate::crc::IntoBytesFixed;
use crate::large_table::{Position, Version};
use bytes::{BufMut, BytesMut};

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
}
