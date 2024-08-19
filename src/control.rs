use crate::crc::IntoBytesFixed;
use crate::large_table::{Position, Version};

pub struct ControlRegion {
    version: Version,
    replay_from: Position,
    snapshot: Box<[Position]>,
}

impl IntoBytesFixed for ControlRegion {
    fn len(&self) -> usize {
        Version::LENGTH + Position::LENGTH + self.snapshot.len() * Position::LENGTH
    }

    fn write_into_bytes(&self, buf: &mut [u8]) {
        let version = self.version.0.to_le_bytes();
        let replay_from = self.replay_from.0.to_le_bytes();
        buf[Self::VERSION_OFFSET..Self::REPLAY_FROM_OFFSET].copy_from_slice(&version);
        buf[Self::REPLAY_FROM_OFFSET..Self::SNAPSHOT_OFFSET].copy_from_slice(&replay_from);
        let snap_buf = &mut buf[Self::SNAPSHOT_OFFSET..];
        for i in 0..self.snapshot.len() {
            let sp = self.snapshot[i].0.to_le_bytes();
            snap_buf[i * Position::LENGTH..(i + 1) * Position::LENGTH].copy_from_slice(&sp);
        }
    }
}

impl ControlRegion {
    const VERSION_OFFSET: usize = 0;
    const REPLAY_FROM_OFFSET: usize = Self::VERSION_OFFSET + Version::LENGTH;
    const SNAPSHOT_OFFSET: usize = Self::REPLAY_FROM_OFFSET + Position::LENGTH;
}
