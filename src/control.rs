use crate::crc::IntoBytesFixed;
use crate::large_table::Version;
use crate::wal::WalPosition;
use bytes::{Buf, BufMut, BytesMut};

pub struct ControlRegion {
    version: Version,
    /// WalPosition::INVALID when wal is empty
    last_position: WalPosition,
    snapshot: Box<[WalPosition]>,
}

impl IntoBytesFixed for ControlRegion {
    fn len(&self) -> usize {
        Version::LENGTH + WalPosition::LENGTH + self.snapshot.len() * WalPosition::LENGTH
    }

    fn write_into_bytes(&self, buf: &mut BytesMut) {
        buf.put_u64(self.version.0);
        self.last_position.write_to_buf(buf);
        for i in 0..self.snapshot.len() {
            self.snapshot[i].write_to_buf(buf);
        }
    }
}

impl ControlRegion {
    const VERSION_OFFSET: usize = 0;
    const REPLAY_FROM_OFFSET: usize = Self::VERSION_OFFSET + Version::LENGTH;
    const SNAPSHOT_OFFSET: usize = Self::REPLAY_FROM_OFFSET + WalPosition::LENGTH;

    pub fn new_empty(large_table_size: usize) -> Self {
        let snapshot = vec![WalPosition::INVALID; large_table_size].into_boxed_slice();
        Self {
            version: Version::ZERO,
            last_position: WalPosition::INVALID,
            snapshot,
        }
    }

    pub fn new(snapshot: Box<[WalPosition]>, version: Version, last_position: WalPosition) -> Self {
        Self {
            snapshot,
            version,
            last_position,
        }
    }

    pub fn version_from_bytes(mut bytes: &[u8]) -> Version {
        Version(bytes.get_u64())
    }

    pub fn from_slice(mut bytes: &[u8], large_table_size: usize) -> Self {
        assert_eq!(
            bytes.len(),
            Self::len_bytes_from_large_table_size(large_table_size)
        );
        let version = bytes.get_u64();
        let replay_from = WalPosition::read_from_buf(&mut bytes);
        let mut snapshot = Vec::with_capacity(large_table_size);
        for _ in 0..large_table_size {
            snapshot.push(WalPosition::read_from_buf(&mut bytes));
        }
        let snapshot = snapshot.into_boxed_slice();
        Self {
            version: Version(version),
            last_position: replay_from,
            snapshot,
        }
    }

    pub fn snapshot(&self) -> &[WalPosition] {
        &self.snapshot
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn last_position(&self) -> WalPosition {
        self.last_position
    }

    pub fn len_bytes(&self) -> usize {
        IntoBytesFixed::len(self)
    }

    pub fn len_bytes_from_large_table_size(large_table_size: usize) -> usize {
        Version::LENGTH + WalPosition::LENGTH + large_table_size * WalPosition::LENGTH
    }
}
