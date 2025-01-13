use minibytes::Bytes;
use std::fs::File;
use std::ops::Range;
use std::os::unix::prelude::FileExt;

pub struct FileRange<'a> {
    file: &'a File,
    range: Range<u64>,
}

pub trait RandomRead {
    fn read(&self, range: Range<usize>) -> Bytes;
    fn len(&self) -> usize;
}

impl RandomRead for Bytes {
    fn read(&self, range: Range<usize>) -> Bytes {
        self.slice(range.start..range.end)
    }

    fn len(&self) -> usize {
        AsRef::<[u8]>::as_ref(self).len()
    }
}

impl RandomRead for FileRange<'_> {
    fn read(&self, range: Range<usize>) -> Bytes {
        let read_range = self.checked_range(range);
        let mut result = vec![0; (read_range.end - read_range.start) as usize];
        self.file
            .read_exact_at(&mut result, read_range.start)
            .expect("Failed to read file");
        result.into()
    }

    fn len(&self) -> usize {
        (self.range.end - self.range.start) as usize
    }
}

impl<'a> FileRange<'a> {
    pub fn new(file: &'a File, range: Range<u64>) -> Self {
        Self { file, range }
    }

    #[inline]
    fn checked_range(&self, range: Range<usize>) -> Range<u64> {
        let start = self.range.start.checked_add(range.start as u64).unwrap();
        let end = start.checked_add(range.len() as u64).unwrap();
        let mapped_range = start..end;
        if start >= self.range.end || end > self.range.end {
            panic!(
                "Trying to read range {range:?}, mapped to {mapped_range:?}, limits {:?} ",
                self.range
            );
        }
        mapped_range
    }
}
