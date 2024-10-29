use minibytes::Bytes;
use std::cell::RefCell;
use std::cmp;
use std::cmp::Ordering;
use std::fs::File;
use std::ops::Range;
use std::os::unix::prelude::FileExt;

pub struct Lookup<R> {
    target: R,
    key_size: usize,
    element_size: usize,
}

pub struct FileRange<'a> {
    file: &'a File,
    range: Range<u64>,
}

pub trait RandomRead {
    fn read(&self, range: Range<usize>) -> Bytes;
    fn len(&self) -> usize;
    fn prefetch_range(&mut self, _range: &Range<usize>) {}
}

pub struct LookupResult {
    pub result: Option<Bytes>,
    pub reads: usize,
    pub narrow_lookup_success: bool,
}

impl<R: RandomRead> Lookup<R> {
    pub fn new(target: R, key_size: usize, element_size: usize) -> Self {
        Self {
            target,
            key_size,
            element_size,
        }
    }

    pub fn lookup(&mut self, key: &[u8]) -> LookupResult {
        let len_elements = self.target.len() / self.element_size;
        let elements_range = 0..len_elements;
        let key_estimate = Self::read_u128(key);
        let scale = u128::MAX / len_elements as u128;
        let key_scaled = (key_estimate / scale) as usize;
        let narrow_range = Self::estimate_lookup_range(key_scaled, &elements_range);
        self.target
            .prefetch_range(&self.element_range(&elements_range));
        self.target
            .prefetch_range(&self.element_range(&narrow_range));
        let mut reads = 0usize;
        if let Some(found) = self.lookup_in(&mut reads, key, narrow_range) {
            return LookupResult {
                reads,
                result: Some(found),
                narrow_lookup_success: true,
            };
        };
        let result = self.lookup_in(&mut reads, key, elements_range);
        LookupResult {
            reads,
            result,
            narrow_lookup_success: false,
        }
    }

    fn estimate_lookup_range(key_scaled: usize, range: &Range<usize>) -> Range<usize> {
        let interval = range.len() / 10; // 10% interval
        let start = cmp::max(range.start, key_scaled.saturating_sub(interval / 2));
        let end = cmp::min(range.end, key_scaled.saturating_add(interval / 2));
        start..end
    }

    fn lookup_in(
        &self,
        reads: &mut usize,
        key: &[u8],
        mut elements_range: Range<usize>,
    ) -> Option<Bytes> {
        while elements_range.len() > 0 {
            let p = Self::middle(&elements_range);
            *reads += 1;
            let got = self.get(p);
            let got_key = &got[..self.key_size];
            match key.cmp(&got_key) {
                Ordering::Less => {
                    elements_range = elements_range.start..p;
                }
                Ordering::Equal => {
                    // println!("key_scaled {key_scaled}, p {p}, reads {reads}");
                    return Some(got);
                }
                Ordering::Greater => {
                    elements_range = p + 1..elements_range.end;
                }
            }
        }
        None
    }

    fn read_u128(key: &[u8]) -> u128 {
        let mut v = [0u8; 16];
        let l = cmp::min(key.len(), 16);
        v[..l].copy_from_slice(&key[..l]);
        u128::from_be_bytes(v)
    }

    fn middle(range: &Range<usize>) -> usize {
        range.start + range.len() / 2
    }

    /// Read the logical element at the given index
    fn get(&self, i: usize) -> Bytes {
        self.target.read(self.element_range(&(i..i + 1)))
    }

    /// Maps range of logical element indexes to range of bytes in the target
    fn element_range(&self, range: &Range<usize>) -> Range<usize> {
        range.start * self.element_size..range.end * self.element_size
    }
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
        if start >= self.range.end || end >= self.range.end {
            panic!(
                "Trying to read range {range:?}, mapped to {mapped_range:?}, limits {:?} ",
                self.range
            );
        }
        mapped_range
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use rand::rngs::ThreadRng;
    use rand::{Rng, RngCore};

    const KEY_SIZE: usize = 32;
    const SIZE: usize = KEY_SIZE + 8;

    #[test]
    fn lookup_test() {
        // todo empty range search test
        for i in 1..100 {
            let (d, sample) = fill(i);
            let mut lookup = Lookup::new(d, KEY_SIZE, SIZE);
            let found = lookup.lookup(&sample[..KEY_SIZE]).result;
            assert_eq!(found, Some(sample));
        }
        const REPEATS: usize = 100;
        let mut reads = 0usize;
        let mut max_reads = 0usize;
        for _ in 0..REPEATS {
            let (d, sample) = fill(1024);
            let counter = RefCell::new(0usize);
            let mut lookup = Lookup::new((d, counter), KEY_SIZE, SIZE);
            let found = lookup.lookup(&sample[..KEY_SIZE]).result;
            assert_eq!(found, Some(sample));
            let iteration_reads = *lookup.target.1.borrow();
            reads += iteration_reads;
            if iteration_reads > max_reads {
                max_reads = iteration_reads;
            }
        }
        // if max_reads > 10 {
        //     panic!("For some reason max_reads > 10: {max_reads}");
        // }
        println!("Avg reads {}, max {max_reads}", reads / REPEATS)
    }

    fn fill(n: usize) -> (Bytes, Bytes) {
        let mut rng = ThreadRng::default();
        let index = rng.gen_range(0..n);
        let mut list = vec![];
        for _ in 0..n {
            let mut data = vec![0; SIZE];
            rng.fill_bytes(&mut data);
            list.push(data);
        }
        let sample = list[index].clone();
        list.sort();
        let mut out = BytesMut::with_capacity(n * SIZE);
        for data in list {
            out.put_slice(&data);
        }
        (out.to_vec().into(), sample.into())
    }

    impl RandomRead for (Bytes, RefCell<usize>) {
        fn read(&self, range: Range<usize>) -> Bytes {
            *self.1.borrow_mut() += 1;
            self.0.slice(range.start..range.end)
        }

        fn len(&self) -> usize {
            AsRef::<[u8]>::as_ref(&self.0).len()
        }
    }
}
