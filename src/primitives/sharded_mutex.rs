use parking_lot::{Mutex, MutexGuard};

pub struct ShardedMutex<V>(Box<[Mutex<V>]>);

impl<V> ShardedMutex<V> {
    pub fn from_iterator(v: impl Iterator<Item = V>) -> Self {
        let arr = v.map(Mutex::new).collect::<Vec<_>>().into_boxed_slice();
        Self(arr)
    }

    pub fn lock(&self, n: usize) -> MutexGuard<'_, V> {
        self.0[n % self.0.len()].lock()
    }

    pub fn mutexes(&self) -> &[Mutex<V>] {
        &self.0
    }
}

impl<V> AsRef<[Mutex<V>]> for ShardedMutex<V> {
    fn as_ref(&self) -> &[Mutex<V>] {
        &self.0
    }
}

impl<V: Default> ShardedMutex<V> {
    pub fn new_default_array<const N: usize>() -> Self
    where
        [Mutex<V>; N]: Default,
    {
        Self(Box::new(<[Mutex<V>; N]>::default()))
    }
}
