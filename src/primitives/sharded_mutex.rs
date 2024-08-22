use parking_lot::{Mutex, MutexGuard};

pub struct ShardedMutex<V, const N: usize>([Mutex<V>; N]);

impl<V, const N: usize> ShardedMutex<V, N> {
    pub fn lock(&self, n: usize) -> MutexGuard<'_, V> {
        self.0[n % self.0.len()].lock()
    }
}

impl<V, const N: usize> AsRef<[Mutex<V>; N]> for ShardedMutex<V, N> {
    fn as_ref(&self) -> &[Mutex<V>; N] {
        &self.0
    }
}

impl<V: Default, const N: usize> Default for ShardedMutex<V, N>
where
    [Mutex<V>; N]: Default,
{
    fn default() -> Self {
        Self(Default::default())
    }
}
