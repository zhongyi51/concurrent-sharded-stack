//! An opt-in object-pool policy, not part of the shared stack's API.
//!
//! Cached objects are temporarily unavailable to other workers. Flush regularly
//! and drop/flush all caches before closing the pool. Do not use this policy for
//! work queues, globally visible returns, or scarce resources requiring fairness.
use concurrent_sharded_stack::{ConcurrentShardedStack, PopError, PushError};

pub struct LocalCache<'a, T: Send + 'static> {
    shared: &'a ConcurrentShardedStack<T>,
    values: Vec<T>,
    capacity: usize,
}
impl<'a, T: Send + 'static> LocalCache<'a, T> {
    pub fn new(shared: &'a ConcurrentShardedStack<T>, capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            shared,
            values: Vec::with_capacity(capacity),
            capacity,
        }
    }
    pub fn take(&mut self) -> Result<T, PopError> {
        if let Some(value) = self.values.pop() {
            return Ok(value);
        }
        let value = self.shared.pop()?;
        // Each transfer still uses the public stack protocol. Nodes are never
        // recycled before a grace period; only owned payloads are cached.
        for _ in 1..self.capacity {
            match self.shared.pop() {
                Ok(extra) => self.values.push(extra),
                Err(_) => break,
            }
        }
        Ok(value)
    }
    pub fn put(&mut self, value: T) -> Result<(), PushError<T>> {
        if self.values.len() < self.capacity {
            self.values.push(value);
            Ok(())
        } else {
            self.shared.push(value)
        }
    }
    pub fn flush(&mut self) -> Result<(), PushError<T>> {
        while let Some(value) = self.values.pop() {
            self.shared.push(value)?;
        }
        Ok(())
    }
}
impl<T: Send + 'static> Drop for LocalCache<'_, T> {
    fn drop(&mut self) {
        while let Some(value) = self.values.pop() {
            // If the owner closed early, the rejected value is destroyed here.
            // No leak, but callers needing every object back must flush first.
            let _ = self.shared.push(value);
        }
    }
}
