use crate::reclaim::Scoped;
use crate::{EpochGuard, Guard, PopError, PushError, core::Core, default_shards};
use crossbeam_queue::ArrayQueue;
use intrusive_collections::singly_linked_list::AtomicLinkOps;
use intrusive_collections::{LinkOps, SinglyLinkedListAtomicLink as Link};
use std::alloc::{Layout, dealloc};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::Arc;

#[repr(C)]
struct Node<T> {
    link: Link,
    value: MaybeUninit<T>,
}

// Only EMPTY storage is allowed here. No T, including borrowed or !Send T, is
// accessed by a deferred callback. The function pointer retains its layout.
struct Block {
    ptr: NonNull<u8>,
    free: unsafe fn(NonNull<u8>),
}
unsafe impl Send for Block {}
impl Block {
    unsafe fn new<T>(ptr: NonNull<Node<T>>) -> Self {
        unsafe fn free<T>(p: NonNull<u8>) {
            unsafe { dealloc(p.as_ptr(), Layout::new::<Node<T>>()) };
        }
        Self {
            ptr: ptr.cast(),
            free: free::<T>,
        }
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        unsafe { (self.free)(self.ptr) };
    }
}

/// A value stack backed by the same intrusive core as [`crate::IntrusiveShardedStack`].
///
/// `pop` immediately moves out T; only empty storage waits for reclamation.
/// Ready storage is cached in bounded per-shard queues. A cache miss allocates;
/// a full/dropped cache frees the block. Pending retired storage is not bounded
/// by the cache capacity. T need not be `'static` or Sync; sharing requires Send.
/// Ordering is LIFO within a shard. `Empty` is a scan, not a global snapshot.
pub struct ConcurrentShardedStack<T, G: Guard = EpochGuard> {
    core: Core<Link, G>,
    cache: Box<[Arc<ArrayQueue<Block>>]>,
    owns: PhantomData<T>,
}
// SAFETY: only the successful remover accesses T; concurrent readers see links.
unsafe impl<T: Send, G: Guard> Send for ConcurrentShardedStack<T, G> {}
unsafe impl<T: Send, G: Guard> Sync for ConcurrentShardedStack<T, G> {}

impl<T> Default for ConcurrentShardedStack<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> ConcurrentShardedStack<T> {
    /// Use the default epoch backend and hardware concurrency hint.
    pub fn new() -> Self {
        Self::with_concurrency(default_shards())
    }
    /// Use the default epoch backend and exactly this many shards.
    /// Panics unless the count is a nonzero power of two.
    pub fn with_concurrency(count: usize) -> Self {
        Self::with_cache_capacity(count, 64)
    }
    /// Limit ready cached blocks per shard. Zero disables caching.
    pub fn with_cache_capacity(count: usize, capacity: usize) -> Self {
        // SAFETY: every inactive guard uses the global epoch domain.
        unsafe { Self::with_guard_factory(count, capacity, EpochGuard::default) }
    }
    /// Flush this thread's default epoch backend; does not wait for callbacks.
    pub fn collect(&self) {
        EpochGuard::collect();
    }
}
impl<T, G: Guard> ConcurrentShardedStack<T, G> {
    /// Select a guard factory and ready-block cache capacity per shard.
    /// Zero capacity disables caching.
    ///
    /// # Safety
    /// The factory must not panic. Every guard it creates must use the same
    /// reclamation domain; the factory keeps that domain alive. Each call returns
    /// an independent, inactive guard.
    pub unsafe fn with_guard_factory(
        count: usize,
        capacity: usize,
        make_guard: impl Fn() -> G + Send + Sync + 'static,
    ) -> Self {
        Self {
            core: Core::new(count, make_guard),
            cache: if capacity == 0 {
                Box::new([])
            } else {
                (0..count)
                    .map(|_| Arc::new(ArrayQueue::new(capacity)))
                    .collect()
            },
            owns: PhantomData,
        }
    }
    /// Publish a value, or return it unchanged if the selected shard is closed.
    pub fn push(&self, value: T) -> Result<(), PushError<T>> {
        if self.core.local_closed() {
            return Err(PushError::Closed(value));
        }
        let cached = self.cache.get(self.core.local()).and_then(|c| c.pop());
        let node = if let Some(block) = cached {
            let node = block.ptr.cast::<Node<T>>();
            // The block has passed reclamation; there are no old link readers.
            unsafe {
                ptr::addr_of_mut!((*node.as_ptr()).value).write(MaybeUninit::new(value));
            }
            std::mem::forget(block);
            node
        } else {
            NonNull::from(Box::leak(Box::new(Node {
                link: Link::new(),
                value: MaybeUninit::new(value),
            })))
        };
        let link = node.cast::<Link>(); // repr(C): link is at offset zero.
        let mut ops = AtomicLinkOps;
        // SAFETY: exclusive new/reclaimed storage, built-in atomic link ops.
        unsafe {
            assert!(ops.acquire_link(link));
            if self.core.push(link, &mut ops) {
                return Ok(());
            }
            ops.release_link(link);
            let value = ptr::addr_of!((*node.as_ptr()).value).read().assume_init();
            let block = Block::new(node);
            if let Some(cache) = self.cache.get(self.core.local()) {
                drop(cache.push(block));
            } else {
                drop(block);
            }
            Err(PushError::Closed(value))
        }
    }
    /// Immediately return T; defer reuse of its former node storage.
    /// `Closed` means every shard is closed and drained, not callback completion.
    pub fn pop(&self) -> Result<T, PopError> {
        let mut guard = Scoped::new(&self.core.guards);
        let link = unsafe { self.core.pop(&AtomicLinkOps)? };
        let node = link.cast::<Node<T>>();
        // SAFETY: only the winner touches payload bytes. Never construct &mut
        // Node or &Node while another reader may still access its link.
        let value = unsafe { ptr::addr_of!((*node.as_ptr()).value).read().assume_init() };
        let block = unsafe { Block::new(node) };
        let cache = self.cache.get(self.core.local()).map(Arc::downgrade);
        unsafe {
            guard.0.retire(link.as_ptr().cast(), move || {
                // Keep the whole Block capture (Send), not its raw pointer field.
                recycle(block, cache);
            });
        }
        Ok(value)
    }
    /// Close shards one by one. Returns whether this call closed any shard.
    /// Concurrent closers may both return true; remaining values can be popped.
    pub fn close(&self) -> bool {
        self.core.close()
    }
    /// Check whether all shards have been closed.
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }
    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.core.count()
    }
}
fn recycle(block: Block, cache: Option<std::sync::Weak<ArrayQueue<Block>>>) {
    // SAFETY: the backend has completed protection for this link identity.
    unsafe {
        AtomicLinkOps.release_link(block.ptr.cast());
    }
    if let Some(cache) = cache.and_then(|c| c.upgrade()) {
        drop(cache.push(block));
    } else {
        drop(block);
    }
}
impl<T, G: Guard> Drop for ConcurrentShardedStack<T, G> {
    fn drop(&mut self) {
        let drain = || {
            for index in 0..self.core.count() {
                while let Some(link) = unsafe { self.core.take_exclusive(index, &AtomicLinkOps) } {
                    let node = link.cast::<Node<T>>();
                    // Allocation is released even if this payload panics.
                    let _block = unsafe { Block::new(node) };
                    unsafe {
                        ptr::addr_of_mut!((*node.as_ptr()).value)
                            .cast::<T>()
                            .drop_in_place();
                    }
                }
            }
        };
        let cleanup = scopeguard::guard((), |_| drain());
        drain();
        scopeguard::ScopeGuard::into_inner(cleanup);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ready(s: &ConcurrentShardedStack<usize>, count: usize) {
        for _ in 0..100_000 {
            s.collect();
            if s.cache[0].len() == count {
                return;
            }
            std::thread::yield_now();
        }
        panic!("cache did not reach expected occupancy");
    }
    #[test]
    fn block_reuse_waits_for_old_reader_and_cache_is_bounded() {
        let s = ConcurrentShardedStack::with_cache_capacity(1, 1);
        s.push(1).unwrap();
        let guard = crossbeam_epoch::pin();
        assert_eq!(s.pop(), Ok(1));
        for _ in 0..16 {
            s.collect();
        }
        assert!(s.cache[0].is_empty());
        drop(guard);
        ready(&s, 1);
        let block = s.cache[0].pop().unwrap();
        let original = block.ptr;
        s.cache[0].push(block).ok().unwrap();
        s.push(2).unwrap();
        assert!(s.cache[0].is_empty());
        assert_eq!(s.pop(), Ok(2));
        ready(&s, 1);
        let block = s.cache[0].pop().unwrap();
        assert_eq!(block.ptr, original);
        s.cache[0].push(block).ok().unwrap();
        for i in 0..4 {
            s.push(i).unwrap();
        }
        for _ in 0..4 {
            s.pop().unwrap();
        }
        for _ in 0..128 {
            s.collect();
        }
        assert!(s.cache[0].len() <= 1);
    }
    #[test]
    fn zero_cache_and_overaligned_and_zero_sized_values() {
        #[repr(align(256))]
        struct Aligned(u8);
        let s = ConcurrentShardedStack::with_cache_capacity(1, 0);
        s.push(Aligned(42)).ok().unwrap();
        assert_eq!(s.pop().unwrap().0, 42);
        assert!(s.cache.is_empty());
        let zst = ConcurrentShardedStack::with_concurrency(1);
        for _ in 0..16 {
            zst.push(()).unwrap();
            assert_eq!(zst.pop(), Ok(()));
            zst.collect();
        }
    }
}
