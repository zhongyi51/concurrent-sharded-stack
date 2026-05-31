//! A lock-free concurrent stack that shards a classic Treiber stack across
//! multiple per-thread shards and reclaims memory with epoch-based GC.
//!
//! See [`ConcurrentShardedStack`] for the main entry point. Compared to a
//! single Treiber stack, sharding spreads CAS contention across independent
//! cache-line-padded shards, at the cost of only providing LIFO ordering
//! *within* a shard rather than globally.
//!
//! # Example
//!
//! ```
//! use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
//!
//! let stack = ConcurrentShardedStack::with_concurrency(4);
//! stack.push(1).unwrap();
//! stack.push(2).unwrap();
//!
//! assert_eq!(stack.pop().unwrap(), 2);
//! assert_eq!(stack.pop().unwrap(), 1);
//! assert_eq!(stack.pop(), Err(PopError::Empty));
//! ```

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use crossbeam_utils::CachePadded;
use std::cell::Cell;
use std::fmt;
use std::hint::spin_loop;
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

static NEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static THREAD_ID: Cell<usize> = Cell::new(
        NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed)
    );
}

fn current_thread_id() -> usize {
    THREAD_ID.with(Cell::get)
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushError<T> {
    /// The stack has been closed.
    Closed(T),
}

impl<T> PushError<T> {
    pub fn into_inner(self) -> T {
        match self {
            PushError::Closed(v) => v,
        }
    }
}

impl<T> fmt::Display for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Closed(_) => write!(f, "stack is closed"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for PushError<T> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopError {
    Empty,
    Closed,
}

impl fmt::Display for PopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PopError::Empty => write!(f, "stack is empty"),
            PopError::Closed => write!(f, "stack is closed"),
        }
    }
}

impl std::error::Error for PopError {}

struct Node<T> {
    value: ManuallyDrop<T>,
    next: Atomic<Node<T>>,
}

impl<T> Node<T> {
    fn new(value: T) -> Self {
        Self {
            value: ManuallyDrop::new(value),
            next: Atomic::null(),
        }
    }
}

/// Concurrent sharded Treiber stack.
pub struct ConcurrentShardedStack<T> {
    shards: Box<[CachePadded<Atomic<Node<T>>>]>,

    /// Bitmap for hinting.
    /// Both `1` and `0` are just meaning that there may be or may not be an element in the corresponding shard.
    bitmap: AtomicUsize,

    shard_index_mask: usize,

    closed: AtomicBool,
}

unsafe impl<T: Send> Send for ConcurrentShardedStack<T> {}
unsafe impl<T: Send> Sync for ConcurrentShardedStack<T> {}

impl<T> Default for ConcurrentShardedStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConcurrentShardedStack<T> {
    /// Creates a stack using `available_parallelism()` as concurrency hint.
    pub fn new() -> Self {
        let concurrency = thread::available_parallelism()
            .map(|n| n.get().next_power_of_two().min(usize::BITS as usize))
            .unwrap_or(4);

        Self::with_concurrency(concurrency)
    }

    /// Creates a stack with exact shard count.
    ///
    /// Panics if:
    ///
    /// - `shard_count == 0`
    /// - `shard_count` is not power of two
    /// - `shard_count > usize::BITS`
    pub fn with_concurrency(shard_count: usize) -> Self {
        assert!(shard_count > 0, "shard_count must be non-zero");

        assert!(
            shard_count.is_power_of_two(),
            "shard_count must be a power of two"
        );

        assert!(
            shard_count <= usize::BITS as usize,
            "shard_count must not exceed usize::BITS"
        );

        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            shards.push(CachePadded::new(Atomic::null()));
        }

        Self {
            shards: shards.into_boxed_slice(),
            bitmap: AtomicUsize::new(0),
            shard_index_mask: shard_count - 1,
            closed: AtomicBool::new(false),
        }
    }

    /// Pushes a value into the stack.
    ///
    /// Returns `Err(PushError::Closed(value))` if the stack is already closed.
    pub fn push(&self, value: T) -> Result<(), PushError<T>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PushError::Closed(value));
        }

        let shard_index = self.current_shard_index();
        let shard = &self.shards[shard_index];
        let bit = 1usize << shard_index;

        let guard = &epoch::pin();
        let mut node = Owned::new(Node::new(value));

        loop {
            let head = shard.load(Ordering::Acquire, guard);

            node.next.store(head, Ordering::Relaxed);

            match shard.compare_exchange_weak(
                head,
                node,
                Ordering::Release,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => {
                    if self.bitmap.load(Ordering::Relaxed) & bit == 0 {
                        self.bitmap.fetch_or(bit, Ordering::Release);
                    }
                    return Ok(());
                }
                Err(err) => {
                    node = err.new;
                    spin_loop();
                }
            }
        }
    }

    /// Pops a value from the stack non-blocking.
    ///
    /// Returns `Err(PopError::Empty)` if the stack is empty.
    /// Returns `Err(PopError::Closed)` if the stack is empty and closed.
    pub fn pop(&self) -> Result<T, PopError> {
        let guard = &epoch::pin();

        let start = self.current_shard_index();
        let shard_count = self.shards.len();

        // 1. Current shard first.
        if let Some(value) = self.try_pop_from_shard(start, guard) {
            return Ok(value);
        }

        // 2. Bitmap-guided search.
        let bits = self.bitmap.load(Ordering::Acquire);

        if bits != 0 {
            for offset in 1..shard_count {
                let index = (start + offset) & self.shard_index_mask;

                if bits & 1usize << index == 0 {
                    continue;
                }

                if let Some(value) = self.try_pop_from_shard(index, guard) {
                    return Ok(value);
                }
            }
        }

        // 3. Fallback.
        for offset in 1..=shard_count {
            let index = (start + offset) & self.shard_index_mask;

            if let Some(value) = self.try_pop_from_shard(index, guard) {
                return Ok(value);
            }
        }

        if self.closed.load(Ordering::Acquire) {
            Err(PopError::Closed)
        } else {
            Err(PopError::Empty)
        }
    }

    /// Closes the stack, existing elements can still be popped.
    pub fn close(&self) -> bool {
        self.closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Checks if the stack is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn current_shard_index(&self) -> usize {
        current_thread_id() & self.shard_index_mask
    }

    fn try_pop_from_shard(&self, index: usize, guard: &Guard) -> Option<T> {
        let shard = &self.shards[index];
        let b = 1usize << index;

        loop {
            let head = shard.load(Ordering::Acquire, guard);

            if head.is_null() {
                // Weak hint clearing.
                self.bitmap.fetch_and(!b, Ordering::AcqRel);
                return None;
            }

            let head_ref = unsafe { head.deref() };
            let next = head_ref.next.load(Ordering::Acquire, guard);

            match shard.compare_exchange_weak(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => {
                    if next.is_null() {
                        // Weak hint clearing.
                        self.bitmap.fetch_and(!b, Ordering::AcqRel);
                    }

                    // Move the value out of the popped node. The node itself is
                    // reclaimed later by the epoch GC; because `value` is a
                    // `ManuallyDrop<T>`, that deferred destruction will not drop
                    // it again.
                    let value = unsafe { ptr::read(&*head_ref.value) };

                    unsafe {
                        guard.defer_destroy(head);
                    }

                    return Some(value);
                }
                Err(_) => {
                    spin_loop();
                }
            }
        }
    }
}

impl<T> Drop for ConcurrentShardedStack<T> {
    fn drop(&mut self) {
        let guard = &epoch::pin();

        for shard in self.shards.iter() {
            let mut current = shard.load(Ordering::Relaxed, guard);

            while !current.is_null() {
                unsafe {
                    let raw = current.as_raw() as *mut Node<T>;
                    let next = (*raw).next.load(Ordering::Relaxed, guard);

                    let mut node = Box::from_raw(raw);
                    // The value was never popped, so we are responsible for
                    // dropping it here (it lives inside a `ManuallyDrop`).
                    ManuallyDrop::drop(&mut node.value);
                    drop(node);

                    current = next;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug)]
    struct DropCounter {
        counter: Arc<AtomicUsize>,
    }

    impl DropCounter {
        fn new(counter: Arc<AtomicUsize>) -> Self {
            Self { counter }
        }
    }

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn new_requires_power_of_two() {
        let s = ConcurrentShardedStack::<usize>::with_concurrency(4);
        assert_eq!(s.shard_count(), 4);
    }

    #[test]
    #[should_panic]
    fn new_panics_if_not_power_of_two() {
        let _ = ConcurrentShardedStack::<usize>::with_concurrency(3);
    }

    #[test]
    #[should_panic]
    fn new_panics_if_too_many_shards() {
        let _ = ConcurrentShardedStack::<usize>::with_concurrency((usize::BITS as usize) * 2);
    }

    #[test]
    fn push_pop_single_thread() {
        let s = ConcurrentShardedStack::with_concurrency(4);

        s.push(1).unwrap();
        s.push(2).unwrap();
        s.push(3).unwrap();

        assert_eq!(s.pop().unwrap(), 3);
        assert_eq!(s.pop().unwrap(), 2);
        assert_eq!(s.pop().unwrap(), 1);

        assert_eq!(s.pop(), Err(PopError::Empty));
    }

    #[test]
    fn close_works() {
        let s = ConcurrentShardedStack::with_concurrency(4);

        assert!(s.push(1).is_ok());

        assert!(s.close());
        assert!(!s.close());

        assert_eq!(s.push(2), Err(PushError::Closed(2)));

        assert_eq!(s.pop().unwrap(), 1);
        assert_eq!(s.pop(), Err(PopError::Closed));
    }

    #[test]
    fn multi_thread_push_pop() {
        let s = Arc::new(ConcurrentShardedStack::with_concurrency(8));

        let threads = 8;
        // Miri executes far slower, so use a much smaller workload there.
        #[cfg(miri)]
        let per_thread = 200;
        #[cfg(not(miri))]
        let per_thread = 10_000;

        let mut handles = Vec::new();

        for t in 0..threads {
            let s = Arc::clone(&s);

            handles.push(std::thread::spawn(move || {
                for i in 0..per_thread {
                    s.push(t * per_thread + i).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut handles = Vec::new();

        for _ in 0..threads {
            let s = Arc::clone(&s);

            handles.push(std::thread::spawn(move || {
                let mut count = 0usize;

                loop {
                    match s.pop() {
                        Ok(_) => count += 1,
                        Err(PopError::Empty) => break,
                        Err(PopError::Closed) => break,
                    }
                }

                count
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        assert_eq!(total, threads * per_thread);
    }

    #[test]
    fn close_after_drain_returns_closed() {
        let s = ConcurrentShardedStack::with_concurrency(4);

        for i in 0..100 {
            s.push(i).unwrap();
        }

        assert!(s.close());

        let mut count = 0;

        loop {
            match s.pop() {
                Ok(_) => count += 1,
                Err(PopError::Empty) => continue,
                Err(PopError::Closed) => break,
            }
        }

        assert_eq!(count, 100);
    }

    #[test]
    fn popped_values_are_dropped_exactly_once() {
        let counter = Arc::new(AtomicUsize::new(0));
        let s = ConcurrentShardedStack::with_concurrency(4);

        for _ in 0..50 {
            s.push(DropCounter::new(Arc::clone(&counter))).unwrap();
        }

        // Pop everything; each popped value should be dropped exactly once when
        // it goes out of scope here.
        let mut popped = 0;
        while let Ok(value) = s.pop() {
            drop(value);
            popped += 1;
        }

        assert_eq!(popped, 50);

        // Force any deferred reclamation to run. Even after reclamation, the
        // count must stay at exactly 50 (no double-drop from `defer_destroy`).
        drop(s);
        epoch::pin().flush();

        assert_eq!(counter.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn remaining_values_are_dropped_when_stack_is_dropped() {
        let counter = Arc::new(AtomicUsize::new(0));

        {
            let s = ConcurrentShardedStack::with_concurrency(4);
            for _ in 0..30 {
                s.push(DropCounter::new(Arc::clone(&counter))).unwrap();
            }
            // Drop the stack without popping; all 30 values must be dropped once.
        }

        assert_eq!(counter.load(Ordering::Relaxed), 30);
    }

    #[test]
    fn partially_drained_stack_drops_each_value_once() {
        let counter = Arc::new(AtomicUsize::new(0));

        {
            let s = ConcurrentShardedStack::with_concurrency(4);
            for _ in 0..40 {
                s.push(DropCounter::new(Arc::clone(&counter))).unwrap();
            }

            for _ in 0..15 {
                let _ = s.pop().unwrap();
            }
            // 15 dropped via pop, 25 remain to be dropped by the stack's Drop.
        }

        epoch::pin().flush();
        assert_eq!(counter.load(Ordering::Relaxed), 40);
    }

    #[test]
    fn concurrent_drop_counter_no_double_free() {
        let counter = Arc::new(AtomicUsize::new(0));
        let s = Arc::new(ConcurrentShardedStack::with_concurrency(4));

        let threads = 4;
        #[cfg(miri)]
        let per_thread = 50;
        #[cfg(not(miri))]
        let per_thread = 200;

        let mut handles = Vec::new();
        for _ in 0..threads {
            let s = Arc::clone(&s);
            let counter = Arc::clone(&counter);
            handles.push(std::thread::spawn(move || {
                for _ in 0..per_thread {
                    s.push(DropCounter::new(Arc::clone(&counter))).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut handles = Vec::new();
        for _ in 0..threads {
            let s = Arc::clone(&s);
            handles.push(std::thread::spawn(move || {
                let mut popped = 0;
                while let Ok(value) = s.pop() {
                    drop(value);
                    popped += 1;
                }
                popped
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, threads * per_thread);

        drop(s);
        epoch::pin().flush();
        assert_eq!(counter.load(Ordering::Relaxed), threads * per_thread);
    }
}
