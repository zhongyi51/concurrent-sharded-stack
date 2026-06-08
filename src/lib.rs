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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

static NEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);
const CLOSED_TAG: usize = 1;
/// Maximum CAS retries before giving up on the local shard and scanning
/// other shards. Only CAS losses count — a genuinely empty local shard
/// breaks out of the retry loop immediately.
const CURRENT_SHARD_CAS_RETRIES: usize = 3;

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
        }
    }

    /// Pushes a value into the stack.
    ///
    /// Returns `Err(PushError::Closed(value))` if the stack is already closed.
    pub fn push(&self, value: T) -> Result<(), PushError<T>> {
        let shard_index = self.current_shard_index();
        let shard = &self.shards[shard_index];

        let guard = &epoch::pin();
        let mut node = Owned::new(Node::new(value));

        loop {
            let head = shard.load(Ordering::Acquire, guard);

            if head.tag() == CLOSED_TAG {
                let node = node.into_box();
                let value = unsafe { ptr::read(&*node.value) };
                return Err(PushError::Closed(value));
            }

            node.next.store(head, Ordering::Relaxed);

            match shard.compare_exchange_weak(
                head,
                node,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(_) => {
                    // Only touch the bitmap on the empty -> non-empty
                    // transition. In the steady state `head` is already
                    // non-null and the bit was set by some earlier push,
                    // so we don't need to read or write the bitmap at all.
                    if head.is_null() {
                        let bit = Self::shard_bit(shard_index);
                        if self.bitmap.load(Ordering::Relaxed) & bit == 0 {
                            self.bitmap.fetch_or(bit, Ordering::Release);
                        }
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

        // 1. Local shard. Retry only on CAS loss; a genuinely empty shard
        // makes us bail out immediately rather than re-loading the same
        // null head repeatedly.
        for _ in 0..CURRENT_SHARD_CAS_RETRIES {
            match self.pop_one(start, guard) {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => break,
                Err(()) => spin_loop(),
            }
        }

        // 2. Bitmap-guided steal via trailing_zeros — O(popcount) over set
        // bits instead of O(shard_count). Exclude the local shard we already
        // tried in Phase 1.
        let mut bits = self.bitmap.load(Ordering::Acquire) & !Self::shard_bit(start);
        while bits != 0 {
            let index = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            for _ in 0..CURRENT_SHARD_CAS_RETRIES {
                match self.pop_one(index, guard) {
                    Ok(Some(value)) => return Ok(value),
                    Ok(None) => break,
                    Err(()) => spin_loop(),
                }
            }
        }

        // 3. Only once the stack is confirmed closed do we do a full,
        // bitmap-agnostic drain. Doing this while the stack is still open
        // would race with concurrent pushes whose bitmap bit hasn't
        // propagated yet, and we'd risk wrongly reporting Empty for
        // elements that are actually in flight.
        if self.all_shards_closed(guard) {
            for offset in 0..shard_count {
                let index = (start + offset) & self.shard_index_mask;
                loop {
                    match self.pop_one(index, guard) {
                        Ok(Some(value)) => return Ok(value),
                        Ok(None) => break,
                        Err(()) => spin_loop(),
                    }
                }
            }
            Err(PopError::Closed)
        } else {
            Err(PopError::Empty)
        }
    }

    /// Closes the stack, existing elements can still be popped.
    pub fn close(&self) -> bool {
        let guard = &epoch::pin();
        let mut changed = false;

        for shard in self.shards.iter() {
            loop {
                let head = shard.load(Ordering::Acquire, guard);

                if head.tag() == CLOSED_TAG {
                    break;
                }

                match shard.compare_exchange_weak(
                    head,
                    head.with_tag(CLOSED_TAG),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(_) => {
                        changed = true;
                        break;
                    }
                    Err(_) => spin_loop(),
                }
            }
        }

        changed
    }

    /// Checks if the stack is closed.
    pub fn is_closed(&self) -> bool {
        let guard = &epoch::pin();
        self.all_shards_closed(guard)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn current_shard_index(&self) -> usize {
        current_thread_id() & self.shard_index_mask
    }

    fn shard_bit(index: usize) -> usize {
        1usize << index
    }

    /// One CAS attempt on shard `index`.
    ///
    /// * `Ok(Some(value))` — popped a value.
    /// * `Ok(None)`        — shard is empty (whether open or closed).
    /// * `Err(())`         — CAS lost to another thread; caller decides whether to retry or move on.
    ///
    /// Both the "empty on arrival" branch and the "last element popped"
    /// branch funnel through [`Self::maybe_clear_bit`], which handles the
    /// race between a clearing `fetch_and` here and a concurrent push's
    /// `fetch_or`. Without that race fix, the bit can get stuck at `0`
    /// over a non-empty shard, leaving the element invisible to stealers
    /// in Phase 2 until the stack is closed.
    fn pop_one(&self, index: usize, guard: &Guard) -> Result<Option<T>, ()> {
        let shard = &self.shards[index];
        let bit = Self::shard_bit(index);

        let head = shard.load(Ordering::Acquire, guard);
        if head.is_null() {
            self.maybe_clear_bit(shard, bit, guard);
            return Ok(None);
        }

        let head_ref = unsafe { head.deref() };
        // `head` was published by a Release CAS that we synchronised with
        // via the Acquire load above, so all of the node's fields are
        // already visible — a Relaxed load on `next` is enough.
        let next = head_ref.next.load(Ordering::Relaxed, guard);
        let new_head = next.with_tag(head.tag());

        // Strong CAS: weak's spurious failures would burn the bounded retry
        // budget in Phase 1/2 and leak a spurious `Empty` to the caller.
        // On x86 strong compiles to the same `lock cmpxchg` as weak; on
        // LL/SC the strong variant does the inner retry itself.
        match shard.compare_exchange(head, new_head, Ordering::AcqRel, Ordering::Relaxed, guard) {
            Ok(_) => {
                if next.is_null() {
                    self.maybe_clear_bit(shard, bit, guard);
                }

                // Move the value out of the popped node. The node itself
                // is reclaimed later by the epoch GC; because `value` is
                // a `ManuallyDrop<T>`, that deferred destruction will not
                // drop it again.
                let value = unsafe { ptr::read(&*head_ref.value) };
                unsafe {
                    guard.defer_destroy(head.with_tag(0));
                }
                Ok(Some(value))
            }
            Err(_) => Err(()),
        }
    }

    /// Clear `bit` for a shard we just observed as empty (either on
    /// arrival or because we popped its last element).
    ///
    /// The naive `fetch_and(!bit)` races with a concurrent push's
    /// `fetch_or(bit)`: if the push happens between our bitmap load and
    /// our `fetch_and`, our store clobbers the push's bit, leaving the
    /// bit at `0` over a now-non-empty shard. Stealers in Phase 2 skip
    /// shards with `bit == 0`, so the pushed element becomes invisible
    /// to anyone but the shard's local owner — and in an asymmetric
    /// workload (pure-pusher threads on some shards, pure-popper threads
    /// on others) the local owner never pops, so the element is stuck
    /// until the stack is `close`d (Phase 3 does a bitmap-agnostic
    /// drain).
    ///
    /// The fix is to re-read the shard after clearing: if a push slipped
    /// in (`head` is now non-null) we re-set the bit via `fetch_or`. The
    /// worst residual outcome is a transient stuck-at-`1` (wasted scan,
    /// self-heals on the next `pop_one(index)` which finds the shard
    /// truly empty), which is correctness-safe.
    ///
    /// The leading `load(Relaxed)` keeps the steady-state cost low: when
    /// the bit is already `0` we skip the `fetch_and` entirely and avoid
    /// invalidating the bitmap cache line.
    fn maybe_clear_bit(&self, shard: &Atomic<Node<T>>, bit: usize, guard: &Guard) {
        if self.bitmap.load(Ordering::Relaxed) & bit == 0 {
            return;
        }
        self.bitmap.fetch_and(!bit, Ordering::Release);
        if !shard.load(Ordering::Acquire, guard).is_null() {
            self.bitmap.fetch_or(bit, Ordering::Release);
        }
    }

    fn all_shards_closed(&self, guard: &Guard) -> bool {
        self.shards
            .iter()
            .all(|shard| shard.load(Ordering::Acquire, guard).tag() == CLOSED_TAG)
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
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

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

    /// Wait until `cond` returns true, or panic with `label` after `timeout`.
    /// Used by the loss-detection tests to turn "popper hangs forever because
    /// an element is invisible" into an explicit, debuggable failure rather
    /// than a CI timeout.
    fn wait_until<F: Fn() -> bool>(timeout: Duration, label: &str, cond: F) {
        let start = Instant::now();
        while !cond() {
            if start.elapsed() > timeout {
                panic!("{label}: condition not met within {timeout:?}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Pure-pusher / pure-popper split with **no** `close()`. Pushers map to a
    /// disjoint set of shards from poppers, so the shards that hold the
    /// pushed elements have no "local owner" that will ever pop them — every
    /// element has to be reached by stealers via the bitmap hint.
    ///
    /// This is the workload the bitmap stuck-at-0 race actually shows up in:
    /// if a pop's `clear_bitmap_bit_if_set` reorders past a concurrent push's
    /// `fetch_or`, the bit gets stuck at 0 over a non-empty shard, the
    /// stealers in Phase 2 skip it, and the element is invisible until the
    /// stack is closed. The test asserts every pushed element is observed by
    /// some popper within a generous timeout; otherwise it fails loudly.
    ///
    /// Skipped under Miri: this is a hardware-race stress test (busy-spin
    /// poppers, no `close()`, watchdog measured in wall-clock seconds), and
    /// Miri's deterministic cooperative scheduler neither exposes the race
    /// window this is designed to catch nor finishes the workload in any
    /// reasonable wall-clock budget. UB and data-race coverage for this
    /// shape is handled by `no_element_loss_after_close_asymmetric`.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn no_element_loss_open_asymmetric() {
        // Miri is too slow to hit this race repeatedly; keep workload tiny.
        #[cfg(miri)]
        let (n_pushers, n_poppers, per_pusher, rounds) = (2, 2, 200, 1);
        #[cfg(not(miri))]
        let (n_pushers, n_poppers, per_pusher, rounds) = (4, 12, 50_000, 4);

        for round in 0..rounds {
            let s = Arc::new(ConcurrentShardedStack::<usize>::with_concurrency(16));
            let popped = Arc::new(AtomicUsize::new(0));
            let pushed = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let total = n_pushers * per_pusher;

            let mut handles = Vec::new();
            for t in 0..n_pushers {
                let s = Arc::clone(&s);
                let pushed = Arc::clone(&pushed);
                handles.push(std::thread::spawn(move || {
                    for i in 0..per_pusher {
                        s.push(t * per_pusher + i).unwrap();
                        pushed.fetch_add(1, Ordering::Relaxed);
                    }
                }));
            }

            for _ in 0..n_poppers {
                let s = Arc::clone(&s);
                let popped = Arc::clone(&popped);
                let stop = Arc::clone(&stop);
                handles.push(std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        match s.pop() {
                            Ok(_) => {
                                popped.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(PopError::Empty) => std::hint::spin_loop(),
                            Err(PopError::Closed) => break,
                        }
                    }
                }));
            }

            // 30 s is enormous for this workload (<100 ms in practice); only
            // a real loss should ever exceed it.
            wait_until(Duration::from_secs(30), "popper drain (open)", || {
                popped.load(Ordering::Relaxed) >= total
            });
            stop.store(true, Ordering::Relaxed);

            for h in handles {
                h.join().unwrap();
            }

            let final_pushed = pushed.load(Ordering::Relaxed);
            let final_popped = popped.load(Ordering::Relaxed);
            assert_eq!(
                final_popped, final_pushed,
                "round {round}: popped {final_popped} != pushed {final_pushed}",
            );
        }
    }

    /// Same asymmetric split, but with the producer side `close()`ing the
    /// stack once it has pushed everything. This is what Phase 3 (the
    /// post-close full scan) exists for — it must catch every in-flight
    /// element even if the bitmap is lying about emptiness.
    #[test]
    fn no_element_loss_after_close_asymmetric() {
        #[cfg(miri)]
        let (n_pushers, n_poppers, per_pusher, rounds) = (2, 2, 200, 1);
        #[cfg(not(miri))]
        let (n_pushers, n_poppers, per_pusher, rounds) = (4, 12, 50_000, 4);

        for round in 0..rounds {
            let s = Arc::new(ConcurrentShardedStack::<usize>::with_concurrency(16));
            let popped = Arc::new(AtomicUsize::new(0));
            let total = n_pushers * per_pusher;

            let mut handles = Vec::new();
            for t in 0..n_pushers {
                let s = Arc::clone(&s);
                handles.push(std::thread::spawn(move || {
                    for i in 0..per_pusher {
                        s.push(t * per_pusher + i).unwrap();
                    }
                }));
            }

            for _ in 0..n_poppers {
                let s = Arc::clone(&s);
                let popped = Arc::clone(&popped);
                handles.push(std::thread::spawn(move || loop {
                    match s.pop() {
                        Ok(_) => {
                            popped.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(PopError::Empty) => std::hint::spin_loop(),
                        Err(PopError::Closed) => break,
                    }
                }));
            }

            // Wait for pushers to finish, then close. Poppers continue
            // draining until they observe Closed.
            let pusher_handles: Vec<_> = handles.drain(..n_pushers).collect();
            for h in pusher_handles {
                h.join().unwrap();
            }
            s.close();

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(
                popped.load(Ordering::Relaxed),
                total,
                "round {round}: drain after close lost elements",
            );
        }
    }

    /// Lopsided shard count: many shards, few threads. Most shards are owned
    /// by nobody, so a stealer that sees the bitmap is the only path to the
    /// elements pushed there. Catches loss-via-stealer-only paths.
    #[test]
    fn no_element_loss_few_threads_many_shards() {
        #[cfg(miri)]
        let (n_pushers, n_poppers, per_pusher, shards) = (2, 1, 100, 8);
        #[cfg(not(miri))]
        let (n_pushers, n_poppers, per_pusher, shards) = (2, 1, 100_000, 32);

        let s = Arc::new(ConcurrentShardedStack::<usize>::with_concurrency(shards));
        let popped = Arc::new(AtomicUsize::new(0));
        let total = n_pushers * per_pusher;

        let mut handles = Vec::new();
        for _ in 0..n_pushers {
            let s = Arc::clone(&s);
            handles.push(std::thread::spawn(move || {
                for i in 0..per_pusher {
                    s.push(i).unwrap();
                }
            }));
        }

        for _ in 0..n_poppers {
            let s = Arc::clone(&s);
            let popped = Arc::clone(&popped);
            handles.push(std::thread::spawn(move || {
                let start = Instant::now();
                while popped.load(Ordering::Relaxed) < total {
                    match s.pop() {
                        Ok(_) => {
                            popped.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(PopError::Empty) => std::hint::spin_loop(),
                        Err(PopError::Closed) => break,
                    }
                    if start.elapsed() > Duration::from_secs(30) {
                        panic!(
                            "popper stuck: popped {} of {}",
                            popped.load(Ordering::Relaxed),
                            total,
                        );
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(popped.load(Ordering::Relaxed), total);
    }
}
