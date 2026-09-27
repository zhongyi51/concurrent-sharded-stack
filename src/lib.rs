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
    /// The target shard has been closed.
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
    /// No item was found by this scan; concurrent activity may hide items.
    Empty,
    /// Every shard was observed closed and empty; no later push can succeed.
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

enum ShardPopResult<T> {
    Popped(T),
    Empty,
    EmptyAndClosed,
}

/// Concurrent sharded Treiber stack.
///
/// Ordering is LIFO within each shard, not across shards. An unsuccessful
/// [`pop`](Self::pop) is a scan, not an atomic snapshot: `Empty` can be returned
/// even when the stack remained nonempty throughout the call. Closing also
/// takes effect one shard at a time; see [`close`](Self::close).
pub struct ConcurrentShardedStack<T> {
    shards: Box<[CachePadded<Atomic<Node<T>>>]>,
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
            .map(|n| n.get().next_power_of_two())
            .unwrap_or(4);

        Self::with_concurrency(concurrency)
    }

    /// Creates a stack with exact shard count.
    ///
    /// Panics if:
    ///
    /// - `shard_count == 0`
    /// - `shard_count` is not power of two
    pub fn with_concurrency(shard_count: usize) -> Self {
        assert!(shard_count > 0, "shard_count must be non-zero");

        assert!(
            shard_count.is_power_of_two(),
            "shard_count must be a power of two"
        );

        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            shards.push(CachePadded::new(Atomic::null()));
        }

        Self {
            shards: shards.into_boxed_slice(),
            shard_index_mask: shard_count - 1,
        }
    }

    /// Pushes a value into the stack.
    ///
    /// Returns `Err(PushError::Closed(value))` if this thread's shard is closed.
    /// While [`close`](Self::close) is in progress, some shards may reject
    /// pushes while others still accept them. After `close` returns, every
    /// shard rejects pushes.
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
                Ok(_) => return Ok(()),
                Err(err) => {
                    node = err.new;
                    spin_loop();
                }
            }
        }
    }

    /// Pops a value from the stack non-blocking.
    ///
    /// Scan order:
    ///
    /// - first scan the current thread's own shard;
    /// - then scan the shard whose last binary bit differs;
    /// - then scan shards whose next binary bit differs;
    /// - continue widening the differing-bit window until all shards are scanned.
    ///
    /// For example, with 8 shards and local shard `start`, the XOR masks are:
    ///
    /// ```text
    /// 0,
    /// 1,
    /// 2, 3,
    /// 4, 5, 6, 7
    /// ```
    ///
    /// So the actual scan order is:
    ///
    /// ```text
    /// start,
    /// start ^ 1,
    /// start ^ 2, start ^ 3,
    /// start ^ 4, start ^ 5, start ^ 6, start ^ 7
    /// ```
    ///
    /// Returns `Err(PopError::Empty)` if no element is found and at least one
    /// shard is still observed open. This is not a consistent snapshot: other
    /// threads can push into an already-scanned shard and drain a later shard,
    /// so `Empty` can occur even if the stack was never globally empty during
    /// the call. Consumers with active producers should retry rather than use
    /// `Empty` as a completion signal.
    ///
    /// Returns `Err(PopError::Closed)` if no element is found and every shard
    /// is observed closed. Unlike `Empty`, this is terminal: closed shards
    /// cannot receive more elements.
    pub fn pop(&self) -> Result<T, PopError> {
        let guard = &epoch::pin();
        let start = self.current_shard_index();
        let shard_count = self.shards.len();

        let mut all_closed = true;

        for mask in 0..shard_count {
            let index = start ^ mask;

            match self.pop_one(index, guard) {
                ShardPopResult::Popped(value) => return Ok(value),
                ShardPopResult::Empty => {
                    all_closed = false;
                }
                ShardPopResult::EmptyAndClosed => {}
            }
            #[cfg(test)]
            tests::after_empty_shard_scan();
        }

        if all_closed {
            Err(PopError::Closed)
        } else {
            Err(PopError::Empty)
        }
    }

    /// Closes every shard. Existing elements can still be popped.
    ///
    /// Closure propagates one shard at a time. During the call, a push can be
    /// rejected on one shard while a later push succeeds on another, and
    /// [`is_closed`](Self::is_closed) can still return `false` after a rejected
    /// push. Once this method returns, all shards are closed.
    ///
    /// Returns `true` if this call closed at least one shard. Concurrent calls
    /// can both return `true`; the result does not identify a unique closer.
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
                        #[cfg(test)]
                        tests::after_shard_closed();
                        break;
                    }
                    Err(_) => spin_loop(),
                }
            }
        }

        changed
    }

    /// Checks whether every shard is closed.
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

    /// Pops from one shard.
    ///
    /// This function loops until the shard produces a definite result:
    ///
    /// - popped one value;
    /// - or the shard is observed empty.
    ///
    /// CAS failures are retried indefinitely for this shard. There is no fixed
    /// retry budget anymore.
    ///
    /// Returns:
    ///
    /// - `value: Some(T)` if a value was popped;
    /// - `value: None` if this shard is currently empty;
    /// - `closed` indicates whether this shard was observed closed.
    fn pop_one(&self, index: usize, guard: &Guard) -> ShardPopResult<T> {
        let shard = &self.shards[index];

        loop {
            let head = shard.load(Ordering::Acquire, guard);
            let closed = head.tag() == CLOSED_TAG;

            if head.is_null() {
                return if closed {
                    ShardPopResult::EmptyAndClosed
                } else {
                    ShardPopResult::Empty
                };
            }

            let head_ref = unsafe { head.deref() };

            // `head` was published by a Release CAS that we synchronized with
            // via the Acquire load above, so the node's fields are visible.
            let next = head_ref.next.load(Ordering::Relaxed, guard);

            // Preserve the CLOSED tag when popping from a closed shard.
            let new_head = next.with_tag(head.tag());

            match shard.compare_exchange(head, new_head, Ordering::AcqRel, Ordering::Relaxed, guard)
            {
                Ok(_) => {
                    // Move the value out of the popped node. The node itself
                    // is reclaimed later by epoch GC; because `value` is inside
                    // `ManuallyDrop<T>`, deferred destruction will not drop it again.
                    let value = unsafe { ptr::read(&*head_ref.value) };

                    unsafe {
                        guard.defer_destroy(head.with_tag(0));
                    }

                    return ShardPopResult::Popped(value);
                }
                Err(_) => {
                    spin_loop();
                }
            }
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
        let drain = || {
            for shard in &self.shards {
                loop {
                    let current = shard.load(Ordering::Relaxed, guard);
                    if current.is_null() {
                        break;
                    }

                    // SAFETY: dropping the stack requires exclusive access,
                    // so no operation can still access its reachable nodes.
                    // Advance the head before user code so a panic leaves only
                    // the remaining nodes reachable for the cleanup guard.
                    unsafe {
                        let mut node = Box::from_raw(current.as_raw() as *mut Node<T>);
                        let next = node.next.load(Ordering::Relaxed, guard);
                        shard.store(next, Ordering::Relaxed);
                        ManuallyDrop::drop(&mut node.value);
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
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, mpsc};
    use std::time::{Duration, Instant};

    // One-shot, per-thread hooks expose deterministic interleavings without
    // adding any instructions to non-test builds.
    type TestHook = Box<dyn FnOnce()>;
    thread_local! {
        static AFTER_EMPTY_SHARD: Cell<Option<TestHook>> = const { Cell::new(None) };
        static AFTER_SHARD_CLOSED: Cell<Option<TestHook>> = const { Cell::new(None) };
    }

    pub(super) fn after_empty_shard_scan() {
        AFTER_EMPTY_SHARD.with(|hook| {
            if let Some(hook) = hook.take() {
                hook();
            }
        });
    }

    pub(super) fn after_shard_closed() {
        AFTER_SHARD_CLOSED.with(|hook| {
            if let Some(hook) = hook.take() {
                hook();
            }
        });
    }

    #[test]
    fn empty_scan_is_not_a_global_snapshot() {
        let s = Arc::new(ConcurrentShardedStack::with_concurrency(2));
        THREAD_ID.with(|id| id.set(1));
        s.push("original").unwrap();

        let (scanned_tx, scanned_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let stack = Arc::clone(&s);
        let popper = thread::spawn(move || {
            THREAD_ID.with(|id| id.set(0));
            AFTER_EMPTY_SHARD.with(|hook| {
                hook.set(Some(Box::new(move || {
                    scanned_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                })));
            });
            stack.pop()
        });

        scanned_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        THREAD_ID.with(|id| id.set(0));
        s.push("replacement").unwrap();
        THREAD_ID.with(|id| id.set(1));
        assert_eq!(s.pop(), Ok("original"));
        // At least one item has existed throughout the pending pop.
        resume_tx.send(()).unwrap();
        assert_eq!(popper.join().unwrap(), Err(PopError::Empty));
        assert_eq!(s.pop(), Ok("replacement"));
    }

    #[test]
    fn close_propagates_per_shard_and_can_have_multiple_winners() {
        let s = Arc::new(ConcurrentShardedStack::with_concurrency(2));
        let (closed_tx, closed_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let stack = Arc::clone(&s);
        let closer = thread::spawn(move || {
            AFTER_SHARD_CLOSED.with(|hook| {
                hook.set(Some(Box::new(move || {
                    closed_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                })));
            });
            stack.close()
        });

        closed_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        THREAD_ID.with(|id| id.set(0));
        assert_eq!(s.push(1), Err(PushError::Closed(1)));
        assert!(!s.is_closed());
        THREAD_ID.with(|id| id.set(1));
        assert_eq!(s.push(2), Ok(()));
        assert!(s.close());
        assert!(s.is_closed());
        resume_tx.send(()).unwrap();
        assert!(closer.join().unwrap());
        assert_eq!(s.pop(), Ok(2));
        assert_eq!(s.pop(), Err(PopError::Closed));
    }

    #[test]
    fn payload_panic_still_drops_remaining_nodes_and_shards() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        struct Payload {
            panic: bool,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
                assert!(!self.panic, "payload destructor panic");
            }
        }

        for first_shard in [0, 2] {
            let drops = Arc::new(AtomicUsize::new(0));
            let s = ConcurrentShardedStack::with_concurrency(4);
            for (shard, panic) in [(first_shard, false), (first_shard, true), (3, false)] {
                THREAD_ID.with(|id| id.set(shard));
                assert!(
                    s.push(Payload {
                        panic,
                        drops: Arc::clone(&drops)
                    })
                    .is_ok()
                );
            }
            assert!(catch_unwind(AssertUnwindSafe(|| drop(s))).is_err());
            assert_eq!(drops.load(Ordering::Relaxed), 3);
        }
    }

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

        // Request reclamation; flush does not guarantee that every deferred
        // callback runs here. Payloads must already have been dropped by pop.
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
    /// element has to be reached by a popper whose local shard differs from
    /// the pusher's, i.e. via the cross-shard scan in [`Self::pop`].
    ///
    /// This is the workload that exercises the worst case of the sharded
    /// scan: there is no fast path to the elements (no popper-thread ever
    /// hits its own shard for these pushes), and poppers must keep walking
    /// the full XOR-mask order until the elements are drained. The test
    /// asserts every pushed element is observed by some popper within a
    /// generous timeout; otherwise it fails loudly.
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
    /// stack once it has pushed everything. The post-`close()` drain must
    /// catch every in-flight element: with no `close()`, a popper can race
    /// indefinitely against pushers and keep observing `Empty` between
    /// pushes, so closing turns the open-ended scan into a finite one.
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
                handles.push(std::thread::spawn(move || {
                    loop {
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
    /// by nobody, so a popper only reaches the elements pushed there via the
    /// cross-shard scan in [`Self::pop`] (its own local shard is empty for
    /// those pushers). Catches loss via the cross-shard scan path.
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
