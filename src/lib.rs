//! Sharded concurrent stacks for unordered object recycling.
//!
//! [`ConcurrentShardedStack`] uses a lock-free Treiber stack and epoch GC.
//! With the default `intrusive` feature, `IntrusiveShardedStack` uses embedded
//! links and returns retired nodes for reuse after an epoch grace period.
//! Both provide LIFO ordering within a shard, without global ordering.
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

#[cfg(feature = "intrusive")]
pub mod intrusive;
#[cfg(feature = "intrusive")]
pub use intrusive::{ConcurrentLinkOps, EpochAdapter, IntrusiveShardedStack, Retired};
#[cfg(feature = "intrusive")]
pub use intrusive_collections;

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
    /// Scans the local shard first, then `start ^ 1`, `start ^ 2`, and so on.
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

    /// Retries CAS until this shard yields a value or is observed empty.
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
#[path = "tests/value.rs"]
mod tests;
