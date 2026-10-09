//! A value-owning sharded stack backed by `concurrent-intrusive-collections`.
//!
//! Values are LIFO within each shard; empty scans and close are not global atomic
//! operations. The underlying intrusive nodes are private implementation details.
//! Payloads require `Send + 'static`, but do not need `Sync` or `Clone`.
//!
//! ```
//! use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
//! let stack = ConcurrentShardedStack::with_concurrency(4);
//! stack.push(1).unwrap();
//! stack.push(2).unwrap();
//! assert_eq!(stack.pop(), Ok(2));
//! assert_eq!(stack.pop(), Ok(1));
//! assert_eq!(stack.pop(), Err(PopError::Empty));
//! ```

#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(missing_docs)]

use concurrent_intrusive_collections::{
    AtomicLink as Atomic, ConcurrentShardedStack as IntrusiveStack, SinglyLinked,
};
use std::cell::UnsafeCell;
use std::fmt;

pub use concurrent_intrusive_collections::PopError;

/// A rejected push, retaining ownership of the original value.
#[derive(Debug, PartialEq, Eq)]
pub enum PushError<T> {
    /// The target shard was closed.
    Closed(T),
}

impl<T> PushError<T> {
    /// Recovers the value that was not inserted.
    pub fn into_inner(self) -> T {
        match self {
            Self::Closed(value) => value,
        }
    }
}

impl<T> fmt::Display for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stack is closed")
    }
}
impl<T: fmt::Debug> std::error::Error for PushError<T> {}

struct Node<T> {
    next: Atomic<Self>,
    value: UnsafeCell<Option<T>>,
}

// SAFETY: the link is a fixed private field, never exposed to users or changed
// by this wrapper after publication. Drop does not follow the non-owning link.
unsafe impl<T> SinglyLinked for Node<T> {
    unsafe fn next(&self) -> &Atomic<Self> {
        &self.next
    }
}

// SAFETY: concurrent stack readers access only next. Exactly one successful pop
// callback takes value, while its epoch guard prevents Node from being dropped.
// Rejection accesses an unpublished Owned<Node>; destruction has exclusive
// access after the grace period. No payload references escape through this type.
// A value can move between threads, so T: Send is required; T: Sync is not.
unsafe impl<T: Send> Sync for Node<T> {}

impl<T> Node<T> {
    fn new(value: T) -> Self {
        Self {
            next: Atomic::null(),
            value: UnsafeCell::new(Some(value)),
        }
    }

    // Caller must be the unique successful pop callback for this node. This
    // does not mutate links or form an exclusive reference to the complete node.
    unsafe fn take(&self) -> T {
        unsafe { &mut *self.value.get() }
            .take()
            .expect("node consumed twice")
    }
}

/// A value-owning concurrent stack with LIFO ordering within each shard.
///
/// Delegates linking, XOR probing, closing and epoch reclamation to
/// `concurrent-intrusive-collections`. A private node allocation stores each
/// value. A successful pop moves out the value immediately; only the empty node
/// allocation waits for the epoch grace period. No intrusive API is exposed.
///
/// `Empty` describes a scan, not a global snapshot. Close takes effect per shard.
/// Values may be sent to another thread, but need not be shareable by reference:
///
/// ```
/// use std::cell::Cell;
/// use concurrent_sharded_stack::ConcurrentShardedStack;
/// let stack = ConcurrentShardedStack::new();
/// stack.push(Cell::new(7)).unwrap();
/// assert_eq!(stack.pop().unwrap().get(), 7);
/// ```
pub struct ConcurrentShardedStack<T: Send + 'static> {
    inner: IntrusiveStack<Node<T>>,
}

impl<T: Send + 'static> Default for ConcurrentShardedStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> ConcurrentShardedStack<T> {
    /// Uses available parallelism rounded up to a power of two (fallback: four).
    pub fn new() -> Self {
        Self {
            inner: IntrusiveStack::new(),
        }
    }

    /// Uses exactly `shard_count` shards.
    ///
    /// # Panics
    /// Panics unless the count is a nonzero power of two.
    pub fn with_concurrency(shard_count: usize) -> Self {
        Self {
            inner: IntrusiveStack::with_concurrency(shard_count),
        }
    }

    /// Pushes a value to this thread's shard, or returns it if that shard is closed.
    pub fn push(&self, value: T) -> Result<(), PushError<T>> {
        self.inner
            .push(Box::new(Node::new(value)))
            .map_err(|error| {
                let mut node = error.into_inner();
                PushError::Closed(
                    node.value
                        .get_mut()
                        .take()
                        .expect("unpublished node is occupied"),
                )
            })
    }

    /// Removes and returns one owned value, scanning local then XOR-ordered shards.
    ///
    /// A successful pop takes the payload exactly once while the node is pinned.
    /// With active producers, retry `Empty`; it is not a completion signal.
    pub fn pop(&self) -> Result<T, PopError> {
        // SAFETY: the underlying stack invokes this callback only for its unique
        // head-CAS winner, with the removed node protected by an epoch guard.
        self.inner.pop_with(|node| unsafe { node.take() })
    }

    /// Closes every shard, preserving queued values for draining.
    ///
    /// Returns whether this call closed at least one shard. Concurrent callers
    /// may both return true. After return all subsequent pushes are rejected.
    pub fn close(&self) -> bool {
        self.inner.close()
    }

    /// Returns whether every shard was observed closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Returns whether every shard was observed empty during a scan.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of shards.
    pub fn shard_count(&self) -> usize {
        self.inner.shard_count()
    }
}

impl<T: Send + 'static> Drop for ConcurrentShardedStack<T> {
    fn drop(&mut self) {
        // Preserve synchronous payload destruction, including draining the rest
        // after one destructor panics. A second panic during unwind aborts as usual.
        let cleanup = scopeguard::guard(&self.inner, |inner| {
            while let Ok(value) = inner.pop_with(|node| unsafe { node.take() }) {
                drop(value);
            }
        });
        while let Ok(value) = cleanup.pop_with(|node| unsafe { node.take() }) {
            drop(value);
        }
        // All take calls above are unique successful pops. Deferred nodes are
        // now empty; their later destruction cannot access a popped payload.
    }
}

#[cfg(test)]
#[path = "tests/value.rs"]
mod tests;
