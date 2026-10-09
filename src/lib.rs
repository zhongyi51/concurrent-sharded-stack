//! Sharded stacks with a shared intrusive core and pluggable reclamation.
//! Values are returned immediately; only empty node storage is retired and cached.
//! Intrusive nodes are delivered after their reclaimer allows safe reuse.
//!
//! ```
//! use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
//! let stack = ConcurrentShardedStack::with_concurrency(4);
//! stack.push(1).unwrap();
//! assert_eq!(stack.pop(), Ok(1));
//! assert_eq!(stack.pop(), Err(PopError::Empty));
//! ```
use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

mod core;
pub mod epoch;
pub mod intrusive;
mod reclaim;
mod value;
pub use epoch::Epoch;
pub use intrusive::{IntrusiveShardedStack, Retired};
pub use intrusive_collections;
pub use reclaim::Reclaimer;
pub use value::ConcurrentShardedStack;

static NEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static THREAD_ID: Cell<usize> = Cell::new(NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed));
}
fn current_thread_id() -> usize {
    THREAD_ID.with(Cell::get)
}
fn default_shards() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().next_power_of_two())
        .unwrap_or(4)
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

#[cfg(test)]
#[path = "tests/value.rs"]
mod tests;
