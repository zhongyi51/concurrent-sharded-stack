//! The stack's only reclamation contract. Backends own their reader protocol.
use std::sync::atomic::AtomicPtr;

/// A reclamation domain shared by a stack, its readers, and its retired nodes.
///
/// `pin` starts an operation; `protect` loads a head safely; `retire` schedules
/// destruction **or reuse**. Implementations may batch work. `collect` need not
/// wait for completion. No operation is required to allocate, but lock-free
/// progress depends on the chosen backend as well as the adapter.
///
/// # Safety
/// - Clones must refer to the same domain, including after the stack is dropped.
/// - `protect` must perform at least an Acquire load and return the exact head
///   bits. Bit zero is a close tag: protect the address with that bit cleared.
///   Null (including tagged null) needs no protection. For hazard pointers,
///   publish protection and revalidate the source **before** returning.
/// - Protection lasts until that guard is dropped or used in another `protect`.
///   No retired action for the protected address may run during that interval.
/// - `retire` must execute its action at most once, only when all earlier valid
///   accesses to that address are finished. This permits rewriting the link,
///   reinsertion, and deallocation. Never drop an unexecuted action early:
///   its captures may own the allocation. Leaking pending work is safe.
/// - `retire` may execute an eligible action synchronously. It must not unwind
///   before accepting the action; an executed action's panic may propagate.
///   Cloning the domain must not panic. Guards must release protection on unwind.
pub unsafe trait Reclaimer: Clone + Send + Sync + 'static {
    /// Reader state; it need not be Send or Sync.
    type Guard<'a>
    where
        Self: 'a;

    /// Enter a read operation in this domain.
    fn pin(&self) -> Self::Guard<'_>;

    /// Load and protect a tagged head, replacing this guard's previous protection.
    ///
    /// # Safety
    /// `head` belongs to this domain; all removals preserve the old link until
    /// retired through this domain. The guard was created by this domain.
    unsafe fn protect<T>(&self, head: &AtomicPtr<T>, guard: &mut Self::Guard<'_>) -> *mut T;

    /// Schedule an action for an unlinked node's **untagged link address**.
    ///
    /// # Safety
    /// The caller owns this removed node, has not already retired it, and will
    /// not republish or mutate its link before the action. The action must keep
    /// the allocation alive, even if execution is delayed beyond stack drop.
    unsafe fn retire(&self, address: *mut (), action: impl FnOnce() + Send + 'static);

    /// Publish local pending work and/or help reclaim eligible nodes.
    fn collect(&self);
}
