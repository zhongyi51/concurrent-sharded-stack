//! The only reclamation trait. Node layout belongs to upstream intrusive traits.
use std::sync::{Arc, atomic::AtomicPtr};

/// Per-operation protection and retirement, independent of node type.
///
/// `protect` enters protection lazily; `unpin` ends it. The stack calls `unpin`
/// on every scope exit, including unwinding. A guard need not be Send, Sync,
/// Clone or Default. A constructor closure supplies fresh guards in one domain.
/// Collection/flush controls belong to the backend, not this trait.
///
/// # Safety
/// - `protect` performs at least an Acquire load and returns the exact head
///   bits. Bit zero is a close tag: protect the address with that bit cleared.
///   Null (including tagged null) needs no protection. Hazard pointers must
///   publish protection and revalidate the source before returning.
/// - Protection lasts until `unpin` or the next `protect` on this guard. No
///   retired action for that address may run while it is protected. Repeated
///   `protect` calls must safely replace protection, including after `unpin`.
/// - `unpin` must not panic and must be idempotent. It ends protection, but must
///   not discard pending retirement work. A panicking `protect` must leave state
///   that `unpin` can clean up.
/// - `retire` runs its action at most once, only after all earlier
///   valid accesses to the address finish. This permits link mutation, reuse,
///   and deallocation. Never drop an unexecuted action early: its captures may
///   own the allocation. Leaking pending work is safe.
/// - `retire` works with an inactive guard as well. It may run an eligible action
///   synchronously, but must not unwind before accepting it. An executed action's
///   panic may propagate. Pending work must remain valid after guard/stack drop.
pub unsafe trait Guard: 'static {
    /// Load and protect a tagged head, replacing previous protection.
    /// # Safety
    /// The source and this guard belong to the same domain. Removed links remain
    /// alive and unchanged until retirement through that domain allows reuse.
    unsafe fn protect<T>(&mut self, head: &AtomicPtr<T>) -> *mut T;

    /// Release this guard's protection. Safe to call repeatedly.
    fn unpin(&mut self);

    /// Retire an unlinked node's **untagged link address**.
    /// # Safety
    /// Caller owns the removed node and has not retired it already. Its link
    /// remains unchanged/unpublished until the action, which keeps its allocation
    /// alive even if execution occurs after stack destruction.
    unsafe fn retire(&mut self, address: *mut (), action: impl FnOnce() + Send + 'static);
}

// Thin shared handle: retired callbacks retain the factory/domain without
// requiring G: Send, and small callbacks still fit Crossbeam's inline buffer.
pub(crate) type Factory<G> = Arc<Box<dyn Fn() -> G + Send + Sync>>;
pub(crate) struct Scoped<G: Guard>(pub(crate) G);
impl<G: Guard> Scoped<G> {
    pub(crate) fn new(factory: &Factory<G>) -> Self {
        Self(factory())
    }
}
impl<G: Guard> Drop for Scoped<G> {
    fn drop(&mut self) {
        self.0.unpin();
    }
}
