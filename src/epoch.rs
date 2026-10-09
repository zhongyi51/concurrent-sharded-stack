//! Default backend. The intrusive algorithm itself has no Crossbeam dependency.
use crate::Reclaimer;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Crossbeam's default global epoch domain, with batched retirement.
///
/// Call `collect` on each retiring thread before waiting or becoming idle.
/// Other threads cannot flush that thread's local bag. Collection has no
/// deadline; a stalled pinned reader can delay reclamation indefinitely.
#[derive(Clone, Copy, Debug, Default)]
pub struct Epoch;

// SAFETY: every instance uses the same global collector. Pin precedes loads;
// Crossbeam delays actions until pre-existing pinned readers have finished.
unsafe impl Reclaimer for Epoch {
    type Guard<'a> = crossbeam_epoch::Guard;

    fn pin(&self) -> Self::Guard<'_> {
        crossbeam_epoch::pin()
    }

    unsafe fn protect<T>(&self, head: &AtomicPtr<T>, _: &mut Self::Guard<'_>) -> *mut T {
        head.load(Ordering::Acquire)
    }

    unsafe fn retire(&self, _: *mut (), action: impl FnOnce() + Send + 'static) {
        crossbeam_epoch::pin().defer(action);
    }

    fn collect(&self) {
        crossbeam_epoch::pin().flush();
    }
}
