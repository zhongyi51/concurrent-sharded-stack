//! Default backend. The intrusive algorithm itself has no Crossbeam dependency.
use crate::Guard;
use std::sync::atomic::{AtomicPtr, Ordering};

/// A lazily pinned guard in Crossbeam's default global epoch domain.
///
/// Call [`Self::collect`] on each retiring thread before waiting or becoming idle.
/// Other threads cannot flush that thread's local bag. Collection has no
/// deadline; a stalled pinned reader can delay reclamation indefinitely.
#[derive(Default)]
pub struct EpochGuard(Option<crossbeam_epoch::Guard>);

impl EpochGuard {
    /// Flush this thread's pending work and help collection; does not wait.
    pub fn collect() {
        crossbeam_epoch::pin().flush();
    }
}

// SAFETY: all guards use the global collector. Pin precedes head loads;
// Crossbeam delays actions until pre-existing pinned readers have finished.
unsafe impl Guard for EpochGuard {
    unsafe fn protect<T>(&mut self, head: &AtomicPtr<T>) -> *mut T {
        self.0.get_or_insert_with(crossbeam_epoch::pin);
        head.load(Ordering::Acquire)
    }
    fn unpin(&mut self) {
        self.0.take();
    }
    unsafe fn retire(&mut self, _: *mut (), action: impl FnOnce() + Send + 'static) {
        self.0
            .get_or_insert_with(crossbeam_epoch::pin)
            .defer(action);
    }
}
