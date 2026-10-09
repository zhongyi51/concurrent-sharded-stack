//! A deliberately blocking reference hazard domain, independent of Crossbeam.
//! The mutex serializes publication with reclamation decisions. This tests the
//! address-based contract, not a production lock-free hazard implementation.
use concurrent_sharded_stack::Reclaimer;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicPtr, Ordering},
};

type Action = Box<dyn FnOnce() + Send>;
#[derive(Default)]
struct State {
    hazards: Vec<usize>,
    free: Vec<usize>,
    pending: Vec<(usize, Action)>,
}
#[derive(Clone, Default)]
pub struct Hazards(Arc<Mutex<State>>);
pub struct Guard {
    domain: Hazards,
    slot: usize,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let mut state = self.domain.0.lock().unwrap();
        state.hazards[self.slot] = 0;
        state.free.push(self.slot);
    }
}
// SAFETY: clones share one domain. Publication and retirement scanning hold the
// same mutex, preventing reclamation between the load and hazard publication.
// Only unprotected addresses are selected; callbacks execute outside the lock.
unsafe impl Reclaimer for Hazards {
    type Guard<'a> = Guard;
    fn pin(&self) -> Guard {
        let mut state = self.0.lock().unwrap();
        let slot = state.free.pop().unwrap_or_else(|| {
            state.hazards.push(0);
            state.hazards.len() - 1
        });
        Guard {
            domain: self.clone(),
            slot,
        }
    }
    unsafe fn protect<T>(&self, source: &AtomicPtr<T>, guard: &mut Guard) -> *mut T {
        assert!(Arc::ptr_eq(&self.0, &guard.domain.0));
        let mut state = self.0.lock().unwrap();
        loop {
            let raw = source.load(Ordering::Acquire);
            state.hazards[guard.slot] = raw.addr() & !1;
            if source.load(Ordering::Acquire) == raw {
                return raw;
            }
        }
    }
    unsafe fn retire(&self, address: *mut (), action: impl FnOnce() + Send + 'static) {
        self.0
            .lock()
            .unwrap()
            .pending
            .push((address.addr(), Box::new(action)));
        self.collect(); // Also exercise backends that reclaim synchronously.
    }
    fn collect(&self) {
        let ready = {
            let mut state = self.0.lock().unwrap();
            let mut ready = Vec::new();
            let mut i = 0;
            while i < state.pending.len() {
                if state.hazards.contains(&state.pending[i].0) {
                    i += 1;
                } else {
                    ready.push(state.pending.swap_remove(i).1);
                }
            }
            ready
        };
        for action in ready {
            action();
        }
    }
}
