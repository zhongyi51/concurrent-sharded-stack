//! Blocking reference hazard domain, independent of Crossbeam. The mutex
//! serializes hazard publication with collection; this is not a lock-free backend.
use concurrent_sharded_stack::Guard;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicPtr, Ordering},
};
use std::{marker::PhantomData, rc::Rc};
type Action = Box<dyn FnOnce() + Send>;
#[derive(Default)]
struct State {
    hazards: Vec<usize>,
    free: Vec<usize>,
    pending: Vec<(usize, Action)>,
    panic_protect: bool,
    retires: usize,
    unpins: usize,
}
#[derive(Clone, Default)]
pub struct Hazards(Arc<Mutex<State>>);
// Deliberately !Send + !Sync: only the factory/domain crosses threads.
pub struct HazardGuard {
    domain: Hazards,
    slot: Option<usize>,
    local: PhantomData<Rc<()>>,
}
impl Hazards {
    pub fn guard(&self) -> HazardGuard {
        HazardGuard {
            domain: self.clone(),
            slot: None,
            local: PhantomData,
        }
    }
    pub fn factory(&self) -> impl Fn() -> HazardGuard + Send + Sync + 'static {
        let domain = self.clone();
        move || domain.guard()
    }
    pub fn panic_next_protect(&self) {
        self.0.lock().unwrap().panic_protect = true;
    }
    pub fn counts(&self) -> (usize, usize, usize) {
        let state = self.0.lock().unwrap();
        (
            state.hazards.iter().filter(|&&p| p != 0).count(),
            state.retires,
            state.unpins,
        )
    }
    pub fn collect(&self) {
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
// No Drop cleanup: tests must observe the stack calling Guard::unpin itself.
// SAFETY: publication and retirement scanning hold the same mutex, preventing
// reclamation between load and hazard publication. Callbacks run outside it.
unsafe impl Guard for HazardGuard {
    unsafe fn protect<T>(&mut self, source: &AtomicPtr<T>) -> *mut T {
        let mut state = self.domain.0.lock().unwrap();
        let slot = *self.slot.get_or_insert_with(|| {
            state.free.pop().unwrap_or_else(|| {
                state.hazards.push(0);
                state.hazards.len() - 1
            })
        });
        let raw = loop {
            let raw = source.load(Ordering::Acquire);
            state.hazards[slot] = raw.addr() & !1;
            if source.load(Ordering::Acquire) == raw {
                break raw;
            }
        };
        let panic = std::mem::take(&mut state.panic_protect);
        drop(state);
        assert!(!panic, "injected protection panic");
        raw
    }
    fn unpin(&mut self) {
        if let Some(slot) = self.slot.take() {
            let mut state = self.domain.0.lock().unwrap();
            state.hazards[slot] = 0;
            state.free.push(slot);
            state.unpins += 1;
        }
    }
    unsafe fn retire(&mut self, address: *mut (), action: impl FnOnce() + Send + 'static) {
        {
            let mut state = self.domain.0.lock().unwrap();
            state.retires += 1;
            state.pending.push((address.addr(), Box::new(action)));
        }
        self.domain.collect(); // Exercise synchronous eligible callbacks too.
    }
}
