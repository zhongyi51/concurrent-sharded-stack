//! Shared intrusive head algorithm. No payload, allocation, or epoch operations.
use crate::{PopError, Reclaimer, current_thread_id};
use crossbeam_utils::CachePadded;
use intrusive_collections::singly_linked_list::SinglyLinkedListOps;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, Ordering};

pub(crate) fn untag<L>(p: *mut L) -> *mut L {
    p.map_addr(|a| a & !1)
}
fn closed<L>(p: *mut L) -> bool {
    p.addr() & 1 != 0
}

pub(crate) struct Core<L, R: Reclaimer> {
    shards: Box<[CachePadded<AtomicPtr<L>>]>,
    pub(crate) reclaimer: R,
}

impl<L, R: Reclaimer> Core<L, R> {
    pub(crate) fn new(count: usize, reclaimer: R) -> Self {
        assert!(
            count.is_power_of_two(),
            "shard_count must be a nonzero power of two"
        );
        assert!(
            align_of::<L>() >= 2,
            "link alignment must leave a close tag bit"
        );
        Self {
            shards: (0..count)
                .map(|_| CachePadded::new(AtomicPtr::new(ptr::null_mut())))
                .collect(),
            reclaimer,
        }
    }
    pub(crate) fn count(&self) -> usize {
        self.shards.len()
    }
    pub(crate) fn local(&self) -> usize {
        current_thread_id() & (self.count() - 1)
    }
    pub(crate) fn local_closed(&self) -> bool {
        closed(self.shards[self.local()].load(Ordering::Acquire))
    }

    // Caller owns a claimed unpublished node, with concurrent, interchangeable ops.
    pub(crate) unsafe fn push<O: SinglyLinkedListOps<LinkPtr = NonNull<L>>>(
        &self,
        link: NonNull<L>,
        ops: &mut O,
    ) -> bool {
        let shard = &self.shards[self.local()];
        let mut guard = self.reclaimer.pin();
        loop {
            // Protection also preserves provenance when copying head into next.
            let head = unsafe { self.reclaimer.protect(shard, &mut guard) };
            if closed(head) {
                return false;
            }
            unsafe {
                ops.set_next(link, NonNull::new(head));
            }
            if shard
                .compare_exchange_weak(head, link.as_ptr(), Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
            std::hint::spin_loop();
        }
    }

    // Winner receives unique removal ownership; link stays claimed/immutable.
    pub(crate) unsafe fn pop<O: SinglyLinkedListOps<LinkPtr = NonNull<L>>>(
        &self,
        ops: &O,
    ) -> Result<NonNull<L>, PopError> {
        let mut guard = self.reclaimer.pin();
        let mut all_closed = true;
        let start = self.local();
        for mask in 0..self.count() {
            let shard = &self.shards[start ^ mask];
            loop {
                let head = unsafe { self.reclaimer.protect(shard, &mut guard) };
                let Some(link) = NonNull::new(untag(head)) else {
                    all_closed &= closed(head);
                    break;
                };
                let next = unsafe { ops.next(link) }.map_or(ptr::null_mut(), NonNull::as_ptr);
                let next = next.map_addr(|a| a | (head.addr() & 1));
                if shard
                    .compare_exchange(head, next, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return Ok(link);
                }
                std::hint::spin_loop();
            }
            #[cfg(test)]
            crate::tests::after_empty_shard_scan();
        }
        Err(if all_closed {
            PopError::Closed
        } else {
            PopError::Empty
        })
    }

    pub(crate) fn close(&self) -> bool {
        let mut changed = false;
        let mut guard = self.reclaimer.pin();
        for shard in &self.shards {
            loop {
                let head = unsafe { self.reclaimer.protect(shard, &mut guard) };
                if closed(head) {
                    break;
                }
                if shard
                    .compare_exchange_weak(
                        head,
                        head.map_addr(|a| a | 1),
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    changed = true;
                    #[cfg(test)]
                    crate::tests::after_shard_closed();
                    break;
                }
            }
        }
        changed
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.shards
            .iter()
            .all(|s| closed(s.load(Ordering::Acquire)))
    }

    // Exclusive stack teardown. Advance before invoking any user destructor.
    pub(crate) unsafe fn take_exclusive<O: SinglyLinkedListOps<LinkPtr = NonNull<L>>>(
        &self,
        index: usize,
        ops: &O,
    ) -> Option<NonNull<L>> {
        let shard = &self.shards[index];
        let link = NonNull::new(untag(shard.load(Ordering::Relaxed)))?;
        let next = unsafe { ops.next(link) };
        shard.store(
            next.map_or(ptr::null_mut(), NonNull::as_ptr),
            Ordering::Relaxed,
        );
        Some(link)
    }
}
