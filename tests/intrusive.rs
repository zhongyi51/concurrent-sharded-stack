use concurrent_sharded_stack::{
    IntrusiveShardedStack, PopError, PushError, Retired, intrusive, intrusive_collections,
};
use intrusive_collections::{SinglyLinkedList, SinglyLinkedListAtomicLink, intrusive_adapter};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::thread;

#[derive(Debug, Default)]
struct Entry {
    link: SinglyLinkedListAtomicLink,
    id: usize,
}
intrusive_adapter!(BoxAdapter = Box<Entry>: Entry { link => SinglyLinkedListAtomicLink });
intrusive_adapter!(ArcAdapter = Arc<Entry>: Entry { link => SinglyLinkedListAtomicLink });
// SAFETY: generated stateless adapters and owning pointers satisfy the constructor contract.
fn entry(id: usize) -> Box<Entry> {
    Box::new(Entry {
        id,
        ..Entry::default()
    })
}

fn drive_until(mut ready: impl FnMut() -> bool) {
    for _ in 0..100_000 {
        if ready() {
            return;
        }
        intrusive::collect();
        thread::yield_now();
    }
    panic!("epoch callbacks did not finish");
}
fn receive<A>(retired: Retired<A>) -> <A::PointerOps as intrusive_collections::PointerOps>::Pointer
where
    A: intrusive_collections::Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: intrusive_collections::singly_linked_list::SinglyLinkedListOps<
            LinkPtr = std::ptr::NonNull<SinglyLinkedListAtomicLink>,
        > + Clone,
    <A::PointerOps as intrusive_collections::PointerOps>::Pointer: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    retired.defer(move |node| tx.send(node).ok().unwrap());
    let mut result = None;
    drive_until(|| {
        result = rx.try_recv().ok();
        result.is_some()
    });
    result.unwrap()
}

#[test]
fn box_identity_lifo_and_upstream_transfer_after_grace_period() {
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(1, BoxAdapter::new()) };
    let a = entry(1);
    let address = &*a as *const Entry;
    stack.push(a).unwrap();
    stack.push(entry(2)).unwrap();
    assert_eq!(receive(stack.pop().unwrap()).id, 2);
    let a = receive(stack.pop().unwrap());
    assert_eq!(&*a as *const Entry, address);
    assert!(!a.link.is_linked());
    let mut list = SinglyLinkedList::new(BoxAdapter::new());
    list.push_front(a);
    stack.push(list.pop_front().unwrap()).unwrap();
    assert_eq!(&*receive(stack.pop().unwrap()) as *const Entry, address);
    assert!(matches!(stack.pop(), Err(PopError::Empty)));
}

#[test]
fn old_reader_prevents_delivery_release_and_reinsertion() {
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(1, ArcAdapter::new()) };
    let a = Arc::new(Entry::default());
    stack.push(a.clone()).unwrap();
    let guard = crossbeam_epoch::pin();
    let retired = stack.pop().unwrap();
    let (tx, rx) = mpsc::channel();
    retired.defer(move |node| tx.send(node).unwrap());
    // Publish the partial batch while the old reader is still pinned: a missing
    // flush must not be the reason the following delivery assertion passes.
    intrusive::collect();
    thread::spawn(|| {
        for _ in 0..64 {
            intrusive::collect();
        }
    })
    .join()
    .unwrap();
    assert!(rx.try_recv().is_err());
    assert!(a.link.is_linked());
    assert!(catch_unwind(AssertUnwindSafe(|| stack.push(a.clone()))).is_err());
    assert_eq!(Arc::strong_count(&a), 2);
    drop(guard);
    let mut node = None;
    drive_until(|| {
        node = rx.try_recv().ok();
        node.is_some()
    });
    let node = node.unwrap();
    assert!(Arc::ptr_eq(&a, &node));
    assert!(!node.link.is_linked());
    stack.push(node).unwrap();
    drop(stack);
    assert_eq!(Arc::strong_count(&a), 1);
}

#[test]
fn retired_token_outlives_stack_and_can_move_between_threads() {
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(4, BoxAdapter::new()) };
    stack.push(entry(9)).unwrap();
    let retired = stack.pop().unwrap();
    drop(stack);
    let node = thread::spawn(move || receive(retired)).join().unwrap();
    assert_eq!(node.id, 9);
    assert!(!node.link.is_linked());
}

#[test]
fn flushed_partial_batch_can_be_collected_while_retiring_thread_is_idle() {
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(1, BoxAdapter::new()) };
    stack.push(entry(7)).unwrap();
    let retired = stack.pop().unwrap();
    // Ensure the retiring thread cannot deliver its own callback before idle.
    let old_reader = crossbeam_epoch::pin();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let (node_tx, node_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        retired.defer(move |node| node_tx.send(node).unwrap());
        intrusive::collect();
        ready_tx.send(()).unwrap();
        stop_rx.recv().unwrap();
    });
    ready_rx.recv().unwrap();
    assert!(node_rx.try_recv().is_err());
    drop(old_reader);
    let mut delivered = None;
    drive_until(|| {
        delivered = node_rx.try_recv().ok();
        delivered.is_some()
    });
    let node = delivered.unwrap();
    assert_eq!(node.id, 7);
    assert!(!node.link.is_linked());
    stop_tx.send(()).unwrap();
    worker.join().unwrap();
}

#[test]
fn thread_exit_flushes_partial_retirement_batch() {
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(1, BoxAdapter::new()) };
    stack.push(entry(11)).unwrap();
    let retired = stack.pop().unwrap();
    let old_reader = crossbeam_epoch::pin();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || retired.defer(move |node| tx.send(node).unwrap()))
        .join()
        .unwrap();
    assert!(rx.try_recv().is_err());
    drop(old_reader);
    let mut delivered = None;
    drive_until(|| {
        delivered = rx.try_recv().ok();
        delivered.is_some()
    });
    assert_eq!(delivered.unwrap().id, 11);
}

#[test]
fn duplicate_acquisition_across_collections_has_one_winner() {
    let a = Arc::new(Entry::default());
    let stacks = [
        unsafe { IntrusiveShardedStack::with_concurrency(1, ArcAdapter::new()) },
        unsafe { IntrusiveShardedStack::with_concurrency(1, ArcAdapter::new()) },
    ];
    let barrier = Barrier::new(2);
    thread::scope(|scope| {
        let handles: Vec<_> = stacks
            .iter()
            .map(|stack| {
                let a = &a;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    catch_unwind(AssertUnwindSafe(|| stack.push(a.clone()))).is_ok()
                })
            })
            .collect();
        assert_eq!(
            handles
                .into_iter()
                .map(|h| usize::from(h.join().unwrap()))
                .sum::<usize>(),
            1
        );
    });
    drop(stacks);
    assert!(!a.link.is_linked());
    assert_eq!(Arc::strong_count(&a), 1);
}

#[derive(Debug)]
struct Counted {
    link: SinglyLinkedListAtomicLink,
    drops: Arc<AtomicUsize>,
    panic: bool,
}
impl Drop for Counted {
    fn drop(&mut self) {
        assert!(!self.link.is_linked());
        self.drops.fetch_add(1, Ordering::Relaxed);
        assert!(!self.panic, "test destructor panic");
    }
}
intrusive_adapter!(CountedAdapter = Box<Counted>: Counted { link => SinglyLinkedListAtomicLink });

#[test]
fn dropped_retired_token_defers_exactly_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(1, CountedAdapter::new()) };
    stack
        .push(Box::new(Counted {
            link: SinglyLinkedListAtomicLink::new(),
            drops: drops.clone(),
            panic: false,
        }))
        .unwrap();
    let guard = crossbeam_epoch::pin();
    drop(stack.pop().unwrap());
    drop(stack);
    for _ in 0..64 {
        intrusive::collect();
    }
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(guard);
    drive_until(|| drops.load(Ordering::Relaxed) == 1);
}

#[test]
fn destructor_panic_drains_remaining_live_nodes() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(4, CountedAdapter::new()) };
    thread::scope(|scope| {
        for worker in 0..4 {
            let stack = &stack;
            let drops = &drops;
            scope.spawn(move || {
                for i in 0..3 {
                    stack
                        .push(Box::new(Counted {
                            link: SinglyLinkedListAtomicLink::new(),
                            drops: drops.clone(),
                            panic: worker == 0 && i == 2,
                        }))
                        .unwrap();
                }
            });
        }
    });
    assert!(catch_unwind(AssertUnwindSafe(|| drop(stack))).is_err());
    assert_eq!(drops.load(Ordering::Relaxed), 12);
}

#[test]
fn concurrent_transfer_delivers_each_id_once() {
    let count = if cfg!(miri) { 8 } else { 4000 };
    for shards in [1, 4, 16] {
        let stack = unsafe { IntrusiveShardedStack::with_concurrency(shards, BoxAdapter::new()) };
        let seen = Arc::new(
            (0..4 * count)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        thread::scope(|scope| {
            for _ in 0..3 {
                let stack = &stack;
                let seen = &seen;
                scope.spawn(move || {
                    loop {
                        match stack.pop() {
                            Ok(retired) => {
                                let seen = seen.clone();
                                retired.defer(move |a| {
                                    assert!(!a.link.is_linked());
                                    assert_eq!(seen[a.id].fetch_add(1, Ordering::Relaxed), 0);
                                });
                            }
                            Err(PopError::Empty) => {
                                intrusive::collect();
                                thread::yield_now();
                            }
                            Err(PopError::Closed) => break,
                        }
                    }
                });
            }
            let producers: Vec<_> = (0..4)
                .map(|worker| {
                    let stack = &stack;
                    scope.spawn(move || {
                        for i in 0..count {
                            stack.push(entry(worker * count + i)).unwrap();
                        }
                    })
                })
                .collect();
            for h in producers {
                h.join().unwrap();
            }
            stack.close();
        });
        drive_until(|| seen.iter().all(|n| n.load(Ordering::Relaxed) == 1));
    }
}

#[test]
fn callback_can_reinsert_without_aba_or_lost_nodes() {
    let stack = Arc::new(unsafe { IntrusiveShardedStack::with_concurrency(4, BoxAdapter::new()) });
    let delivered = Arc::new(AtomicUsize::new(0));
    for id in 0..16 {
        stack.push(entry(id)).unwrap();
    }
    let count = if cfg!(miri) { 8 } else { 2000 };
    thread::scope(|scope| {
        for _ in 0..4 {
            let stack = &stack;
            let delivered = &delivered;
            scope.spawn(move || {
                for _ in 0..count {
                    let mut retired = None;
                    drive_until(|| {
                        retired = stack.pop().ok();
                        retired.is_some()
                    });
                    let stack = stack.clone();
                    let delivered = delivered.clone();
                    retired.unwrap().defer(move |node| {
                        stack.push(node).unwrap();
                        delivered.fetch_add(1, Ordering::Release);
                    });
                }
            });
        }
    });
    drive_until(|| delivered.load(Ordering::Acquire) == 4 * count);
    stack.close();
    let mut ids = Vec::new();
    while let Ok(retired) = stack.pop() {
        ids.push(receive(retired).id);
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..16).collect::<Vec<_>>());
}

#[test]
fn racing_close_preserves_accepted_and_rejected_nodes() {
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(4, BoxAdapter::new()) };
    let barrier = Barrier::new(5);
    let count = if cfg!(miri) { 8 } else { 1000 };
    let mut ids = thread::scope(|scope| {
        let closer = scope.spawn(|| {
            barrier.wait();
            stack.close();
        });
        let producers: Vec<_> = (0..4)
            .map(|worker| {
                let stack = &stack;
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut rejected = Vec::new();
                    barrier.wait();
                    for i in 0..count {
                        if let Err(PushError::Closed(a)) = stack.push(entry(worker * count + i)) {
                            assert!(!a.link.is_linked());
                            rejected.push(a.id);
                        }
                    }
                    rejected
                })
            })
            .collect();
        closer.join().unwrap();
        producers
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(stack.is_closed());
    assert!(!stack.close());
    let node = entry(usize::MAX);
    let address = &*node as *const Entry;
    let node = stack.push(node).unwrap_err().into_inner();
    assert_eq!(&*node as *const Entry, address);
    assert!(!node.link.is_linked());
    loop {
        match stack.pop() {
            Ok(a) => ids.push(receive(a).id),
            Err(PopError::Closed) => break,
            Err(PopError::Empty) => panic!("all shards already closed"),
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..4 * count).collect::<Vec<_>>());
}

#[test]
fn validates_shard_count() {
    for n in [0, 3] {
        assert!(
            catch_unwind(|| unsafe {
                IntrusiveShardedStack::with_concurrency(n, BoxAdapter::new())
            })
            .is_err()
        );
    }
}
