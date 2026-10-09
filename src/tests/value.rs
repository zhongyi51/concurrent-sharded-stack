use super::*;
use concurrent_intrusive_collections::epoch;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

#[test]
fn send_but_not_sync_payloads_can_cross_threads() {
    use std::cell::Cell;
    fn send_sync<T: Send + Sync>() {}
    send_sync::<ConcurrentShardedStack<Cell<usize>>>();
    let stack = Arc::new(ConcurrentShardedStack::with_concurrency(4));
    let producer = Arc::clone(&stack);
    std::thread::spawn(move || producer.push(Cell::new(42)).unwrap())
        .join()
        .unwrap();
    let value = stack.pop().unwrap();
    value.set(43);
    assert_eq!(value.get(), 43);
}

#[test]
fn payload_panic_still_drops_every_remaining_value() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    struct Payload {
        id: usize,
        drops: Arc<Vec<AtomicUsize>>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            assert_eq!(self.drops[self.id].fetch_add(1, Ordering::Relaxed), 0);
            assert_ne!(self.id, 4, "payload destructor panic");
        }
    }
    let drops = Arc::new((0..12).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let stack = ConcurrentShardedStack::with_concurrency(4);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let (stack, drops) = (&stack, &drops);
            scope.spawn(move || {
                for id in worker * 3..worker * 3 + 3 {
                    assert!(
                        stack
                            .push(Payload {
                                id,
                                drops: Arc::clone(drops)
                            })
                            .is_ok()
                    );
                }
            });
        }
    });
    let pinned = epoch::pin();
    assert!(catch_unwind(AssertUnwindSafe(|| drop(stack))).is_err());
    assert!(drops.iter().all(|n| n.load(Ordering::Relaxed) == 1));
    drop(pinned);
}

#[test]
fn mpmc_transfers_each_non_sync_value_exactly_once() {
    use std::cell::Cell;
    let count = if cfg!(miri) { 16 } else { 2000 };
    let stack = Arc::new(ConcurrentShardedStack::with_concurrency(8));
    let seen = Arc::new(
        (0..count * 4)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let readers = (0..4)
        .map(|_| {
            let (stack, seen) = (Arc::clone(&stack), Arc::clone(&seen));
            std::thread::spawn(move || {
                loop {
                    match stack.pop() {
                        Ok(value) => {
                            let value: Cell<usize> = value;
                            assert_eq!(seen[value.get()].fetch_add(1, Ordering::Relaxed), 0);
                        }
                        Err(PopError::Empty) => std::thread::yield_now(),
                        Err(PopError::Closed) => break,
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let stack = &stack;
            scope.spawn(move || {
                for id in worker * count..(worker + 1) * count {
                    stack.push(Cell::new(id)).unwrap();
                }
            });
        }
    });
    stack.close();
    for reader in readers {
        reader.join().unwrap();
    }
    assert!(seen.iter().all(|n| n.load(Ordering::Relaxed) == 1));
}

#[test]
fn rejected_and_popped_payloads_are_not_dropped_by_retirement() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = ConcurrentShardedStack::with_concurrency(1);
    let guard = epoch::pin();
    stack.push(DropCounter::new(Arc::clone(&drops))).unwrap();
    let popped = stack.pop().unwrap();
    stack.close();
    let rejected = stack
        .push(DropCounter::new(Arc::clone(&drops)))
        .unwrap_err()
        .into_inner();
    drop(stack);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(popped);
    drop(rejected);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(guard);
    for _ in 0..256 {
        epoch::pin().flush();
    }
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[derive(Debug)]
struct DropCounter {
    counter: Arc<AtomicUsize>,
}

impl DropCounter {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn new_requires_power_of_two() {
    let s = ConcurrentShardedStack::<usize>::with_concurrency(4);
    assert_eq!(s.shard_count(), 4);
}

#[test]
#[should_panic]
fn new_panics_if_not_power_of_two() {
    let _ = ConcurrentShardedStack::<usize>::with_concurrency(3);
}

#[test]
fn push_pop_single_thread() {
    let s = ConcurrentShardedStack::with_concurrency(4);

    s.push(1).unwrap();
    s.push(2).unwrap();
    s.push(3).unwrap();

    assert_eq!(s.pop().unwrap(), 3);
    assert_eq!(s.pop().unwrap(), 2);
    assert_eq!(s.pop().unwrap(), 1);

    assert_eq!(s.pop(), Err(PopError::Empty));
}

#[test]
fn close_works() {
    let s = ConcurrentShardedStack::with_concurrency(4);

    assert!(s.push(1).is_ok());

    assert!(s.close());
    assert!(!s.close());

    assert_eq!(s.push(2), Err(PushError::Closed(2)));

    assert_eq!(s.pop().unwrap(), 1);
    assert_eq!(s.pop(), Err(PopError::Closed));
}

#[test]
fn multi_thread_push_pop() {
    let s = Arc::new(ConcurrentShardedStack::with_concurrency(8));

    let threads = 8;
    // Miri executes far slower, so use a much smaller workload there.
    #[cfg(miri)]
    let per_thread = 200;
    #[cfg(not(miri))]
    let per_thread = 10_000;

    let mut handles = Vec::new();

    for t in 0..threads {
        let s = Arc::clone(&s);

        handles.push(std::thread::spawn(move || {
            for i in 0..per_thread {
                s.push(t * per_thread + i).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let mut handles = Vec::new();

    for _ in 0..threads {
        let s = Arc::clone(&s);

        handles.push(std::thread::spawn(move || {
            let mut count = 0usize;

            loop {
                match s.pop() {
                    Ok(_) => count += 1,
                    Err(PopError::Empty) => break,
                    Err(PopError::Closed) => break,
                }
            }

            count
        }));
    }

    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

    assert_eq!(total, threads * per_thread);
}

#[test]
fn close_after_drain_returns_closed() {
    let s = ConcurrentShardedStack::with_concurrency(4);

    for i in 0..100 {
        s.push(i).unwrap();
    }

    assert!(s.close());

    let mut count = 0;

    loop {
        match s.pop() {
            Ok(_) => count += 1,
            Err(PopError::Empty) => continue,
            Err(PopError::Closed) => break,
        }
    }

    assert_eq!(count, 100);
}

#[test]
fn popped_values_are_dropped_exactly_once() {
    let counter = Arc::new(AtomicUsize::new(0));
    let s = ConcurrentShardedStack::with_concurrency(4);

    for _ in 0..50 {
        s.push(DropCounter::new(Arc::clone(&counter))).unwrap();
    }

    // Pop everything; each popped value should be dropped exactly once when
    // it goes out of scope here.
    let mut popped = 0;
    while let Ok(value) = s.pop() {
        drop(value);
        popped += 1;
    }

    assert_eq!(popped, 50);

    // Request reclamation; flush does not guarantee that every deferred
    // callback runs here. Payloads must already have been dropped by pop.
    drop(s);
    epoch::pin().flush();

    assert_eq!(counter.load(Ordering::Relaxed), 50);
}

#[test]
fn remaining_values_are_dropped_when_stack_is_dropped() {
    let counter = Arc::new(AtomicUsize::new(0));

    {
        let s = ConcurrentShardedStack::with_concurrency(4);
        for _ in 0..30 {
            s.push(DropCounter::new(Arc::clone(&counter))).unwrap();
        }
        // Drop the stack without popping; all 30 values must be dropped once.
    }

    assert_eq!(counter.load(Ordering::Relaxed), 30);
}

#[test]
fn partially_drained_stack_drops_each_value_once() {
    let counter = Arc::new(AtomicUsize::new(0));

    {
        let s = ConcurrentShardedStack::with_concurrency(4);
        for _ in 0..40 {
            s.push(DropCounter::new(Arc::clone(&counter))).unwrap();
        }

        for _ in 0..15 {
            let _ = s.pop().unwrap();
        }
        // 15 dropped via pop, 25 remain to be dropped by the stack's Drop.
    }

    epoch::pin().flush();
    assert_eq!(counter.load(Ordering::Relaxed), 40);
}

#[test]
fn concurrent_drop_counter_no_double_free() {
    let counter = Arc::new(AtomicUsize::new(0));
    let s = Arc::new(ConcurrentShardedStack::with_concurrency(4));

    let threads = 4;
    #[cfg(miri)]
    let per_thread = 50;
    #[cfg(not(miri))]
    let per_thread = 200;

    let mut handles = Vec::new();
    for _ in 0..threads {
        let s = Arc::clone(&s);
        let counter = Arc::clone(&counter);
        handles.push(std::thread::spawn(move || {
            for _ in 0..per_thread {
                s.push(DropCounter::new(Arc::clone(&counter))).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let mut handles = Vec::new();
    for _ in 0..threads {
        let s = Arc::clone(&s);
        handles.push(std::thread::spawn(move || {
            let mut popped = 0;
            while let Ok(value) = s.pop() {
                drop(value);
                popped += 1;
            }
            popped
        }));
    }

    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total, threads * per_thread);

    drop(s);
    epoch::pin().flush();
    assert_eq!(counter.load(Ordering::Relaxed), threads * per_thread);
}

/// Wait until `cond` returns true, or panic with `label` after `timeout`.
/// Used by the loss-detection tests to turn "popper hangs forever because
/// an element is invisible" into an explicit, debuggable failure rather
/// than a CI timeout.
fn wait_until<F: Fn() -> bool>(timeout: Duration, label: &str, cond: F) {
    let start = Instant::now();
    while !cond() {
        if start.elapsed() > timeout {
            panic!("{label}: condition not met within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Pure-pusher / pure-popper split with **no** `close()`. Pushers map to a
/// disjoint set of shards from poppers, so the shards that hold the
/// pushed elements have no "local owner" that will ever pop them — every
/// element has to be reached by a popper whose local shard differs from
/// the pusher's, i.e. via the cross-shard scan in [`Self::pop`].
///
/// This is the workload that exercises the worst case of the sharded
/// scan: there is no fast path to the elements (no popper-thread ever
/// hits its own shard for these pushes), and poppers must keep walking
/// the full XOR-mask order until the elements are drained. The test
/// asserts every pushed element is observed by some popper within a
/// generous timeout; otherwise it fails loudly.
///
/// Skipped under Miri: this is a hardware-race stress test (busy-spin
/// poppers, no `close()`, watchdog measured in wall-clock seconds), and
/// Miri's deterministic cooperative scheduler neither exposes the race
/// window this is designed to catch nor finishes the workload in any
/// reasonable wall-clock budget. UB and data-race coverage for this
/// shape is handled by `no_element_loss_after_close_asymmetric`.
#[cfg_attr(miri, ignore)]
#[test]
fn no_element_loss_open_asymmetric() {
    // Miri is too slow to hit this race repeatedly; keep workload tiny.
    #[cfg(miri)]
    let (n_pushers, n_poppers, per_pusher, rounds) = (2, 2, 200, 1);
    #[cfg(not(miri))]
    let (n_pushers, n_poppers, per_pusher, rounds) = (4, 12, 50_000, 4);

    for round in 0..rounds {
        let s = Arc::new(ConcurrentShardedStack::<usize>::with_concurrency(16));
        let popped = Arc::new(AtomicUsize::new(0));
        let pushed = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let total = n_pushers * per_pusher;

        let mut handles = Vec::new();
        for t in 0..n_pushers {
            let s = Arc::clone(&s);
            let pushed = Arc::clone(&pushed);
            handles.push(std::thread::spawn(move || {
                for i in 0..per_pusher {
                    s.push(t * per_pusher + i).unwrap();
                    pushed.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for _ in 0..n_poppers {
            let s = Arc::clone(&s);
            let popped = Arc::clone(&popped);
            let stop = Arc::clone(&stop);
            handles.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match s.pop() {
                        Ok(_) => {
                            popped.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(PopError::Empty) => std::hint::spin_loop(),
                        Err(PopError::Closed) => break,
                    }
                }
            }));
        }

        // 30 s is enormous for this workload (<100 ms in practice); only
        // a real loss should ever exceed it.
        wait_until(Duration::from_secs(30), "popper drain (open)", || {
            popped.load(Ordering::Relaxed) >= total
        });
        stop.store(true, Ordering::Relaxed);

        for h in handles {
            h.join().unwrap();
        }

        let final_pushed = pushed.load(Ordering::Relaxed);
        let final_popped = popped.load(Ordering::Relaxed);
        assert_eq!(
            final_popped, final_pushed,
            "round {round}: popped {final_popped} != pushed {final_pushed}",
        );
    }
}

/// Same asymmetric split, but with the producer side `close()`ing the
/// stack once it has pushed everything. The post-`close()` drain must
/// catch every in-flight element: with no `close()`, a popper can race
/// indefinitely against pushers and keep observing `Empty` between
/// pushes, so closing turns the open-ended scan into a finite one.
#[test]
fn no_element_loss_after_close_asymmetric() {
    #[cfg(miri)]
    let (n_pushers, n_poppers, per_pusher, rounds) = (2, 2, 200, 1);
    #[cfg(not(miri))]
    let (n_pushers, n_poppers, per_pusher, rounds) = (4, 12, 50_000, 4);

    for round in 0..rounds {
        let s = Arc::new(ConcurrentShardedStack::<usize>::with_concurrency(16));
        let popped = Arc::new(AtomicUsize::new(0));
        let total = n_pushers * per_pusher;

        let mut handles = Vec::new();
        for t in 0..n_pushers {
            let s = Arc::clone(&s);
            handles.push(std::thread::spawn(move || {
                for i in 0..per_pusher {
                    s.push(t * per_pusher + i).unwrap();
                }
            }));
        }

        for _ in 0..n_poppers {
            let s = Arc::clone(&s);
            let popped = Arc::clone(&popped);
            handles.push(std::thread::spawn(move || {
                loop {
                    match s.pop() {
                        Ok(_) => {
                            popped.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(PopError::Empty) => std::hint::spin_loop(),
                        Err(PopError::Closed) => break,
                    }
                }
            }));
        }

        // Wait for pushers to finish, then close. Poppers continue
        // draining until they observe Closed.
        let pusher_handles: Vec<_> = handles.drain(..n_pushers).collect();
        for h in pusher_handles {
            h.join().unwrap();
        }
        s.close();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            popped.load(Ordering::Relaxed),
            total,
            "round {round}: drain after close lost elements",
        );
    }
}

/// Lopsided shard count: many shards, few threads. Most shards are owned
/// by nobody, so a popper only reaches the elements pushed there via the
/// cross-shard scan in [`Self::pop`] (its own local shard is empty for
/// those pushers). Catches loss via the cross-shard scan path.
#[test]
fn no_element_loss_few_threads_many_shards() {
    #[cfg(miri)]
    let (n_pushers, n_poppers, per_pusher, shards) = (2, 1, 100, 8);
    #[cfg(not(miri))]
    let (n_pushers, n_poppers, per_pusher, shards) = (2, 1, 100_000, 32);

    let s = Arc::new(ConcurrentShardedStack::<usize>::with_concurrency(shards));
    let popped = Arc::new(AtomicUsize::new(0));
    let total = n_pushers * per_pusher;

    let mut handles = Vec::new();
    for _ in 0..n_pushers {
        let s = Arc::clone(&s);
        handles.push(std::thread::spawn(move || {
            for i in 0..per_pusher {
                s.push(i).unwrap();
            }
        }));
    }

    for _ in 0..n_poppers {
        let s = Arc::clone(&s);
        let popped = Arc::clone(&popped);
        handles.push(std::thread::spawn(move || {
            let start = Instant::now();
            while popped.load(Ordering::Relaxed) < total {
                match s.pop() {
                    Ok(_) => {
                        popped.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(PopError::Empty) => std::hint::spin_loop(),
                    Err(PopError::Closed) => break,
                }
                if start.elapsed() > Duration::from_secs(30) {
                    panic!(
                        "popper stuck: popped {} of {}",
                        popped.load(Ordering::Relaxed),
                        total,
                    );
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(popped.load(Ordering::Relaxed), total);
}
