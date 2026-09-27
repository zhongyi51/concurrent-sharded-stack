//! Comparative benchmarks for `ConcurrentShardedStack`.
//!
//! Every scenario runs the *same* workload against two lock-free
//! implementations:
//!
//! * [`ConcurrentShardedStack`] — this crate,
//! * [`lockfree::stack::Stack`] — the well-known lock-free stack crate on
//!   crates.io. Unlike the sharded implementation, it provides global LIFO.
//!
//! The two scenarios model how a concurrent LIFO stack is actually used under
//! load:
//!
//! * `object_pool` — a fixed pool of objects; every worker repeatedly *acquires*
//!   (pop) and *releases* (push) one. This is the connection/buffer pool used
//!   by web servers and thread pools.
//! * `mpmc` — dedicated producer and consumer threads with no pre-fill.
//!   Benchmark IDs give both counts, e.g. `4p_4c` means eight workers.
//!
//! Workers share a start barrier. Thread creation and teardown remain inside
//! the timed iteration, so these are end-to-end workload measurements.

use std::sync::{Arc, Barrier};
use std::thread;

use concurrent_sharded_stack::ConcurrentShardedStack;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lockfree::stack::Stack as LockFreeStack;

/// Minimal API shared by every implementation under test.
trait Stackish: Send + Sync {
    fn push_one(&self, value: usize);
    fn pop_one(&self) -> Option<usize>;
}

impl Stackish for ConcurrentShardedStack<usize> {
    fn push_one(&self, value: usize) {
        self.push(value).unwrap();
    }
    fn pop_one(&self) -> Option<usize> {
        self.pop().ok()
    }
}

impl Stackish for LockFreeStack<usize> {
    fn push_one(&self, value: usize) {
        self.push(value);
    }
    fn pop_one(&self) -> Option<usize> {
        self.pop()
    }
}

/// Round the workload's shard hint to the required power of two.
fn shard_hint(threads: usize) -> usize {
    threads.next_power_of_two()
}

/// The two implementations, as factories taking the thread count (the sharded
/// stack sizes itself from it).
type Factory = fn(usize) -> Arc<dyn Stackish>;

fn implementations() -> [(&'static str, Factory); 2] {
    [
        ("sharded", |threads| {
            Arc::new(ConcurrentShardedStack::with_concurrency(shard_hint(
                threads,
            )))
        }),
        ("lockfree", |_| Arc::new(LockFreeStack::new())),
    ]
}

/// Object-pool worker counts; MPMC uses this many workers in each role.
const THREAD_COUNTS: [usize; 2] = [4, 32];
const OPS_PER_THREAD: usize = 20_000;

/// Object-pool workload: pre-fill a pool sized to the thread count, then have
/// every worker repeatedly acquire (pop) and release (push) an object. Models a
/// connection pool / buffer pool shared across many request handlers.
fn bench_object_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_pool");
    // Each worker holds at most one object at a time; size the pool so there is
    // contention but pops usually succeed.
    let pool_per_thread = 4;

    for &threads in &THREAD_COUNTS {
        group.throughput(Throughput::Elements((threads * OPS_PER_THREAD) as u64));

        for (name, factory) in implementations() {
            let id = BenchmarkId::new(name, threads);
            group.bench_with_input(id, &threads, |b, &threads| {
                b.iter(|| {
                    let pool = factory(threads);
                    for i in 0..threads * pool_per_thread {
                        pool.push_one(i);
                    }

                    let start = Arc::new(Barrier::new(threads + 1));
                    let mut handles = Vec::new();
                    for _ in 0..threads {
                        let pool = Arc::clone(&pool);
                        let start = Arc::clone(&start);
                        handles.push(thread::spawn(move || {
                            start.wait();
                            let mut serviced = 0usize;
                            for _ in 0..OPS_PER_THREAD {
                                // A sharded scan may miss an existing object.
                                // Retry without increasing the fixed pool size.
                                let obj = loop {
                                    if let Some(obj) = pool.pop_one() {
                                        break obj;
                                    }
                                    thread::yield_now();
                                };
                                pool.push_one(obj);
                                serviced += 1;
                            }
                            serviced
                        }));
                    }

                    start.wait();
                    let mut total = 0usize;
                    for h in handles {
                        total += h.join().unwrap();
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

/// Dedicated producers and consumers each process a fixed quota, avoiding
/// an additional shared counter on every pop. Ordering is not measured.
fn bench_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc");

    for &threads in &THREAD_COUNTS {
        let produced = threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(produced as u64));

        for (name, factory) in implementations() {
            let id = BenchmarkId::new(name, format!("{threads}p_{threads}c"));
            group.bench_with_input(id, &threads, |b, &threads| {
                b.iter(|| {
                    let stack = factory(threads);
                    let start = Arc::new(Barrier::new(2 * threads + 1));

                    let mut producers = Vec::new();
                    for _ in 0..threads {
                        let stack = Arc::clone(&stack);
                        let start = Arc::clone(&start);
                        producers.push(thread::spawn(move || {
                            start.wait();
                            for i in 0..OPS_PER_THREAD {
                                stack.push_one(i);
                            }
                        }));
                    }

                    let mut consumers = Vec::new();
                    for _ in 0..threads {
                        let stack = Arc::clone(&stack);
                        let start = Arc::clone(&start);
                        consumers.push(thread::spawn(move || {
                            start.wait();
                            for _ in 0..OPS_PER_THREAD {
                                while stack.pop_one().is_none() {
                                    thread::yield_now();
                                }
                            }
                            OPS_PER_THREAD
                        }));
                    }

                    start.wait();
                    for h in producers {
                        h.join().unwrap();
                    }
                    let total: usize = consumers.into_iter().map(|h| h.join().unwrap()).sum();
                    assert_eq!(total, produced);
                    total
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_object_pool, bench_mpmc);
criterion_main!(benches);
