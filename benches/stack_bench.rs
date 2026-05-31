//! Comparative benchmarks for `ConcurrentShardedStack`.
//!
//! Running a data structure in isolation tells you little, so every scenario
//! here runs the *same* workload against two lock-free implementations:
//!
//! * [`ConcurrentShardedStack`] — this crate,
//! * [`lockfree::stack::Stack`] — the well-known lock-free stack crate on
//!   crates.io, the natural apples-to-apples competitor.
//!
//! The scenarios model how a concurrent stack is actually used under load:
//!
//! * `object_pool` — a fixed pool of objects; every worker repeatedly *acquires*
//!   (pop) and *releases* (push) one. This is the connection/buffer pool used by
//!   web servers and thread pools.
//! * `mpmc` — dedicated producer and consumer threads (fan-out work queue).
//! * `asymmetric_rw` — a skewed read/write mix (write-heavy and read-heavy),
//!   since real workloads are rarely a clean 1:1 of pushes and pops.
//!
//! All run with thread counts that go well past the core count (up to 256), to
//! reflect heavily oversubscribed servers rather than a tidy 4-thread demo.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// Shards are bounded by `usize::BITS`; cap the hint so high thread counts do
/// not blow past the limit.
fn shard_hint(threads: usize) -> usize {
    threads.next_power_of_two().min(usize::BITS as usize)
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

/// Oversubscribed thread counts: from one-per-core up to 256 (web-server /
/// thread-pool territory, far beyond physical parallelism).
const THREAD_COUNTS: [usize; 4] = [8, 32, 64, 256];
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

                    let mut handles = Vec::new();
                    for _ in 0..threads {
                        let pool = Arc::clone(&pool);
                        handles.push(thread::spawn(move || {
                            let mut serviced = 0usize;
                            for _ in 0..OPS_PER_THREAD {
                                // Acquire; if the pool is momentarily empty, just
                                // release a fresh object (pool grows slightly).
                                let obj = pool.pop_one().unwrap_or(0);
                                pool.push_one(obj);
                                serviced += 1;
                            }
                            serviced
                        }));
                    }

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

/// Dedicated producers fan items out; dedicated consumers drain them. Consumers
/// stop once the global popped count reaches everything the producers pushed.
fn bench_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc");

    for &threads in &THREAD_COUNTS {
        let produced = threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(produced as u64));

        for (name, factory) in implementations() {
            let id = BenchmarkId::new(name, threads);
            group.bench_with_input(id, &threads, |b, &threads| {
                b.iter(|| {
                    let stack = factory(threads);
                    let popped_total = Arc::new(AtomicUsize::new(0));

                    let mut producers = Vec::new();
                    for _ in 0..threads {
                        let stack = Arc::clone(&stack);
                        producers.push(thread::spawn(move || {
                            for i in 0..OPS_PER_THREAD {
                                stack.push_one(i);
                            }
                        }));
                    }

                    let mut consumers = Vec::new();
                    for _ in 0..threads {
                        let stack = Arc::clone(&stack);
                        let popped_total = Arc::clone(&popped_total);
                        consumers.push(thread::spawn(move || {
                            while popped_total.load(Ordering::Relaxed) < produced {
                                if stack.pop_one().is_some() {
                                    popped_total.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    thread::yield_now();
                                }
                            }
                        }));
                    }

                    for h in producers {
                        h.join().unwrap();
                    }
                    for h in consumers {
                        h.join().unwrap();
                    }
                    popped_total.load(Ordering::Relaxed)
                });
            });
        }
    }
    group.finish();
}

/// Skewed read/write workload. Each thread runs a fixed op mix where pushes and
/// pops are *not* balanced: `write_heavy` does 4 pushes per pop, `read_heavy`
/// does 4 pops per push. Pops that find the stack empty are counted as misses,
/// which is itself representative of a read-heavy consumer.
fn bench_asymmetric_rw(c: &mut Criterion) {
    let mut group = c.benchmark_group("asymmetric_rw");

    // (label, `true` => the 1-in-5 op is a pop; `false` => it is a push).
    let regimes: [(&str, bool); 2] = [("write_heavy", true), ("read_heavy", false)];

    for (regime, minority_is_pop) in regimes {
        for &threads in &THREAD_COUNTS {
            group.throughput(Throughput::Elements((threads * OPS_PER_THREAD) as u64));

            for (name, factory) in implementations() {
                let id = BenchmarkId::new(format!("{regime}/{name}"), threads);
                group.bench_with_input(id, &threads, |b, &threads| {
                    b.iter(|| {
                        let stack = factory(threads);
                        // Seed so read-heavy runs have something to take early.
                        for i in 0..threads * 8 {
                            stack.push_one(i);
                        }

                        let mut handles = Vec::new();
                        for _ in 0..threads {
                            let stack = Arc::clone(&stack);
                            handles.push(thread::spawn(move || {
                                let mut hits = 0usize;
                                for i in 0..OPS_PER_THREAD {
                                    // 1 in every 5 ops is the "minority" op.
                                    let minority = i % 5 == 0;
                                    let do_pop = minority == minority_is_pop;
                                    if do_pop {
                                        if stack.pop_one().is_some() {
                                            hits += 1;
                                        }
                                    } else {
                                        stack.push_one(i);
                                    }
                                }
                                hits
                            }));
                        }

                        let mut total = 0usize;
                        for h in handles {
                            total += h.join().unwrap();
                        }
                        total
                    });
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_object_pool, bench_mpmc, bench_asymmetric_rw);
criterion_main!(benches);
