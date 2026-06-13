//! Comparative benchmarks for `ConcurrentShardedStack`.
//!
//! Every scenario runs the *same* workload against two lock-free
//! implementations:
//!
//! * [`ConcurrentShardedStack`] — this crate,
//! * [`lockfree::stack::Stack`] — the well-known lock-free stack crate on
//!   crates.io, the natural apples-to-apples competitor.
//!
//! The two scenarios model how a concurrent LIFO stack is actually used under
//! load:
//!
//! * `object_pool` — a fixed pool of objects; every worker repeatedly *acquires*
//!   (pop) and *releases* (push) one. This is the connection/buffer pool used
//!   by web servers and thread pools.
//! * `mpmc` — a LIFO work queue with dedicated producer and consumer threads
//!   (fan-out). The LIFO ordering shows up as "newer items are popped first",
//!   so the bench exercises the sharded stack under pure push/pop traffic
//!   with no pre-fill.
//!
//! Both run with thread counts that go well past the core count (up to 256),
//! to reflect heavily oversubscribed servers rather than a tidy 4-thread demo.

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

/// LIFO queue workload: dedicated producers fan items in, dedicated consumers
/// drain them. Consumers stop once the global popped count reaches everything
/// the producers pushed. The stack ordering means the most recently pushed
/// items are popped first, so this is a stress test of the core push/pop path
/// under concurrent traffic.
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

criterion_group!(benches, bench_object_pool, bench_mpmc);
criterion_main!(benches);
