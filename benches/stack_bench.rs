//! End-to-end container workloads, not isolated push/pop latency measurements.
//!
//! The sharded stack does not provide global LIFO. The `lockfree` and mutex
//! stacks do; Crossbeam's queues provide FIFO. These different contracts are
//! interchangeable only for workloads that do not require a particular order.
//!
//! `object_pool` repeatedly acquires and returns values from a fixed pool of
//! four values per worker. `mpmc` transfers unique values between equal numbers
//! of producers and consumers; `4p_4c` means eight worker threads in total.
//! Shard counts are the worker count (pool) or producer count (MPMC), rounded up
//! to a power of two. ArrayQueue holds the entire initial pool in `object_pool`
//! and has a fixed capacity of 1,024 in `mpmc`, applying backpressure when full.
//!
//! Each iteration includes container construction, prefill, thread creation,
//! a shared start barrier, work, joining, validation, and container destruction.
//! Failed operations all yield and retry; long stalls fail with a diagnostic.
//! Implementations use static dispatch. Value checks and retry bookkeeping are
//! part of the measured workload; there is no shared per-operation counter.

use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use concurrent_sharded_stack::ConcurrentShardedStack;
use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use crossbeam_queue::{ArrayQueue, SegQueue};
use lockfree::stack::Stack as LockFreeStack;

/// Minimal statically dispatched API for unordered transfer workloads.
trait Container: Send + Sync + 'static {
    fn new(workers: usize, capacity: usize) -> Self;
    /// Return the original value when bounded storage is full.
    fn push_one(&self, value: usize) -> Result<(), usize>;
    fn pop_one(&self) -> Option<usize>;
}

impl Container for ConcurrentShardedStack<usize> {
    fn new(workers: usize, _: usize) -> Self {
        Self::with_concurrency(workers.next_power_of_two())
    }
    fn push_one(&self, value: usize) -> Result<(), usize> {
        // Nothing in these workloads closes the stack.
        self.push(value).unwrap();
        Ok(())
    }
    fn pop_one(&self) -> Option<usize> {
        self.pop().ok()
    }
}

impl Container for LockFreeStack<usize> {
    fn new(_: usize, _: usize) -> Self {
        Self::new()
    }
    fn push_one(&self, value: usize) -> Result<(), usize> {
        self.push(value);
        Ok(())
    }
    fn pop_one(&self) -> Option<usize> {
        self.pop()
    }
}

impl Container for Mutex<Vec<usize>> {
    fn new(_: usize, capacity: usize) -> Self {
        Self::new(Vec::with_capacity(capacity))
    }
    fn push_one(&self, value: usize) -> Result<(), usize> {
        self.lock().unwrap().push(value);
        Ok(())
    }
    fn pop_one(&self) -> Option<usize> {
        self.lock().unwrap().pop()
    }
}

impl Container for SegQueue<usize> {
    fn new(_: usize, _: usize) -> Self {
        Self::new()
    }
    fn push_one(&self, value: usize) -> Result<(), usize> {
        self.push(value);
        Ok(())
    }
    fn pop_one(&self) -> Option<usize> {
        self.pop()
    }
}

impl Container for ArrayQueue<usize> {
    fn new(_: usize, capacity: usize) -> Self {
        Self::new(capacity)
    }
    fn push_one(&self, value: usize) -> Result<(), usize> {
        self.push(value)
    }
    fn pop_one(&self) -> Option<usize> {
        self.pop()
    }
}

/// Check time only after sustained failure; successful operations need no clock.
#[derive(Default)]
struct Retry {
    attempts: usize,
    since: Option<Instant>,
}

impl Retry {
    fn yield_now(&mut self) {
        thread::yield_now();
        self.attempts += 1;
        if self.attempts % 1_024 == 0 {
            let now = Instant::now();
            let since = self.since.get_or_insert(now);
            assert!(
                now.duration_since(*since) < Duration::from_secs(30),
                "container operation stalled for 30 seconds; aborting benchmark"
            );
        }
    }
}

fn push_wait<C: Container>(container: &C, mut value: usize) {
    let mut retry = Retry::default();
    while let Err(returned) = container.push_one(value) {
        // In particular, an ArrayQueue retry must not discard a full-queue value.
        value = returned;
        retry.yield_now();
    }
}

fn pop_wait<C: Container>(container: &C) -> usize {
    let mut retry = Retry::default();
    loop {
        if let Some(value) = container.pop_one() {
            return value;
        }
        retry.yield_now();
    }
}

/// MPMC uses this many threads in each role: 2, 8, or 16 threads in total.
const THREAD_COUNTS: [usize; 3] = [1, 4, 8];
const OPS_PER_THREAD: usize = 20_000;
const POOL_PER_THREAD: usize = 4;
const MPMC_CAPACITY: usize = 1_024;

fn bench_object_pool_for<C: Container>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str) {
    for threads in THREAD_COUNTS {
        group.throughput(Throughput::Elements((threads * OPS_PER_THREAD) as u64));
        group.bench_with_input(BenchmarkId::new(name, threads), &threads, |b, &threads| {
            b.iter(|| {
                let pool_size = threads * POOL_PER_THREAD;
                let pool = Arc::new(C::new(threads, pool_size));
                for value in 0..pool_size {
                    push_wait(&*pool, value);
                }

                let start = Arc::new(Barrier::new(threads + 1));
                let mut handles = Vec::with_capacity(threads);
                for _ in 0..threads {
                    let pool = Arc::clone(&pool);
                    let start = Arc::clone(&start);
                    handles.push(thread::spawn(move || {
                        start.wait();
                        for _ in 0..OPS_PER_THREAD {
                            // A sharded scan may miss an existing object. Retry
                            // without increasing the fixed pool size.
                            let value = pop_wait(&*pool);
                            push_wait(&*pool, value);
                        }
                        OPS_PER_THREAD
                    }));
                }

                start.wait();
                let serviced: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
                assert_eq!(serviced, threads * OPS_PER_THREAD);
                // With all workers joined, an empty scan is a stable result.
                let mut remaining = Vec::with_capacity(pool_size);
                while let Some(value) = pool.pop_one() {
                    remaining.push(value);
                }
                remaining.sort_unstable();
                assert_eq!(remaining, (0..pool_size).collect::<Vec<_>>());
                serviced
            });
        });
    }
}

fn bench_object_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_pool");
    bench_object_pool_for::<ConcurrentShardedStack<usize>>(&mut group, "sharded");
    bench_object_pool_for::<LockFreeStack<usize>>(&mut group, "lockfree_lifo");
    bench_object_pool_for::<Mutex<Vec<usize>>>(&mut group, "mutex_vec_lifo");
    bench_object_pool_for::<SegQueue<usize>>(&mut group, "segqueue_fifo");
    bench_object_pool_for::<ArrayQueue<usize>>(&mut group, "arrayqueue_fifo");
    group.finish();
}

fn bench_mpmc_for<C: Container>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str) {
    for threads in THREAD_COUNTS {
        let produced = threads * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(produced as u64));
        let id = BenchmarkId::new(name, format!("{threads}p_{threads}c"));
        group.bench_with_input(id, &threads, |b, &threads| {
            b.iter(|| {
                let container = Arc::new(C::new(threads, MPMC_CAPACITY));
                let start = Arc::new(Barrier::new(2 * threads + 1));

                let mut producers = Vec::with_capacity(threads);
                for producer in 0..threads {
                    let container = Arc::clone(&container);
                    let start = Arc::clone(&start);
                    producers.push(thread::spawn(move || {
                        start.wait();
                        for i in 0..OPS_PER_THREAD {
                            push_wait(&*container, producer * OPS_PER_THREAD + i);
                        }
                    }));
                }

                let mut consumers = Vec::with_capacity(threads);
                for _ in 0..threads {
                    let container = Arc::clone(&container);
                    let start = Arc::clone(&start);
                    consumers.push(thread::spawn(move || {
                        start.wait();
                        let mut count = 0;
                        let mut sum = 0u64;
                        let mut xor = 0;
                        for _ in 0..OPS_PER_THREAD {
                            let value = pop_wait(&*container);
                            assert!(value < produced);
                            count += 1;
                            sum += value as u64;
                            xor ^= value;
                        }
                        (count, sum, xor)
                    }));
                }

                start.wait();
                for producer in producers {
                    producer.join().unwrap();
                }
                let (count, sum, xor) = consumers
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .fold((0, 0u64, 0), |(count, sum, xor), (n, s, x)| {
                        (count + n, sum + s, xor ^ x)
                    });
                assert_eq!(count, produced);
                assert_eq!(sum, produced as u64 * (produced as u64 - 1) / 2);
                assert_eq!(xor, (0..produced).fold(0, |acc, value| acc ^ value));
                assert!(container.pop_one().is_none());
                count
            });
        });
    }
}

fn bench_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc");
    bench_mpmc_for::<ConcurrentShardedStack<usize>>(&mut group, "sharded");
    bench_mpmc_for::<LockFreeStack<usize>>(&mut group, "lockfree_lifo");
    bench_mpmc_for::<Mutex<Vec<usize>>>(&mut group, "mutex_vec_lifo");
    bench_mpmc_for::<SegQueue<usize>>(&mut group, "segqueue_fifo");
    bench_mpmc_for::<ArrayQueue<usize>>(&mut group, "arrayqueue_fifo");
    group.finish();
}

criterion_group!(benches, bench_object_pool, bench_mpmc);
criterion_main!(benches);
