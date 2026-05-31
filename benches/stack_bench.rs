//! Benchmarks modelling realistic usage of `ConcurrentShardedStack`.
//!
//! The scenarios here intentionally avoid micro-benchmarking a single atomic
//! operation. Instead they measure throughput under contention patterns that
//! actually occur in production:
//!
//! * single-threaded LIFO buffering (baseline),
//! * multi-producer / multi-consumer pipelines,
//! * a work-stealing style pool where every thread both pushes and pops,
//! * a comparison against the common `Mutex<Vec<T>>` baseline.

use std::sync::{Arc, Mutex};
use std::thread;

use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Drain helper used by the consumer threads: spin until the stack is empty
/// (and closed), counting how many items were observed.
fn drain_until_closed<T>(stack: &ConcurrentShardedStack<T>) -> usize {
    let mut count = 0;
    loop {
        match stack.pop() {
            Ok(_) => count += 1,
            Err(PopError::Empty) => thread::yield_now(),
            Err(PopError::Closed) => break,
        }
    }
    count
}

/// Baseline: a single thread pushing then popping everything (LIFO buffer).
fn bench_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_thread_push_pop");
    for &ops in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(ops as u64));
        group.bench_with_input(BenchmarkId::from_parameter(ops), &ops, |b, &ops| {
            b.iter(|| {
                let stack = ConcurrentShardedStack::with_concurrency(4);
                for i in 0..ops {
                    stack.push(i).unwrap();
                }
                let mut sum = 0usize;
                while let Ok(v) = stack.pop() {
                    sum = sum.wrapping_add(v);
                }
                sum
            });
        });
    }
    group.finish();
}

/// Multi-producer / multi-consumer pipeline: half the threads push, the other
/// half drain. Models a fan-out work queue.
fn bench_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc_pipeline");
    let per_thread = 50_000usize;

    for &threads in &[2usize, 4, 8] {
        let producers = threads;
        let consumers = threads;
        let total = (producers * per_thread) as u64;
        group.throughput(Throughput::Elements(total));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{producers}p{consumers}c")),
            &threads,
            |b, _| {
                b.iter(|| {
                    let stack = Arc::new(ConcurrentShardedStack::with_concurrency(
                        threads.next_power_of_two(),
                    ));

                    let mut handles = Vec::new();
                    for _ in 0..producers {
                        let stack = Arc::clone(&stack);
                        handles.push(thread::spawn(move || {
                            for i in 0..per_thread {
                                stack.push(i).unwrap();
                            }
                        }));
                    }

                    let mut consumer_handles = Vec::new();
                    for _ in 0..consumers {
                        let stack = Arc::clone(&stack);
                        consumer_handles.push(thread::spawn(move || drain_until_closed(&stack)));
                    }

                    for h in handles {
                        h.join().unwrap();
                    }
                    stack.close();

                    let mut drained = 0usize;
                    for h in consumer_handles {
                        drained += h.join().unwrap();
                    }
                    drained
                });
            },
        );
    }
    group.finish();
}

/// Work-stealing style pool: every thread alternates pushing and popping,
/// mimicking a task scheduler where workers both spawn and execute tasks.
fn bench_work_stealing(c: &mut Criterion) {
    let mut group = c.benchmark_group("work_stealing_mixed");
    let ops_per_thread = 50_000usize;

    for &threads in &[2usize, 4, 8] {
        group.throughput(Throughput::Elements((threads * ops_per_thread) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(threads), &threads, |b, _| {
            b.iter(|| {
                let stack = Arc::new(ConcurrentShardedStack::with_concurrency(
                    threads.next_power_of_two(),
                ));

                // Seed each shard so pops have something to take early on.
                for i in 0..threads {
                    stack.push(i).unwrap();
                }

                let mut handles = Vec::new();
                for t in 0..threads {
                    let stack = Arc::clone(&stack);
                    handles.push(thread::spawn(move || {
                        let mut popped = 0usize;
                        for i in 0..ops_per_thread {
                            if (t + i) & 1 == 0 {
                                stack.push(i).unwrap();
                            } else if stack.pop().is_ok() {
                                popped += 1;
                            }
                        }
                        popped
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
    group.finish();
}

/// Comparison baseline: the same MPMC workload backed by a `Mutex<Vec<T>>`.
fn bench_mutex_vec_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("mutex_vec_baseline_mpmc");
    let per_thread = 50_000usize;

    for &threads in &[2usize, 4, 8] {
        let total = (threads * per_thread) as u64;
        group.throughput(Throughput::Elements(total));
        group.bench_with_input(BenchmarkId::from_parameter(threads), &threads, |b, _| {
            b.iter(|| {
                let stack = Arc::new(Mutex::new(Vec::<usize>::new()));
                let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

                let mut handles = Vec::new();
                for _ in 0..threads {
                    let stack = Arc::clone(&stack);
                    handles.push(thread::spawn(move || {
                        for i in 0..per_thread {
                            stack.lock().unwrap().push(i);
                        }
                    }));
                }

                let mut consumers = Vec::new();
                for _ in 0..threads {
                    let stack = Arc::clone(&stack);
                    let done = Arc::clone(&done);
                    consumers.push(thread::spawn(move || {
                        let mut count = 0usize;
                        loop {
                            let popped = stack.lock().unwrap().pop();
                            match popped {
                                Some(_) => count += 1,
                                None => {
                                    if done.load(std::sync::atomic::Ordering::Acquire) {
                                        break;
                                    }
                                    thread::yield_now();
                                }
                            }
                        }
                        count
                    }));
                }

                for h in handles {
                    h.join().unwrap();
                }
                done.store(true, std::sync::atomic::Ordering::Release);

                let mut drained = 0usize;
                for h in consumers {
                    drained += h.join().unwrap();
                }
                drained
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_single_thread,
    bench_mpmc,
    bench_work_stealing,
    bench_mutex_vec_baseline
);
criterion_main!(benches);
