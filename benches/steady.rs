//! Persistent workers; setup/validation excluded; one warmup then samples.
//! Separate allocation-counting runs from throughput measurements.
#[path = "../examples/support/local_cache.rs"]
mod local_cache;
use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
use local_cache::LocalCache;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::{
        Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

struct CountAlloc;
static COUNT: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountAlloc = CountAlloc;

struct Config {
    mode: String,
    workers: usize,
    shards: usize,
    ops: usize,
    samples: usize,
    bytes: usize,
    producers: usize,
    count: bool,
}
#[derive(Debug)]
struct Value {
    id: usize,
    bytes: Vec<u8>,
}
struct WorkerSample {
    latency: Vec<u64>,
    ids: Vec<usize>,
}

fn pop_wait(pool: &ConcurrentShardedStack<Value>) -> Value {
    take_wait(|| pool.pop())
}

fn take_wait(mut take: impl FnMut() -> Result<Value, PopError>) -> Value {
    let mut failures = 0;
    let mut started = None;
    loop {
        match take() {
            Ok(value) => return value,
            Err(PopError::Empty) => {}
            Err(PopError::Closed) => panic!("benchmark pool closed unexpectedly"),
        }
        std::thread::yield_now();
        failures += 1;
        if failures % 65_536 == 0 {
            assert!(
                started.get_or_insert_with(Instant::now).elapsed() < Duration::from_secs(30),
                "stalled consumer"
            );
        }
    }
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let arg = |name: &str, default: &str| {
        args.windows(2)
            .find(|a| a[0] == name)
            .map(|a| a[1].clone())
            .unwrap_or_else(|| default.into())
    };
    let config = Config {
        mode: arg("--mode", "shared"),
        workers: arg("--workers", "4").parse().unwrap(),
        shards: arg("--shards", "4").parse().unwrap(),
        ops: arg("--ops", "200000").parse().unwrap(),
        samples: arg("--samples", "9").parse().unwrap(),
        bytes: arg("--bytes", "256").parse().unwrap(),
        producers: arg("--producers", "1").parse().unwrap(),
        count: args.iter().any(|a| a == "--count"),
    };
    assert!(config.workers > 0 && config.samples > 0 && config.ops > 0);
    assert!(["shared", "cached", "empty", "mpmc"].contains(&config.mode.as_str()));
    if config.mode == "mpmc" {
        assert!(config.producers > 0 && config.producers < config.workers);
    }
    let pool = ConcurrentShardedStack::with_concurrency(config.shards);
    let pool_size = if config.mode == "shared" || config.mode == "cached" {
        config.workers * 8
    } else {
        0
    };
    for id in 0..pool_size {
        pool.push(Value {
            id,
            bytes: vec![0; config.bytes],
        })
        .unwrap();
    }
    let ready = Barrier::new(config.workers + 1);
    let start = Barrier::new(config.workers + 1);
    let done = Barrier::new(config.workers + 1);
    let released = Barrier::new(config.workers + 1);
    let mut timings = Vec::with_capacity(config.samples + 1);
    let mut allocation_counts = Vec::with_capacity(config.samples + 1);
    let results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..config.workers {
            let (config, pool, ready, start, done, released) =
                (&config, &pool, &ready, &start, &done, &released);
            handles.push(scope.spawn(move || {
                // Register and warm this worker's collector before timing.
                let _ = concurrent_intrusive_collections::epoch::pin();
                let mut samples = Vec::with_capacity(config.samples + 1);
                let mut cache = LocalCache::new(pool, 4);
                for _ in 0..=config.samples {
                    let consumer = config.mode == "mpmc" && worker >= config.producers;
                    let iterations = if consumer {
                        let total = config.ops * config.producers;
                        let consumers = config.workers - config.producers;
                        total / consumers
                            + usize::from(worker - config.producers < total % consumers)
                    } else {
                        config.ops
                    };
                    let mut latency = Vec::with_capacity(iterations / 997 + 1);
                    let mut ids = Vec::with_capacity(if consumer { iterations } else { 0 });
                    // Producer payload allocations are setup, node allocations
                    // remain part of the transfer. No hidden per-op counter.
                    let mut pending: Vec<_> = if config.mode == "mpmc" && !consumer {
                        (0..iterations)
                            .map(|i| Value {
                                id: worker * config.ops + i,
                                bytes: vec![0; config.bytes],
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    ready.wait();
                    start.wait();
                    for i in 0..iterations {
                        // Coprime to the 64-operation cache flush period.
                        let began = (i % 997 == 0).then(Instant::now);
                        match config.mode.as_str() {
                            "empty" => {
                                assert!(pool.pop().is_err());
                            }
                            "mpmc" if !consumer => {
                                pool.push(pending.pop().unwrap()).unwrap();
                            }
                            "mpmc" => {
                                ids.push(pop_wait(pool).id);
                            }
                            mode => {
                                let mut value = if mode == "cached" {
                                    take_wait(|| cache.take())
                                } else {
                                    pop_wait(pool)
                                };
                                if !value.bytes.is_empty() {
                                    value.bytes[0] = value.bytes[0].wrapping_add(1);
                                }
                                black_box(&value.bytes);
                                if mode == "cached" {
                                    cache.put(value).unwrap();
                                    if i % 64 == 63 {
                                        cache.flush().unwrap();
                                    }
                                } else {
                                    pool.push(value).unwrap();
                                }
                            }
                        }
                        if let Some(began) = began {
                            latency.push(began.elapsed().as_nanos() as u64);
                        }
                    }
                    cache.flush().unwrap();
                    done.wait();
                    released.wait();
                    samples.push(WorkerSample { latency, ids });
                }
                samples
            }));
        }
        for _ in 0..=config.samples {
            ready.wait();
            ALLOCS.store(0, Ordering::Relaxed);
            COUNT.store(config.count, Ordering::Relaxed);
            let began = Instant::now();
            start.wait();
            done.wait();
            timings.push(began.elapsed());
            COUNT.store(false, Ordering::Relaxed);
            allocation_counts.push(ALLOCS.load(Ordering::Relaxed));
            released.wait();
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    if pool_size > 0 {
        let mut ids = Vec::new();
        while let Ok(value) = pool.pop() {
            ids.push(value.id);
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..pool_size).collect::<Vec<_>>());
    }
    println!("mode,workers,shards,bytes,producers,counting,sample,mops,p50_ns,p99_ns,allocations");
    for sample in 1..=config.samples {
        let mut latency: Vec<_> = results
            .iter()
            .flat_map(|w| w[sample].latency.iter().copied())
            .collect();
        latency.sort_unstable();
        let ops = if config.mode == "mpmc" {
            config.producers * config.ops
        } else {
            config.workers * config.ops
        };
        if config.mode == "mpmc" {
            let mut ids: Vec<_> = results
                .iter()
                .flat_map(|w| w[sample].ids.iter().copied())
                .collect();
            ids.sort_unstable();
            assert_eq!(ids, (0..ops).collect::<Vec<_>>());
        }
        println!(
            "{},{},{},{},{},{},{},{:.4},{},{},{}",
            config.mode,
            config.workers,
            config.shards,
            config.bytes,
            config.producers,
            config.count,
            sample,
            ops as f64 / timings[sample].as_secs_f64() / 1e6,
            latency[latency.len() / 2],
            latency[(latency.len() - 1) * 99 / 100],
            allocation_counts[sample]
        );
    }
}
