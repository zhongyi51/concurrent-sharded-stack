//! Bounded worker caches reduce shared-stack node allocations.
//! Run with `cargo run --example cached_buffer_recycling`.
#[path = "support/local_cache.rs"]
mod local_cache;
use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
use local_cache::LocalCache;
use std::sync::Barrier;

fn main() {
    const WORKERS: usize = 4;
    const BUFFERS: usize = WORKERS * 8;
    let pool = ConcurrentShardedStack::with_concurrency(WORKERS);
    for id in 0..BUFFERS {
        pool.push((id, Vec::<u8>::with_capacity(256))).unwrap();
    }
    let start = Barrier::new(WORKERS);
    std::thread::scope(|s| {
        for _ in 0..WORKERS {
            let (pool, start) = (&pool, &start);
            s.spawn(move || {
                let mut cache = LocalCache::new(pool, 4);
                start.wait();
                for request in 0..10_000 {
                    let mut value = loop {
                        match cache.take() {
                            Ok(value) => break value,
                            Err(PopError::Empty) => std::thread::yield_now(),
                            Err(PopError::Closed) => panic!("closed before join"),
                        }
                    };
                    value.1.extend_from_slice(b"response");
                    std::hint::black_box(&value.1);
                    value.1.clear();
                    cache.put(value).unwrap();
                    // Bound hoarding in service iterations; this is not a
                    // wall-clock fairness guarantee.
                    if request % 64 == 63 {
                        cache.flush().unwrap();
                    }
                }
                cache.flush().unwrap();
            });
        }
    });
    pool.close();
    let mut returned = Vec::new();
    while let Ok((id, buffer)) = pool.pop() {
        assert!(buffer.is_empty());
        returned.push(id);
    }
    returned.sort_unstable();
    assert_eq!(returned, (0..BUFFERS).collect::<Vec<_>>());
    println!("40,000 responses; all {BUFFERS} buffers returned exactly once");
}
