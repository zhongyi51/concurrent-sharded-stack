#[path = "../examples/support/local_cache.rs"]
mod local_cache;
use concurrent_sharded_stack::ConcurrentShardedStack;
use local_cache::LocalCache;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[test]
fn unwind_returns_all_cached_objects_once() {
    let pool = ConcurrentShardedStack::with_concurrency(1);
    for id in 0..8 {
        pool.push(id).unwrap();
    }
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let mut cache = LocalCache::new(&pool, 4);
            for _ in 0..20 {
                let value = cache.take().unwrap();
                cache.put(value).unwrap();
            }
            panic!("worker exits");
        }))
        .is_err()
    );
    let mut ids = Vec::new();
    while let Ok(id) = pool.pop() {
        ids.push(id);
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..8).collect::<Vec<_>>());
}

#[test]
fn close_rejection_preserves_ownership_and_drops_every_value_once() {
    #[derive(Debug)]
    struct Value(Arc<AtomicUsize>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = ConcurrentShardedStack::with_concurrency(1);
    for _ in 0..4 {
        pool.push(Value(drops.clone())).unwrap();
    }
    let mut cache = LocalCache::new(&pool, 4);
    let value = cache.take().unwrap();
    cache.put(value).unwrap();
    pool.close();
    let rejected = cache.flush().unwrap_err().into_inner();
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(rejected);
    drop(cache);
    drop(pool);
    assert_eq!(drops.load(Ordering::Relaxed), 4);
}

#[test]
fn periodic_flush_preserves_fixed_pool_across_workers() {
    let pool = ConcurrentShardedStack::with_concurrency(4);
    for id in 0..32 {
        pool.push(id).unwrap();
    }
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                let mut cache = LocalCache::new(&pool, 4);
                for i in 0..if cfg!(miri) { 16 } else { 10_000 } {
                    let value = loop {
                        if let Ok(v) = cache.take() {
                            break v;
                        }
                        std::thread::yield_now();
                    };
                    cache.put(value).unwrap();
                    if i % 16 == 15 {
                        cache.flush().unwrap();
                    }
                }
            });
        }
    });
    let mut ids = Vec::new();
    while let Ok(id) = pool.pop() {
        ids.push(id);
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..32).collect::<Vec<_>>());
}
