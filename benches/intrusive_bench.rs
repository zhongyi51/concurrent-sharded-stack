//! End-to-end completed recycling cycles / transfers, including epoch delivery.
use concurrent_sharded_stack::{
    ConcurrentShardedStack, EpochAdapter, IntrusiveShardedStack, intrusive, intrusive_collections,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use intrusive_collections::{SinglyLinkedListAtomicLink, intrusive_adapter};
use std::sync::{
    Arc, Barrier, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
struct Node {
    link: SinglyLinkedListAtomicLink,
    id: usize,
}
intrusive_adapter!(NodeAdapter = Box<Node>: Node { link => SinglyLinkedListAtomicLink });
// SAFETY: stateless generated adapter using Box and the atomic singly link.
unsafe impl EpochAdapter for NodeAdapter {}
trait Pool: Send + Sync + 'static {
    fn new(shards: usize) -> Self;
    fn push(&self, node: Box<Node>);
    fn pop_into(&self, callback: impl FnOnce(Box<Node>) + Send + 'static) -> bool;
}
impl Pool for IntrusiveShardedStack<NodeAdapter> {
    fn new(shards: usize) -> Self {
        Self::with_concurrency(shards, NodeAdapter::new())
    }
    fn push(&self, node: Box<Node>) {
        self.push(node).unwrap();
    }
    fn pop_into(&self, callback: impl FnOnce(Box<Node>) + Send + 'static) -> bool {
        match self.pop() {
            Ok(node) => {
                node.defer(callback);
                true
            }
            Err(_) => false,
        }
    }
}
impl Pool for ConcurrentShardedStack<Box<Node>> {
    fn new(shards: usize) -> Self {
        Self::with_concurrency(shards)
    }
    fn push(&self, node: Box<Node>) {
        self.push(node).unwrap();
    }
    fn pop_into(&self, callback: impl FnOnce(Box<Node>) + Send + 'static) -> bool {
        match self.pop() {
            Ok(node) => {
                callback(node);
                true
            }
            Err(_) => false,
        }
    }
}
// Preserve identical Box payloads in the conventional baseline.
#[allow(clippy::vec_box)]
impl Pool for Mutex<Vec<Box<Node>>> {
    fn new(shards: usize) -> Self {
        Mutex::new(Vec::with_capacity(shards * 4))
    }
    fn push(&self, node: Box<Node>) {
        self.lock().unwrap().push(node);
    }
    fn pop_into(&self, callback: impl FnOnce(Box<Node>) + Send + 'static) -> bool {
        let node = self.lock().unwrap().pop();
        match node {
            Some(node) => {
                callback(node);
                true
            }
            None => false,
        }
    }
}
fn wait(mut ready: impl FnMut() -> bool) {
    let start = Instant::now();
    let mut attempts = 0;
    while !ready() {
        intrusive::collect();
        thread::yield_now();
        attempts += 1;
        if attempts % 1024 == 0 {
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "benchmark stalled"
            );
        }
    }
}
const OPS: usize = 10_000;
fn recycling<P: Pool>(workers: usize) {
    let pool = Arc::new(P::new(workers));
    for id in 0..workers * 4 {
        pool.push(Box::new(Node {
            id,
            ..Node::default()
        }));
    }
    let barrier = Barrier::new(workers);
    let done: Vec<_> = (0..workers)
        .map(|_| Arc::new(crossbeam_utils::CachePadded::new(AtomicUsize::new(0))))
        .collect();
    thread::scope(|scope| {
        for done in &done {
            let pool = &pool;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..OPS {
                    wait(|| {
                        let return_pool = pool.clone();
                        let done = done.clone();
                        pool.pop_into(move |node| {
                            std::hint::black_box(node.id);
                            return_pool.push(node);
                            done.fetch_add(1, Ordering::Release);
                        })
                    });
                }
            });
        }
    });
    wait(|| done.iter().all(|n| n.load(Ordering::Acquire) == OPS));
    validate(&*pool, workers * 4);
}
fn validate(pool: &impl Pool, count: usize) {
    let seen = Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    for _ in 0..count {
        let seen = seen.clone();
        assert!(pool.pop_into(move |node| {
            assert_eq!(seen[node.id].fetch_add(1, Ordering::Release), 0);
        }));
    }
    wait(|| seen.iter().all(|n| n.load(Ordering::Acquire) == 1));
    assert!(!pool.pop_into(|_| panic!("extra node")));
}
fn transfer<P: Pool>(pairs: usize) {
    let pool = P::new(pairs);
    let barrier = Barrier::new(2 * pairs);
    let seen = Arc::new(
        (0..pairs * OPS)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    thread::scope(|scope| {
        for producer in 0..pairs {
            let pool = &pool;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for i in 0..OPS {
                    pool.push(Box::new(Node {
                        id: producer * OPS + i,
                        ..Node::default()
                    }));
                }
            });
        }
        for _ in 0..pairs {
            let pool = &pool;
            let barrier = &barrier;
            let seen = &seen;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..OPS {
                    wait(|| {
                        let seen = seen.clone();
                        pool.pop_into(move |node| {
                            assert_eq!(seen[node.id].fetch_add(1, Ordering::Release), 0);
                        })
                    });
                }
            });
        }
    });
    wait(|| seen.iter().all(|n| n.load(Ordering::Acquire) == 1));
    assert!(!pool.pop_into(|_| panic!("extra node")));
}
fn benchmarks(c: &mut Criterion) {
    for workload in ["epoch_pool", "epoch_transfer"] {
        let mut group = c.benchmark_group(workload);
        for workers in [1, 4, 8] {
            group.throughput(Throughput::Elements((workers * OPS) as u64));
            let label = if workload == "epoch_pool" {
                format!("{workers}w")
            } else {
                format!("{workers}p_{workers}c")
            };
            for name in ["intrusive", "value_stack", "mutex_vec"] {
                group.bench_function(BenchmarkId::new(name, &label), |b| {
                    b.iter(|| match (workload, name) {
                        ("epoch_pool", "intrusive") => {
                            recycling::<IntrusiveShardedStack<NodeAdapter>>(workers)
                        }
                        ("epoch_pool", "value_stack") => {
                            recycling::<ConcurrentShardedStack<Box<Node>>>(workers)
                        }
                        ("epoch_pool", _) => recycling::<Mutex<Vec<Box<Node>>>>(workers),
                        (_, "intrusive") => transfer::<IntrusiveShardedStack<NodeAdapter>>(workers),
                        (_, "value_stack") => {
                            transfer::<ConcurrentShardedStack<Box<Node>>>(workers)
                        }
                        _ => transfer::<Mutex<Vec<Box<Node>>>>(workers),
                    })
                });
            }
        }
        group.finish();
    }
}
criterion_group!(benches, benchmarks);
criterion_main!(benches);
