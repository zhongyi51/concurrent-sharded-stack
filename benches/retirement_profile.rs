//! Allocation profiling, separate from throughput benchmarks. No external profiler needed.
//! Preallocate payloads, then count allocation requests in each ownership phase.
use concurrent_sharded_stack::{
    ConcurrentShardedStack, IntrusiveShardedStack, intrusive, intrusive_collections,
};
use intrusive_collections::{SinglyLinkedListAtomicLink, intrusive_adapter};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Default, Debug)]
struct Counts {
    allocations: usize,
    bytes: usize,
    large_allocations: usize,
}
thread_local! {
    static COUNTS: Cell<Option<Counts>> = const { Cell::new(None) };
}
fn record(size: usize) {
    let _ = COUNTS.try_with(|counter| {
        if let Some(mut counts) = counter.get() {
            counts.allocations += 1;
            counts.bytes += size;
            counts.large_allocations += usize::from(size >= 1024);
            counter.set(Some(counts));
        }
    });
}
struct CountingAllocator;
// SAFETY: forwards the allocator contract unchanged; counting uses non-allocating TLS.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, size) };
        if !ptr.is_null() {
            record(size);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
static DELIVERED: AtomicUsize = AtomicUsize::new(0);
const N: usize = 65_536;

#[derive(Debug, Default)]
struct Node {
    link: SinglyLinkedListAtomicLink,
    id: usize,
}
intrusive_adapter!(NodeAdapter = Box<Node>: Node { link => SinglyLinkedListAtomicLink });

fn measure(label: &str, run: impl FnOnce()) -> Counts {
    COUNTS.set(Some(Counts::default()));
    run();
    let counts = COUNTS.replace(None).unwrap();
    println!(
        "{label}: {counts:?}; bytes/node={:.2}",
        counts.bytes as f64 / N as f64
    );
    counts
}
fn delivered(node: Box<Node>) {
    assert!(!node.link.is_linked());
    std::hint::black_box(node.id);
    drop(node);
    DELIVERED.fetch_add(1, Ordering::Relaxed);
}
fn main() {
    let mode = std::env::args()
        .skip(1)
        .find(|arg| !arg.starts_with("--"))
        .unwrap_or_else(|| "intrusive".into());
    assert!(matches!(mode.as_str(), "intrusive" | "eager" | "value"));
    println!(
        "mode={mode}; nodes={N}; payload bytes={}",
        size_of::<Node>()
    );
    let nodes: Vec<_> = (0..N)
        .map(|id| {
            Box::new(Node {
                id,
                ..Node::default()
            })
        })
        .collect();
    // Initialize epoch TLS first. Keep an old reader pinned to separate enqueue
    // allocations from callback execution; this is not a throughput measurement.
    let guard = crossbeam_epoch::pin();
    if mode == "value" {
        let stack = ConcurrentShardedStack::with_concurrency(1);
        measure("push", || {
            for node in nodes {
                stack.push(node).unwrap();
            }
        });
        measure("pop", || {
            for _ in 0..N {
                delivered(stack.pop().unwrap());
            }
        });
    } else {
        let stack = unsafe { IntrusiveShardedStack::with_concurrency(1, NodeAdapter::new()) };
        let push_counts = measure("push", || {
            for node in nodes {
                stack.push(node).unwrap();
            }
        });
        assert_eq!(
            push_counts.allocations, 0,
            "intrusive push allocated a wrapper"
        );
        let retirement_counts = measure("pop + defer", || {
            for _ in 0..N {
                stack.pop().unwrap().defer(delivered);
                // Reproduces the old per-node flush policy after the fix.
                if mode == "eager" {
                    intrusive::collect();
                }
            }
        });
        if mode == "intrusive" {
            // An allocation regression check, not a dependency on an exact bag
            // size: even Crossbeam's reduced sanitizer bags should amortize.
            assert!(
                retirement_counts.allocations < N / 2,
                "retirement allocated per node"
            );
        }
        assert_eq!(DELIVERED.load(Ordering::Relaxed), 0);
    }
    drop(guard);
    measure("flush + delivery", || {
        let deadline = Instant::now() + Duration::from_secs(30);
        intrusive::collect();
        while DELIVERED.load(Ordering::Relaxed) != N {
            intrusive::collect();
            assert!(Instant::now() < deadline, "delivery stalled");
        }
    });
    assert_eq!(DELIVERED.load(Ordering::Relaxed), N);
}
