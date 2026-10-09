//! Allocation experiment, not a throughput benchmark. Run single-threaded.
//! The large payload makes node allocations distinguishable from EBR metadata.
use concurrent_sharded_stack::ConcurrentShardedStack;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const PAYLOAD: usize = 8192;
const NODE_BYTES: usize = PAYLOAD + size_of::<intrusive_collections::SinglyLinkedListAtomicLink>();
static COUNTING: AtomicBool = AtomicBool::new(false);
static NODES: AtomicUsize = AtomicUsize::new(0);
struct Allocator;
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() && layout.size() == NODE_BYTES && COUNTING.load(Ordering::Relaxed) {
            NODES.fetch_add(1, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn run(capacity: usize) -> usize {
    let stack = ConcurrentShardedStack::with_cache_capacity(1, capacity);
    let rounds = 128;
    let batch = 32;
    NODES.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    for _ in 0..rounds {
        for _ in 0..batch {
            stack.push([42u8; PAYLOAD]).unwrap();
        }
        for _ in 0..batch {
            std::hint::black_box(stack.pop().unwrap());
        }
        // Same explicit collector work in both modes. These quiescent intervals
        // model completed batches, not an uninterrupted low-latency hot loop.
        for _ in 0..256 {
            stack.collect();
        }
    }
    COUNTING.store(false, Ordering::Relaxed);
    let count = NODES.load(Ordering::Relaxed);
    println!(
        "cache_capacity={capacity}; pushes={}; node_allocations={count}; node_bytes={NODE_BYTES}",
        rounds * batch
    );
    count
}
fn main() {
    drop(crossbeam_epoch::pin());
    let disabled = run(0);
    let cached = run(64);
    assert_eq!(disabled, 4096);
    assert!(
        cached < disabled / 4,
        "reclaimed blocks were not being reused"
    );
}
