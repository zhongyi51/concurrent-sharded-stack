# concurrent-sharded-stack

[![CI](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/concurrent-sharded-stack.svg)](https://crates.io/crates/concurrent-sharded-stack)
[![docs.rs](https://docs.rs/concurrent-sharded-stack/badge.svg)](https://docs.rs/concurrent-sharded-stack)

Lock-free sharded stacks for unordered object recycling. Requires Rust 1.85+.
Both variants use Treiber stacks and `crossbeam-epoch`; neither uses a shard mutex.

| Type | Node storage | Result of `pop()` |
|---|---|---|
| `ConcurrentShardedStack<T>` | Allocates a wrapper on each push | Immediately returns `T` |
| `IntrusiveShardedStack<A>` | Uses the object's embedded atomic link | Returns `Retired<A>`; delivers the pointer after an epoch grace period |

## Install

```toml
[dependencies]
concurrent-sharded-stack = "0.3"
```

The **`intrusive` feature is enabled by default**. To use only the original value
stack, without the `intrusive-collections` dependency:

```toml
concurrent-sharded-stack = { version = "0.3", default-features = false }
```

## Values

```rust
use concurrent_sharded_stack::ConcurrentShardedStack;
let pool = ConcurrentShardedStack::with_concurrency(4);
pool.push(Vec::<u8>::with_capacity(256)).unwrap();
let mut buffer = pool.pop().unwrap();
buffer.clear();
pool.push(buffer).unwrap();
```

## Intrusive nodes

```rust
use concurrent_sharded_stack::{EpochAdapter, IntrusiveShardedStack, intrusive_collections};
use intrusive_collections::{intrusive_adapter, SinglyLinkedListAtomicLink};

#[derive(Debug)]
struct Entry { link: SinglyLinkedListAtomicLink, value: usize }
intrusive_adapter!(EntryAdapter = Box<Entry>: Entry { link => SinglyLinkedListAtomicLink });
// SAFETY: the generated stateless adapter and Box preserve node ownership
// and address until the collector restores the original pointer.
unsafe impl EpochAdapter for EntryAdapter {}

let stack = IntrusiveShardedStack::with_concurrency(4, EntryAdapter::new());
stack.push(Box::new(Entry { link: SinglyLinkedListAtomicLink::new(), value: 42 })).unwrap();
stack.pop().unwrap().defer(|entry| {
    // Runs on a collecting thread, after old readers finish.
    assert_eq!(entry.value, 42);
    assert!(!entry.link.is_linked());
    // Now entry can be freed or reinserted.
});
```

A popped node stays claimed until callback delivery; immediately returning a
`Box` or reusing its link would invalidate concurrent readers. Retirement is
batched: call `intrusive::collect()` **on each retiring thread before it waits or
becomes idle**, and periodically while awaiting callbacks. Another thread cannot
flush that thread's pending batch; thread exit also flushes it. Collection is
not a synchronous barrier; a pinned thread can indefinitely delay delivery.
Dropping `Retired` schedules destruction instead. Epoch bookkeeping and large
callbacks may allocate, although the stack never allocates wrapper nodes.

See the [intrusive guide](docs/intrusive.md) for trait contracts and tradeoffs,
or run the complete [recycling example](examples/intrusive_recycling.rs):

```sh
cargo run --example intrusive_recycling
```

## Shared contracts

- Shards must be a nonzero power of two. Ordering is LIFO per shard, not global.
- `pop()` scans in XOR order. `Empty` is retryable, not an atomic snapshot.
- `close()` closes shards gradually. Rejected pushes return the original value;
  stored nodes remain drainable. Its boolean reports whether this call closed
  any shard, so multiple callers may return `true`.
- `PopError::Closed` means all shards are closed and drained. Intrusive callbacks
  may still be pending; neither `close()` nor stack destruction waits for them.

[Benchmarks and profiling](docs/benchmarks/2026-10-08-retirement-fix.md) measure completed delivery,
including the epoch cost. [Validation and contribution guide](CONTRIBUTING.md).
[Changelog](CHANGELOG.md). Licensed under [Apache-2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT), at your option.
