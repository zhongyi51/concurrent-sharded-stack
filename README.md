# concurrent-sharded-stack

[![CI](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/concurrent-sharded-stack.svg)](https://crates.io/crates/concurrent-sharded-stack)
[![docs.rs](https://docs.rs/concurrent-sharded-stack/badge.svg)](https://docs.rs/concurrent-sharded-stack)

A value-owning concurrent sharded stack. Version 0.4 delegates its concurrent
algorithm to [concurrent-intrusive-collections](https://crates.io/crates/concurrent-intrusive-collections)
and exposes only the ordinary `ConcurrentShardedStack<T>` API.

```toml
[dependencies]
concurrent-sharded-stack = "0.4"
```

Requires Rust 1.85 and `T: Send + 'static`. Values need not implement `Sync` or `Clone`.

```rust
use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
let stack = ConcurrentShardedStack::with_concurrency(4);
stack.push(String::from("first")).unwrap();
stack.push(String::from("second")).unwrap();
assert_eq!(stack.pop().unwrap(), "second");
assert!(stack.close());
assert_eq!(stack.pop().unwrap(), "first");
assert_eq!(stack.pop(), Err(PopError::Closed));
let rejected = stack.push(String::from("rejected")).unwrap_err().into_inner();
assert_eq!(rejected, "rejected");
```

## Semantics

- `new()` selects available parallelism rounded up to a power of two, with a
  four-shard fallback. `with_concurrency(n)` requires a nonzero power of two.
- Push targets a stable thread-local shard; pop probes local then XOR-ordered shards.
  LIFO applies within each shard, not globally. A busy shard can starve another.
- `Empty` reports a scan, not an atomic snapshot. Retry while producers are active.
- `close()` closes each shard and returns whether this call changed any shard.
  Multiple concurrent callers may return true. After return, later pushes fail.
  Existing values can be drained until `Closed`; `is_closed()` observes closure.
- `is_empty()` observes empty shards during a scan; it is not a global snapshot.
- `shard_count()` reports the configured number of shards.

## Ownership and safety

Each push allocates a private node. A successful pop immediately returns the
original owned value. Only the now-empty node allocation awaits the default
Crossbeam collector's grace period, so payloads may be reused immediately.
Rejected pushes return the original value without publishing it.

The wrapper uses a private `UnsafeCell<Option<T>>`. Concurrent readers inspect
only the link; exactly one successful pop callback moves out the value while
pinned. The node is never exposed, and no shared payload reference is returned.
This is why `Send` values such as `Cell<usize>` remain supported without `Sync`.
The wrapper does not duplicate the shard/CAS/reclamation algorithm.

Drop drains remaining payloads synchronously, including continuing after one
payload destructor panics. A second panic during unwinding aborts as usual.
Retired empty node allocations may outlive the stack. Allocation, reclamation and
payload destructors are not guaranteed lock-free; the underlying link algorithm is.

## Migrating from 0.3

- Keep `ConcurrentShardedStack`, `push(T)`, `pop()`, close and shard-count calls.
- Value types now require `Send + 'static`; scoped borrows and non-Send values are
  no longer supported. `T: Sync` is not required.
- The `intrusive` feature, `intrusive` module, `IntrusiveShardedStack`, adapters,
  `Retired` and the `intrusive_collections` reexport have been removed entirely.
  Remove `features = ["intrusive"]` from dependency declarations.
- For intrusive collections, depend directly on `concurrent-intrusive-collections`
  0.3 and use its `SinglyLinked`/`Box<T>` callback API. It does not reproduce the
  former adapter or retired-node-delivery API.
- `PopError` is reexported from the shared core; match its variants rather than
  relying on previous error display strings.

Run `cargo run --example buffer_recycling` for a bounded buffer reuse example.
The surviving `stack_bench` compares value stacks; records under `docs/benchmarks`
are historical measurements of earlier releases, not performance claims for 0.4.
See [CONTRIBUTING.md](https://github.com/zhongyi51/concurrent-sharded-stack/blob/main/CONTRIBUTING.md)
for validation commands and [CHANGELOG.md](https://github.com/zhongyi51/concurrent-sharded-stack/blob/main/CHANGELOG.md)
for release history. Licensed under MIT OR Apache-2.0.
