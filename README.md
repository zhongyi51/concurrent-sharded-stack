# concurrent-sharded-stack

[![CI](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/concurrent-sharded-stack.svg)](https://crates.io/crates/concurrent-sharded-stack)
[![docs.rs](https://docs.rs/concurrent-sharded-stack/badge.svg)](https://docs.rs/concurrent-sharded-stack)

Sharded stacks for unordered object recycling. Rust 1.85+.
One intrusive Treiber core; one `Guard` trait for reader protection and
retirement. Crossbeam EBR is the bundled default backend, not part of the core.

| Type | Storage | `pop()` |
|---|---|---|
| `ConcurrentShardedStack<T, G = EpochGuard>` | Internal intrusive nodes; bounded cache of reclaimed blocks | Returns `T` immediately |
| `IntrusiveShardedStack<A, G = EpochGuard, L = SinglyLinkedListAtomicLink>` | User's embedded link | Returns `Retired<A, G, L>`; ownership delivered when safe |

## Values

```rust
use concurrent_sharded_stack::ConcurrentShardedStack;
let pool = ConcurrentShardedStack::with_concurrency(4);
pool.push(Vec::<u8>::with_capacity(256)).unwrap();
let mut buffer = pool.pop().unwrap();
buffer.clear();
pool.push(buffer).unwrap();
```

The value API remains safe and accepts borrowed/non-Send values locally; sharing
requires `T: Send`, not Sync. Only empty storage enters the reclaimer. The default
cache retains at most 64 ready blocks per shard; `with_cache_capacity(shards,
capacity)` changes this (zero disables caching). Cache misses allocate.
Pending retired blocks can exceed the cache limit while readers delay reclamation.

## Intrusive nodes

```rust
use concurrent_sharded_stack::{IntrusiveShardedStack, intrusive_collections};
use intrusive_collections::{intrusive_adapter, SinglyLinkedListAtomicLink};

#[derive(Debug)]
struct Entry { link: SinglyLinkedListAtomicLink, value: usize }
intrusive_adapter!(A = Box<Entry>: Entry { link => SinglyLinkedListAtomicLink });
// SAFETY: generated stateless Box adapter and built-in singly atomic link.
let stack = unsafe { IntrusiveShardedStack::with_concurrency(4, A::new()) };
stack.push(Box::new(Entry { link: Default::default(), value: 42 })).unwrap();
stack.pop().unwrap().defer(|entry| {
    assert_eq!(entry.value, 42);
    // May now free, mutate, reinsert, or transfer to an upstream collection.
});
stack.collect(); // helps progress; does not wait for callback completion
```

No marker traits: upstream `Adapter`, `LinkOps`, `SinglyLinkedListOps` describe
nodes. The unsafe constructor asserts their concurrent ownership contract once;
subsequent operations are safe. Custom atomic links use those same upstream traits.
A retired token keeps its link claimed and unchanged; dropping it schedules
node destruction. Forgetting it leaks ownership.

## Custom reclamation

Node layout and reclamation are independent: adapters describe nodes; `G: Guard`
provides exactly `protect`, `unpin`, and `retire`. `protect` establishes protection
itself, including the load/validation protocol required by hazard pointers. The
stack calls `unpin` on scope exit, including unwinding. Retirement identifies the
**untagged link address**, not the containing object.

Supply guards with a normal closure through unsafe
`IntrusiveShardedStack::with_guard_factory(shards, adapter, make_guard)` or
`ConcurrentShardedStack::with_guard_factory(shards, cache_capacity, make_guard)`.
All guards must belong to the same domain instance. Only the factory is shared;
G need not implement Send, Sync, Clone or Default. Collection/flush is the
backend's own API, outside the three-method trait.
See [contracts and migration](docs/intrusive.md) and the independent
[address-based test backend](tests/support/mod.rs) (blocking reference code).

With the default epoch backend, call `collect()` on each retiring thread before it waits or becomes
idle. A stalled pinned reader can delay reclamation indefinitely. The head
algorithm uses no locks; progress and allocation costs also depend on the
backend, adapter, bounded cache queues, allocator, and callbacks.

## Shared semantics

- Nonzero power-of-two shard count; LIFO within shards, no global ordering.
- `Empty` is a retryable scan result, not a consistent global snapshot.
- `close()` closes shards gradually; stored nodes remain drainable. Multiple
  concurrent closers may each return true.
- `Closed` means closed and drained, not callbacks completed. Stack drop drains
  live nodes synchronously and does not wait for already retired nodes.

0.4 removes `EpochAdapter`/`ConcurrentLinkOps` and the intrusive `Default` impl.
The old `intrusive` feature name remains a compatibility flag; both facades now
share the required intrusive dependency, also with `--no-default-features`.

[Guard refactor validation](docs/benchmarks/2026-10-09-guard.md) ·
[Validation](CONTRIBUTING.md) · [Changelog](CHANGELOG.md).
Licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT).
