# concurrent-sharded-stack

[![CI](https://github.com/l1z3/concurrent-sharded-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/l1z3/concurrent-sharded-stack/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/concurrent-sharded-stack.svg)](https://crates.io/crates/concurrent-sharded-stack)
[![docs.rs](https://docs.rs/concurrent-sharded-stack/badge.svg)](https://docs.rs/concurrent-sharded-stack)

A lock-free concurrent stack that reduces contention by sharding a classic
[Treiber stack](https://en.wikipedia.org/wiki/Treiber_stack) across multiple
per-thread shards, reclaiming memory safely with epoch-based garbage collection
(via [`crossbeam-epoch`](https://docs.rs/crossbeam-epoch)).

## Why sharding?

A single Treiber stack funnels every `push`/`pop` through one atomic head
pointer, so under heavy multi-threaded load the CAS loop becomes a contention
bottleneck. This crate keeps **N independent shards** (each a Treiber stack on
its own cache line) and routes each thread to a shard derived from its thread
id. A coarse atomic bitmap hints which shards are non-empty so that `pop` can
steal from other shards when the local one is empty.

Trade-off: the structure is a *bag*-like LIFO. Ordering is only LIFO **within a
shard**; across shards there is no global ordering guarantee.

## Example

```rust
use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
use std::sync::Arc;
use std::thread;

let stack = Arc::new(ConcurrentShardedStack::new());

let mut handles = Vec::new();
for t in 0..4 {
    let stack = Arc::clone(&stack);
    handles.push(thread::spawn(move || {
        stack.push(t).unwrap();
    }));
}
for h in handles {
    h.join().unwrap();
}

let mut popped = Vec::new();
while let Ok(v) = stack.pop() {
    popped.push(v);
}
assert_eq!(popped.len(), 4);
```

## API highlights

- `ConcurrentShardedStack::new()` — shard count derived from
  `available_parallelism()`.
- `with_concurrency(n)` — exact shard count (must be a non-zero power of two,
  `<= usize::BITS`).
- `push(value) -> Result<(), PushError<T>>` — fails only if the stack is closed.
- `pop() -> Result<T, PopError>` — non-blocking; distinguishes `Empty` from
  `Closed`.
- `close()` / `is_closed()` — graceful shutdown; existing elements remain
  poppable until drained.

## Safety

The implementation is `unsafe`-heavy by nature (lock-free + manual memory
reclamation). Correctness is checked under [Miri] in CI using the Tree Borrows
aliasing model, including dedicated tests that detect double-drops and leaks of
non-`Copy` payloads.

[Miri]: https://github.com/rust-lang/miri

## Benchmarks

Realistic-scenario benchmarks (single-thread buffering, MPMC pipeline,
work-stealing, and a `Mutex<Vec>` baseline) live in `benches/`:

```sh
cargo bench
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
