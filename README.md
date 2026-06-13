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
id. When the local shard is empty, `pop` falls back to a **tree-like probe**:
shards are visited in the XOR-mask order `start, start^1, start^2, start^3,
...`, which is the same order a BFS visits a binary tree of `N` leaves — shard
`start` is the root, the next bit corresponds to the next tree level, and so
on. Locality first, distant shards last. The scan is fully bitmap-free: no
shared hint, no cross-core cache-line invalidations on a hot metadata line.

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
  with no upper bound beyond the obvious memory limit).
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

The benchmarks in `benches/` compare **two implementations on the same
workload**:

- this crate's `ConcurrentShardedStack`,
- [`lockfree::stack::Stack`](https://crates.io/crates/lockfree) — a popular
  single lock-free stack on crates.io.

Two workloads model real usage under thread counts bracketed to typical ECS
shapes (4 threads ≈ low-end 2 vCPU, 32 threads ≈ high-end 16 vCPU):

- `object_pool` — a fixed pool where every worker repeatedly acquires (pop) and
  releases (push) an object (connection/buffer pool pattern).
- `mpmc` — dedicated producer and consumer threads (fan-out work queue).

```sh
cargo bench
```

Sharding keeps throughput roughly flat as threads pile up, whereas a single
lock-free stack degrades under CAS contention — that gap is the whole point of
the crate. See the *Benchmarks* section below for a snapshot of measured
throughput.

### Environment

Captured on the maintainer's local machine.

| Item       | Value                                       |
|------------|---------------------------------------------|
| CPU        | 12th Gen Intel Core i7-12700F (12c / 20t)   |
| OS         | Windows 11 Pro (build 26200)                |
| Rust       | rustc 1.93.0 / cargo 1.93.0                 |
| Build      | `cargo bench --bench stack_bench` (release) |

### Results

Numbers are criterion-estimated medians from a single `cargo bench` run; 100
samples per case, 5–15 s wall clock per case (longer for the contended
lockfree / 32-thread cases). All `thrpt` figures are millions of
elements pushed+popped per second (`Melem/s`); `time` is wall-clock per
bench iteration.

#### `object_pool` — acquire / release a fixed pool

| Threads | Sharded thrpt | lockfree thrpt | Sharded time | lockfree time | Sharded vs lockfree |
|--------:|--------------:|---------------:|-------------:|--------------:|--------------------:|
|       4 |  33.88 Melem/s |   5.85 Melem/s |       2.36 ms |      13.68 ms |               5.79x |
|      32 |  85.46 Melem/s |   4.34 Melem/s |       7.49 ms |     147.36 ms |              19.69x |

The sharded stack scales 2.5× from 4 → 32 threads (33.88 → 85.46 Melem/s)
while the single lock-free stack actually regresses (5.85 → 4.34 Melem/s)
— a textbook CAS-contention cliff at 32 threads where every producer and
consumer is fighting over the one atomic head.

#### `mpmc` — dedicated producers / consumers (LIFO)

| Threads | Sharded thrpt | lockfree thrpt | Sharded time | lockfree time | Sharded vs lockfree |
|--------:|--------------:|---------------:|-------------:|--------------:|--------------------:|
|       4 |  15.48 Melem/s |   5.21 Melem/s |       5.17 ms |      15.37 ms |               2.97x |
|      32 |  17.69 Melem/s |   4.69 Melem/s |      36.18 ms |     136.32 ms |               3.77x |

Even on the cleaner pure-push/pure-pop workload, sharded stays 3–4× ahead
and the lockfree stack again drifts *down* with more threads (4.69
Melem/s at 32t vs 5.21 at 4t).

### Reproduce

```sh
cargo bench --bench stack_bench
```

Raw criterion output is in `bench_0.2.1.txt` (gitignored via `bench_*.txt`).

## Changelog

### 0.2.1

- Fix README.

### 0.2.0

- Refactored the shard scan in `pop` to a tree-like probe.

### 0.1.0

- Initial release.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
