# concurrent-sharded-stack

[![CI](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml)
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
...`. This groups neighboring shard indices first; thread IDs do not encode
CPU or NUMA placement, so it does not guarantee hardware locality. The scan is
fully bitmap-free: no shared hint, no cross-core cache-line invalidations on a
hot metadata line.

Trade-off: the structure is a *bag*-like LIFO. Ordering is only LIFO **within a
shard**; across shards there is no global ordering guarantee.

An `Empty` result means one scan found no item. It is not an atomic snapshot:
concurrent pushes and pops can make it occur while the stack remains nonempty.
Consumers should retry while producers are active. `Closed` from `pop` is
terminal: all shards were observed closed and drained.

`close()` propagates one shard at a time. While it is running, a rejected push
on one shard can precede a successful push on another, and `is_closed()` may
still be false. After `close()` returns, every shard rejects pushes. Its boolean
means this call closed at least one shard; concurrent callers can both get true.

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
aliasing model. Dedicated tests check payload drop counts, cleanup after a
payload destructor panics, and documented concurrent scan/close behavior.
Deferred node reclamation is not guaranteed to finish before process exit;
Miri's leak check is disabled, so these tests do not prove every retired node
has been reclaimed. As with standard containers, a second destructor panic
during unwinding aborts the process.

[Miri]: https://github.com/rust-lang/miri

## Benchmarks

The benchmarks in `benches/` compare **two implementations on the same
workload**:

- this crate's `ConcurrentShardedStack`,
- [`lockfree::stack::Stack`](https://crates.io/crates/lockfree) — a popular
  single lock-free stack on crates.io.

Two workloads exercise different access patterns:

- `object_pool` — a fixed pool where every worker repeatedly acquires (pop) and
  releases (push) an object (connection/buffer pool pattern), with 4 or 32 workers.
  An unsuccessful scan retries without growing the pool.
- `mpmc` — 4 producers + 4 consumers, or 32 producers + 32 consumers. Each worker
  handles a fixed quota; there is no extra shared per-item progress counter.

Workers start together at a barrier. Thread creation and teardown are included
in the timed iterations. The two implementations provide different ordering
contracts: the baseline is globally LIFO, whereas this crate is LIFO per shard.

```sh
cargo bench
```

### Historical measurements

The following numbers were collected before the benchmark fixes above: the
old object-pool workload could grow the pool after an unsuccessful scan, MPMC
used a shared counter on every pop, and workers had no start barrier. They are
retained as historical results, not measurements of the current harness.
Rerun `cargo bench` before drawing performance conclusions about this version.

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
lockfree cases). `thrpt` counts millions of pool iterations or MPMC transferred
elements per second (`Melem/s`), not individual push and pop calls; `time` is
wall-clock per bench iteration.

#### `object_pool` — acquire / release a fixed pool

| Threads | Sharded thrpt | lockfree thrpt | Sharded time | lockfree time | Sharded vs lockfree |
|--------:|--------------:|---------------:|-------------:|--------------:|--------------------:|
|       4 |  33.88 Melem/s |   5.85 Melem/s |       2.36 ms |      13.68 ms |               5.79x |
|      32 |  85.46 Melem/s |   4.34 Melem/s |       7.49 ms |     147.36 ms |              19.69x |

#### `mpmc` — dedicated producers / consumers

The old labels 4 and 32 meant workers **per role**, not total workers. The table
below shows the actual producer + consumer counts.

| Producers + consumers | Sharded thrpt | lockfree thrpt | Sharded time | lockfree time | Sharded vs lockfree |
|--------:|--------------:|---------------:|-------------:|--------------:|--------------------:|
|   4 + 4 |  15.48 Melem/s |   5.21 Melem/s |       5.17 ms |      15.37 ms |               2.97x |
| 32 + 32 |  17.69 Melem/s |   4.69 Melem/s |      36.18 ms |     136.32 ms |               3.77x |

These historical MPMC results used 8 and 64 total workers respectively.

### Reproduce

```sh
cargo bench --bench stack_bench
```

Raw criterion output is in `bench_0.2.1.txt` (gitignored via `bench_*.txt`).

## Changelog

### Unreleased

- Document weak empty scans and gradual closing, with deterministic tests.
- Drain remaining payloads if one payload destructor panics.
- Correct benchmark worker labels, keep the object pool fixed, synchronize
  worker starts, and remove the MPMC progress-counter bottleneck.
- Check the declared Rust 1.85 minimum in CI.

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
