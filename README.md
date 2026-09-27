# concurrent-sharded-stack

[![CI](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/zhongyi51/concurrent-sharded-stack/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/concurrent-sharded-stack.svg)](https://crates.io/crates/concurrent-sharded-stack)
[![docs.rs](https://docs.rs/concurrent-sharded-stack/badge.svg)](https://docs.rs/concurrent-sharded-stack)

A concurrent container for recycling objects and buffers when their order does
not matter. It spreads a Treiber stack across independent shards to reduce
contention on a single atomic head, using `crossbeam-epoch` for node reclamation.

Consider it when many threads repeatedly return and acquire reusable values,
and can retry an unsuccessful scan. Each shard is LIFO; the whole container has
no global ordering guarantee.

## Getting started

Requires Rust 1.85 or later.

```sh
cargo add concurrent-sharded-stack
```

```rust
use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};

let buffers = ConcurrentShardedStack::with_concurrency(4);
buffers.push(Vec::<u8>::with_capacity(256)).unwrap();

let mut buffer = buffers.pop().unwrap();
buffer.extend_from_slice(b"response body");
// Send or process the contents before returning the buffer.
buffer.clear();
buffers.push(buffer).unwrap();

buffers.close();
assert!(buffers.pop().is_ok()); // Closing preserves values already stored.
assert_eq!(buffers.pop(), Err(PopError::Closed));
```

The [buffer recycling example](examples/buffer_recycling.rs) shares a fixed
number of buffers between workers, backs off on `Empty`, and joins workers
before closing and draining the pool:

```sh
cargo run --example buffer_recycling
```

## Choosing a container

| Requirement | Guidance |
|---|---|
| Reuse values without a global order | This crate is an option; measure it with your workload. |
| Global LIFO ordering | Use a single stack, such as `Mutex<Vec<T>>`. |
| FIFO ordering | Use a queue, such as `crossbeam_queue::SegQueue`. |
| Fixed capacity or backpressure | Use a bounded queue or channel, such as `crossbeam_queue::ArrayQueue`. |
| Await or block until work arrives | Use a channel; this crate has no waiting or notification API. |
| No allocation per operation | Each `push` here allocates a node, even when the payload's buffer is reused. |

`pop()` returning `Empty` means one scan found no item. It is **not** an atomic
snapshot and can occur while concurrent operations keep the container nonempty.
Retry while producers are active; use a separate completion protocol or close
the container after producers finish. `Closed` from `pop()` is terminal: all
shards were observed closed and drained.

`close()` takes effect one shard at a time. During closing, a rejected push can
precede another thread's successful push, and `is_closed()` may still be false.
After `close()` returns, every shard rejects pushes. Its boolean means that this
call closed at least one shard; concurrent callers can both receive `true`.

## API and implementation

- `new()` chooses a power-of-two shard count from `available_parallelism()`.
- `with_concurrency(n)` sets the exact shard count; `n` must be a nonzero power
  of two. Shards are not a bound on the number of threads or stored elements.
- `push(value)` preserves a rejected value in `PushError::Closed(value)`.
- `pop()` distinguishes a retryable `Empty` from terminal `Closed`.

Each thread starts at a shard derived from its thread ID. An empty local shard
causes a scan in XOR-mask order: `start`, `start ^ 1`, `start ^ 2`, and so on.
Heads are cache-line padded, with no shared occupancy bitmap. Thread IDs do not
encode CPU or NUMA placement, so this does not guarantee hardware locality.

See the [API documentation](https://docs.rs/concurrent-sharded-stack) for the
full contracts.

## Benchmarks

The [VM benchmark report](docs/benchmarks/2026-09-27-vm.md) records the environment,
commands, measurements, and limitations of the current harness. Results are
specific to that workload and VM; they are not a general speedup claim.

On the recorded 8-CPU-quota Linux VM, the 8-worker pool case measured
26.43–28.84 million acquire/return cycles/s for this crate, 13.73–14.82 for
`Mutex<Vec<T>>`, and 57.16–61.90 for `SegQueue`. These ranges span two runs'
mean-based throughput estimates, not confidence intervals. The sharded stack
was slower than all four baselines in the single-worker pool case, and both
Crossbeam queues were faster in every tested MPMC configuration. See the report
for all results, per-run intervals, and contract differences.

The harness compares this crate with `lockfree::stack::Stack`, `Mutex<Vec<T>>`,
`crossbeam_queue::SegQueue`, and bounded `crossbeam_queue::ArrayQueue`:

- `object_pool`: 1, 4, or 8 workers acquire and return values from a fixed pool.
  An unsuccessful scan retries without creating another payload.
- `mpmc`: 1, 4, or 8 producers and the same number of consumers (2, 8, or 16
  total threads), each handling a fixed quota.

Workers start at a barrier. Timings include container setup and teardown, thread
creation and joining, and the operations themselves. The containers have
different ordering, capacity, allocation, and reclamation behavior; this is an
end-to-end workload comparison, not a comparison of equivalent stack contracts.

```sh
cargo bench --bench stack_bench
```

[Historical Windows measurements](docs/benchmarks/historical.md) used an older,
flawed harness and are archived separately. They do not describe this version.

## Safety and maintenance

CI checks the Rust 1.85 minimum, stable tests, formatting, Clippy, and [Miri]
with Tree Borrows. Tests cover payload drop counts, cleanup after a payload
destructor panics, and documented scan/close behavior. These checks help catch
regressions; they do not prove the unsafe implementation correct.

Node reclamation is deferred and need not finish before process exit. Miri's
leak check is disabled, so it does not verify reclamation of every retired node.
As with standard containers, a second destructor panic during unwinding aborts
the process.

Bug reports, workload reports, and focused contributions are welcome; see
[CONTRIBUTING.md](CONTRIBUTING.md) for validation commands and useful report
details. If the crate is useful to you, a GitHub star helps others find it.

[Miri]: https://github.com/rust-lang/miri

## Changelog

### 0.2.2 — 2026-09-27

- Document weak empty scans and gradual closing, with deterministic tests.
- Drain remaining payloads if one payload destructor panics, using `scopeguard`
  to resume cleanup.
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

Licensed under either [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your
option.
