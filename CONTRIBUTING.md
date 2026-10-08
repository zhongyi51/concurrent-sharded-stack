# Contributing

Include a reproducer, Rust/crate versions, OS/CPU, shard and worker counts, and
how producers finish. `Empty` alone is not a completion signal. For intrusive
nodes, distinguish removal from eventual callback delivery.

## Checks

```sh
cargo fmt --all -- --check
cargo test --all-targets
cargo test --doc
cargo test --all-targets --no-default-features
cargo test --doc --no-default-features
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings
cargo +1.85.0 check --lib
cargo +1.85.0 check --lib --no-default-features
cargo run --example buffer_recycling
cargo run --example intrusive_recycling
```

Tests are grouped as follows:

- `src/tests/value.rs`: original value-stack behavior, deterministic scan/close
  interleavings, concurrent transfer, drop and panic cleanup.
- `tests/intrusive.rs`: retired-node ownership, cross-thread delivery, delayed
  link release, duplicate rejection, reuse, unique IDs, close and drop races.
- `tests/intrusive_custom.rs`: a user-defined link and trait implementation.
- Rustdoc: working API examples and compile-fail tests for missing contracts.

Unsafe changes need an invariant explanation and targeted interleaving tests.
Use the flags in CI for Miri:

```sh
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-permissive-provenance -Zmiri-disable-isolation -Zmiri-ignore-leaks" cargo +nightly miri test --lib --test intrusive --test intrusive_custom
```

The global epoch collector can retain internal allocations at process exit, so
Miri's process-exit leak check is disabled. Tests explicitly verify node drop
counts and callback completion instead. Miri and stress tests are useful checks,
not a proof of correctness.

## Benchmarks

Run on an otherwise idle host, separately from builds/tests:

```sh
cargo bench --bench stack_bench
cargo bench --bench intrusive_bench
```

Record environment, commands, raw results, payloads, workers/shards and what is
timed. The intrusive harness counts **completed** cycles/transfers, including
epoch callbacks. Retiring a node is not completed delivery. Retain slower
results, and distinguish allocation, synchronization and reclamation costs.
