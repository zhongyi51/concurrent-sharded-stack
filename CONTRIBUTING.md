# Contributing

Small, focused changes and reports from real workloads are welcome. For a bug,
include a minimal reproducer, crate and Rust versions, OS/architecture, and the
result you expected. For concurrent behavior, include how producers finish and
when the container is closed; `Empty` alone is not a completion signal.

For a performance report, include the harness, release build command, CPU,
available CPUs or VM quota, payload size, worker/shard counts, and raw results.
Mention whether allocation, setup, thread startup, and cleanup are timed. Keep
results where this crate is slower, and identify contract differences between
containers.

## Local checks

```sh
cargo fmt --all -- --check
cargo test --all-targets
cargo test --doc
cargo clippy --all-targets -- -D warnings
cargo +1.85.0 check --lib
cargo run --example buffer_recycling
```

The supported minimum Rust version is 1.85. Benchmark and other development
dependencies are checked on stable; the library itself is checked on the
minimum version. Keep dependency and public API changes relevant to the fix.

Changes to unsafe code or concurrent behavior should explain the invariant
being preserved and include a focused regression test. Use the nightly Miri
command and flags from [CI](.github/workflows/ci.yml) when changing those paths;
leak checking is disabled there because epoch reclamation is deferred. Avoid
timing-based tests when a controlled thread interleaving can demonstrate the
behavior.

Run benchmarks separately from builds or tests, on an otherwise idle machine:

```sh
cargo bench --bench stack_bench
```
