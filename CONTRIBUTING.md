# Contributing

The public crate contains only the ordinary value stack. Its shard algorithm
lives in concurrent-intrusive-collections; fixes to link updates belong there.
Describe ownership and aliasing invariants when changing the private payload
wrapper. Preserve Send-but-not-Sync payloads and synchronous Drop behavior.

```sh
cargo fmt --all -- --check
cargo test --locked
cargo test --locked --release
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo doc --locked --no-deps
cargo +1.85.0 test --locked
cargo run --locked --example buffer_recycling
cargo package --locked
```

Do not use `cargo test --all-targets`: the harness-free benchmark runs full
workloads. Compile it with `cargo check --all-targets`, and run it separately.

Miri uses the documented Crossbeam compatibility options:

```sh
MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-ignore-leaks" cargo +nightly miri test --lib -- --test-threads=1
```

The global collector's baseline reproduces process-exit retained allocations
and Stacked Borrows issues; these options are not a claim that arbitrary leaks
are safe. Tests count payload destruction and exact IDs. Stress tests and Miri
are evidence, not formal proofs. Legacy hardware race stress can be skipped by
Miri where explicitly annotated.

Run `cargo bench --bench stack_bench` on an idle machine. Historical measurements
in docs/benchmarks used prior implementations; do not present them as 0.4 results.

Before release, update version/changelog, push, and wait for CI on that commit.
Run `cargo publish --dry-run --locked` from a clean checkout, publish, and verify
the registry archive against the commit before creating the matching version tag.
