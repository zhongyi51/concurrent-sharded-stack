# Intrusive retirement: profiling and fix — 2026-10-08

The 0.3.0 slowdown had a concrete implementation defect: `Retired::defer`
flushed Crossbeam's local retirement bag after **every node**. This defeated
batching and allocated a full bag for each callback. Removing that flush makes
the intrusive transfer workloads faster than the original stack in this run.
The four-nodes-per-worker recycling pools still favor immediate value delivery.

## Evidence

Baseline: `19bb8ee2e9a38c6176e88c8d49b689049495239f` (0.3.0). Same
i7-12700F / Windows 10.0.26200 / Rust 1.93.0 / Crossbeam 0.9.21 environment as
[the release measurements](2026-10-08-intrusive.md).

Samply 0.13.1 recorded Windows ETW profiles of the optimized benchmark for ten
seconds, before and after the fix, for `epoch_transfer/intrusive/8p_8c`.
The EXE/PDB pairs matched; Windows `dbghelp` resolved local Rust symbols that
Samply's initial symbol pass left as addresses. Symbolized stack summaries are
retained in [profiling data](2026-10-08-retirement-profile.json).

Using each sample's `threadCPUDelta` as weight (rather than counting sleeping
threads as active CPU), the named functions' self attribution was:

| Function | Before | After |
|---|---:|---:|
| `crossbeam_epoch::internal::Local::flush` | 28.91% | <0.01% |
| `crossbeam_epoch::internal::Global::collect` | 30.14% | 0.30% |
| `crossbeam_epoch::internal::Global::try_advance` | 4.77% | 0.23% |

These are statistical, whole-process self attributions, not per-operation
latencies or an exhaustive accounting of inlined reclamation code. After the
fix, callbacks, head operations and benchmark validation dominate instead.
Throughput below was measured separately with the profiler off.

The independent allocator instrument in
[`retirement_profile.rs`](../../benches/retirement_profile.rs) confirms causality.
It preallocates 65,536 identical 16-byte payloads and registers epoch TLS before
counting. An old pinned reader prevents callback execution during measurement,
separating retirement metadata from node destruction:

| Enqueue phase | Allocations | Requested bytes | Bytes per node |
|---|---:|---:|---:|
| Intrusive push, before or after | 0 | 0 | 0 |
| Intrusive pop + defer, before | 65,536 | 135,790,592 | 2,072.00 |
| Intrusive pop + defer, after | 1,023 | 2,119,656 | 32.34 |
| Original value-stack push | 65,536 | 1,048,576 | 16.00 |
| Original value-stack pop | 1,023 | 2,119,656 | 32.34 |

The last partial bag is still local at the phase boundary. These are allocation
requests in the named phases, not resident memory, peak memory or a complete
collector drain. Callback captures fit Crossbeam's inline deferred storage in
this instrument. Large captures may still allocate separately.

Crossbeam holds up to 64 callbacks per bag in this build. `flush` publishes even
a one-callback bag to its global queue and runs collection/epoch advancement.
Thus avoiding the 16-byte value-stack wrapper previously introduced a 2,072-byte
retirement allocation on every node, plus global queue traffic. Restoring
batching cuts enqueue metadata allocation by approximately 98.44%. The `eager`
instrument mode reproduces the original allocation counts on the fixed code.

## End-to-end comparison

The existing `intrusive_bench.rs` harness is unchanged: same payloads, 1/4/8
shards, 10,000 operations per worker, four preallocated nodes per pool worker,
and all callbacks/ID validation completed before an iteration ends. No larger
pool or early completion was substituted. These short, same-session runs use
20 samples, one-second warmup and one-second requested measurement time. Builds,
tests and profiling did not overlap throughput measurements.

Millions of completed cycles/transfers per second, calculated consistently from
Criterion **mean** iteration times:

| Workload | Intrusive before | Intrusive after | Speedup | Original stack after |
|---|---:|---:|---:|---:|
| Pool, 1 worker | 4.46 | 5.24 | 1.17x | 8.67 |
| Pool, 4 workers | 3.31 | 4.55 | 1.38x | 9.91 |
| Pool, 8 workers | 3.52 | 4.76 | 1.35x | 9.92 |
| Transfer, 1 producer + 1 consumer | 3.31 | 5.35 | 1.62x | 4.00 |
| Transfer, 4 producers + 4 consumers | 3.09 | 8.88 | 2.88x | 7.48 |
| Transfer, 8 producers + 8 consumers | 3.57 | 11.09 | 3.11x | 9.12 |

The corrected transfer rates exceed the matched original-stack rates by roughly
19–34%. Small pools still exhaust reusable nodes before the grace period ends:
the intrusive variant must wait for safe ownership delivery, whereas the value
stack returns the payload immediately and retires only its wrapper. Pool
exhaustion also forces partial-bag flushes, limiting batching. Removing a node
allocation does not remove that ownership-latency difference. This fix does not
claim universal superiority or change the reclamation safety contract.

A separate ten-second profile of the fixed four-worker pool supports this
remaining limitation: local flushing still accounts for 13.06% of weighted
self attribution, global collection 7.31%, and epoch advancement 5.30%. The
unchanged harness explicitly collects whenever the pool is exhausted.

[Before output](2026-10-08-retirement-before.txt),
[after output](2026-10-08-retirement-after.txt),
[mean estimates and within-run 95% bounds](2026-10-08-retirement-estimates.json).
Console throughput can use regression slopes; the table uses means throughout.
Desktop scheduling and heterogeneous cores remain uncontrolled; confidence
intervals do not include between-run variation.

## API and validation

`defer` and dropping a `Retired` now use the ordinary thread-local epoch cache.
**Each retiring thread must call `intrusive::collect()` before waiting for
delivery or becoming idle.** A different thread cannot flush that cache.
Thread exit flushes it too. For eager publication, explicitly call `collect`
after `defer`, accepting the original overhead. `collect` is not a grace-period
barrier. Nodes remain inaccessible and claimed until old readers finish.

Regression tests cover a partial batch flushed before its thread idles,
publication at thread exit, and an old reader preventing delivery even after
flush. Existing exactly-once, reuse/ABA, close-race, duplicate-link, custom-link
and destructor tests still pass. The allocation instrument asserts that push
does not allocate and retirement does not allocate once per node; it also runs
under `cargo test --all-targets`.

Validated: both feature modes and all targets, rustdoc, Clippy with warnings
denied, Rust 1.85 library checks, and all 13 intrusive/custom-link tests under
Miri with the repository's documented flags (including ignored collector-cache
leaks at process exit).

## Reproduce

```sh
cargo bench --bench retirement_profile -- intrusive
cargo bench --bench retirement_profile -- eager
cargo bench --bench retirement_profile -- value
cargo bench --bench intrusive_bench -- --noplot --sample-size 20 --warm-up-time 1 --measurement-time 1 --save-baseline after_flush_fix
```

Run the same throughput command at the baseline revision with
`--save-baseline before_flush_fix`. For Windows sampling, pass the corresponding
benchmark executable to:

```text
samply record --save-only --unstable-presymbolicate --symbol-dir <PDB-directory> -o profile.json.gz <benchmark.exe> epoch_transfer/intrusive/8p_8c --bench --profile-time 10
```

Keep the matching PDB until symbolization finishes. Sampling and allocation
instrumentation are diagnostic runs, not the source of throughput estimates.
