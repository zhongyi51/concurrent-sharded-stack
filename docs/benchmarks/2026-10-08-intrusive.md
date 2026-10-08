# Lock-free intrusive benchmarks — 2026-10-08

**Historical 0.3.0 results.** [Follow-up profiling and its fix](2026-10-08-retirement-fix.md)
identified per-node epoch flushing as the main transfer bottleneck. The results
below predate that fix and do not describe the current implementation.

This report measures the 0.3.0 epoch-based implementation, including delivery
of retired nodes. It replaces the unpublished mutex-prototype measurements.
In these two runs the intrusive variant was slower than the original value
stack in every matched workload. Avoiding wrapper allocation did not compensate
for the then-unidentified per-node flush overhead and delayed pool availability.
The API offers embedded storage and a lock-free head algorithm, not a universal
throughput improvement.

## Environment and reproduction

- Intel Core i7-12700F, 20 logical processors; Windows NT 10.0.26200, x86_64 MSVC.
- rustc 1.93.0 (254b59607 2026-01-19), Criterion 0.5.1.
- intrusive-collections 0.10.3; crossbeam-epoch 0.9.21; crossbeam-utils 0.8.23.
- Source: the 0.3.0 release, based on `d50a879a4e0ea60e2f191a0e0cac4940f35f0cab`.
- Optimized Cargo bench profile, retaining debug symbols. No affinity or CPU
  frequency controls; heterogeneous cores and desktop activity add variation.
  This task ran no other builds or tests during either measurement run.

Run separately after building/fetching dependencies:

```sh
cargo bench --bench intrusive_bench -- --noplot --sample-size 20 --warm-up-time 1 --measurement-time 1 --save-baseline epoch1
cargo bench --bench intrusive_bench -- --noplot --sample-size 20 --warm-up-time 1 --measurement-time 1 --save-baseline epoch2
```

The recorded commands additionally used `--offline` and writable workspace
TEMP/TMP directories. Criterion may extend measurement time to collect samples.
These are short exploratory runs; use longer runs on a controlled host before
making workload-specific decisions.

## What is timed

All containers transport the same `Box<Node>` with an atomic singly-linked link
and a `usize` ID (16-byte node here). The value stack adds a wrapper allocation
on each push; the intrusive stack uses the embedded link. A mutex-backed vector
is retained only as a benchmark baseline, not as an intrusive implementation.

- **Pool:** 1/4/8 workers, four preallocated nodes per worker, 10,000 completed
  acquire/return cycles per worker. The intrusive callback returns the node to
  the pool only after its grace period. A depleted pool drives collection and
  yields; no new payloads are created. Final IDs must match the initial pool.
- **Transfer:** 1/4/8 producers plus equally many consumers, 10,000 nodes per
  producer. Payload allocation/destruction and delivery are timed. Unique IDs
  are checked with per-ID atomic counters; a shared Arc carries that validation
  state into callbacks and its reference-count traffic is included.
- Both stacks use 1/4/8 shards. Workers start at barriers. Timings include setup,
  worker creation/joining, validation, retries, callback progress, and teardown.
- The callback abstraction is identical for all containers; the original stack
  and vector invoke it synchronously, while the intrusive stack defers it.
  Pool callbacks capture an Arc to the pool and a per-worker padded completion
  counter. This instrumentation is included for every implementation.
- Completion waits until every operation's delivery/return callback has run.
  Merely removing a node does not count as a completed intrusive transfer.
- Epoch bookkeeping may allocate. `Retired::defer` flushes work so an idle
  retiring thread does not leave callbacks in its local cache. Those costs and
  the small pool's grace-period stalls are part of these results.
- Global LIFO (vector) and per-shard LIFO (stacks) differ; these workloads allow
  arbitrary order. The original stack may retain its internal retired wrappers
  beyond an iteration, unlike synchronously destroyed delivered payloads.

## Results

Each cell is **run 1 / run 2**, in millions of completed cycles/transfers per
second, calculated from Criterion's mean iteration time. One unit includes
one successful push and one successful pop. Values are point estimates, not
confidence bounds.

| Workload | Intrusive epoch stack | Original value stack | Mutex vector |
|---|---:|---:|---:|
| Pool, 1 worker | 4.16 / 4.29 | 4.30 / 8.32 | 7.51 / 13.49 |
| Pool, 4 workers | 2.59 / 3.13 | 6.80 / 9.84 | 4.01 / 3.45 |
| Pool, 8 workers | 2.48 / 3.43 | 6.35 / 10.29 | 3.56 / 3.08 |
| Transfer, 1 producer + 1 consumer | 1.42 / 3.19 | 4.20 / 3.98 | 4.20 / 4.44 |
| Transfer, 4 producers + 4 consumers | 2.89 / 2.92 | 7.48 / 7.58 | 2.31 / 2.54 |
| Transfer, 8 producers + 8 consumers | 3.65 / 3.60 | 9.02 / 9.92 | 2.32 / 2.52 |

The intrusive version exceeded the mutex vector in both multi-producer transfer
configurations, but not the original stack. Single-worker and some pool rates
varied substantially between runs. These results compare whole ownership
protocols, not just CAS or allocation costs. Larger pools, larger payloads,
batched retirement and other callback designs would be different workloads
and have not been measured here.

[Run 1 output](2026-10-08-intrusive-run1.txt),
[run 2 output](2026-10-08-intrusive-run2.txt), and
[mean estimates with 95% bounds](2026-10-08-intrusive-estimates.json) are retained.
The console may report regression-slope throughput for linear sampling; this
table consistently uses means. Within-run intervals do not account for
between-run variation. The [earlier Linux value-only report](2026-09-27-vm.md)
and [historical harness](historical.md) are separate workloads/environments.
