# Historical benchmark results

These measurements came from the README before the 0.2.2 release
([source revision](https://github.com/zhongyi51/concurrent-sharded-stack/tree/9ce4f967c0c4679540a2bb4312a2fdfcf2435a9b)).
They were collected on the maintainer's machine with the **old harness**:

- An unsuccessful object-pool scan could create another payload, growing the pool.
- MPMC workers updated a shared counter on every pop.
- Workers did not synchronize their starts at a barrier.
- MPMC labels counted workers per role, rather than total workers.

These issues change the workload and its contention. The numbers below are
retained for provenance, not as evidence for current performance. Do not compare
them directly with the [current VM results](2026-09-27-vm.md) or describe their
ratios as speedups of the current release.

## Reported environment

| Item | Value |
|---|---|
| CPU | Intel Core i7-12700F, 12 cores / 20 threads |
| OS | Windows 11 Pro, build 26200 |
| Rust | rustc 1.93.0 / cargo 1.93.0 |
| Command | `cargo bench --bench stack_bench` |

The original README described these as Criterion-estimated medians from one
run, with 100 samples per case. Throughput counts complete acquire/release
cycles or transferred elements, not individual push/pop calls. The original
raw file, `bench_0.2.1.txt`, was gitignored and is not present in the repository;
these figures have not been independently reconstructed from raw samples.

## Object pool

| Workers | Sharded (Mcycles/s) | lockfree (Mcycles/s) | Sharded time | lockfree time |
|---:|---:|---:|---:|---:|
| 4 | 33.88 | 5.85 | 2.36 ms | 13.68 ms |
| 32 | 85.46 | 4.34 | 7.49 ms | 147.36 ms |

Despite the old description as a fixed pool, this workload could grow the pool.

## MPMC

| Producers + consumers | Sharded (Mitems/s) | lockfree (Mitems/s) | Sharded time | lockfree time |
|---:|---:|---:|---:|---:|
| 4 + 4 | 15.48 | 5.21 | 5.17 ms | 15.37 ms |
| 32 + 32 | 17.69 | 4.69 | 36.18 ms | 136.32 ms |

The original labels were 4 and 32, but these cases used 8 and 64 total workers.
