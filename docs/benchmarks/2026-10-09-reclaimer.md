# Reclaimer abstraction and cached values — 2026-10-09

Historical measurements of the earlier `Reclaimer` draft (`e2e94cc`), before
the three-method `Guard` revision. See [current validation](2026-10-09-guard.md).

This change shares one intrusive algorithm and makes protection/retirement
replaceable. It demonstrably reuses value-node storage; it is **not a universal
throughput improvement**. The ordinary API retains immediate payload delivery.

## Method

Baseline: `1cde7b3` (0.3.1). Candidate: this PR's 0.4 working tree; production
source and executable hashes are retained in [environment.json](2026-10-09-reclaimer/environment.json).
Linux x86_64 VM, AMD EPYC 9V74, 9 visible vCPUs with an 8-CPU cgroup quota,
Rust 1.93.0. No CPU affinity or frequency controls. Host activity is uncontrolled.
No tests or compilation ran during throughput measurements.

The existing `intrusive_bench` workloads are unchanged apart from constructor
migration: 10,000 operations per worker, 1/4/8 shards, four payload nodes per
pool worker, identical Box payloads, callback completion and exactly-once ID
validation before each timed iteration ends. Thread creation, teardown and
validation remain inside timing. Value delivery is immediate; empty-shell
collector draining is not a completion requirement for the value workload in
either revision. This is not a steady-state allocator-free microbenchmark.

First run: before then after, 20 samples, 0.5 s warmup, 1 s requested measurement
per case. Rates below use `operations / Criterion mean time`, not console slopes.
Units: **million completed cycles/transfers per second**.

| Case | Before | After | Change |
|---|---:|---:|---:|
| epoch_pool/intrusive/1w | 4.76 | 4.54 | -4.7% |
| epoch_pool/intrusive/4w | 3.38 | 1.76 | -47.8% |
| epoch_pool/intrusive/8w | 2.57 | 2.61 | +1.7% |
| epoch_pool/mutex_vec/1w | 19.72 | 19.41 | -1.6% |
| epoch_pool/mutex_vec/4w | 7.29 | 6.59 | -9.6% |
| epoch_pool/mutex_vec/8w | 6.31 | 6.26 | -0.7% |
| epoch_pool/value_stack/1w | 10.99 | 10.28 | -6.5% |
| epoch_pool/value_stack/4w | 13.23 | 6.94 | -47.6% |
| epoch_pool/value_stack/8w | 8.79 | 7.29 | -17.1% |
| epoch_transfer/intrusive/1p_1c | 6.24 | 7.81 | +25.2% |
| epoch_transfer/intrusive/4p_4c | 9.01 | 9.64 | +7.0% |
| epoch_transfer/intrusive/8p_8c | 10.98 | 7.08 | -35.5% |
| epoch_transfer/mutex_vec/1p_1c | 6.94 | 6.40 | -7.9% |
| epoch_transfer/mutex_vec/4p_4c | 2.32 | 3.77 | +62.5% |
| epoch_transfer/mutex_vec/8p_8c | 3.12 | 4.40 | +41.0% |
| epoch_transfer/value_stack/1p_1c | 4.51 | 5.26 | +16.6% |
| epoch_transfer/value_stack/4p_4c | 8.20 | 8.22 | +0.2% |
| epoch_transfer/value_stack/8p_8c | 6.15 | 6.27 | +2.0% |

## Check the apparent regressions

The first run also changes the untouched mutex baseline substantially, so its
percentages should not be interpreted as stable effects. For the four selected
cases below, reverse the order (after then before), use 1 s warmup and 3 s
requested measurement, still 20 samples. Do not combine different cases into
one average or discard the unfavorable observations.

| Case | Before | After | Change |
|---|---:|---:|---:|
| epoch_pool/intrusive/4w | 3.78 | 3.12 | -17.5% |
| epoch_pool/value_stack/4w | 12.34 | 14.19 | +15.0% |
| epoch_transfer/intrusive/8p_8c | 11.31 | 9.75 | -13.8% |
| epoch_transfer/value_stack/8p_8c | 8.19 | 7.80 | -4.8% |

The large initial 4-worker value-pool loss does not reproduce; the verification
run reverses its sign. The two selected intrusive cases remain slower in the
verification run (roughly 14–18%). These measurements do not establish a cause
or a hardware-independent regression size. This remains a performance risk for
latency-sensitive adoption; measure the intended production workload before
claiming parity or an improvement. Raw mean estimates, within-run confidence
intervals, and samples for all 18 original cases and all 4 verification cases
are in [results.json](2026-10-09-reclaimer/results.json) and
[verification.json](2026-10-09-reclaimer/verification.json). They do not capture
between-run/host variation. Text logs are supplementary; the first before log
is incomplete, while all 18 Criterion estimate/sample pairs were retained.

## Allocation experiment

`cache_profile` uses an 8192-byte value (8200-byte wrapper), 128 batches of 32
pushes/pops, and 256 collection calls between batches, single-threaded. Both
modes perform identical collection work. It counts node-sized allocation
requests separately from EBR metadata; it is not a throughput result.

| Ready cache capacity per shard | Pushes | Node allocations |
|---|---:|---:|
| 0 | 4096 | 4096 |
| 64 | 4096 | 32 |

That is 99.22% fewer node allocations in this batched workload. No claim of zero
allocations or a bounded retirement backlog is made. Under the separate pinned
old-reader experiment, the cache cannot reuse anything: 65,536 value pushes
still allocate 65,536 wrappers, as required. Intrusive push allocates zero
wrappers; 65,536 intrusive pop/defer operations request 1,023 EBR bags (32.34
bytes per node), preserving 0.3.1 batching. Default value callbacks also remain
small enough to avoid one closure allocation per node in this instrument.

## Validation

- Default and no-default-feature all-target runs: 37 unit/integration tests each,
  plus benchmark smoke checks, including both allocation instruments.
- Three rustdoc cases in each feature mode, including unsafe-constructor misuse.
- Clippy with warnings denied in both modes, formatting, warning-free rustdoc.
- Rust 1.85 checks in both modes.
- Miri full initial suite: 34 passed, one pre-existing hardware stress test ignored.
  The expanded 6-test custom-backend suite then passed with **strict provenance
  and leak checking enabled**. Together this covers 36 distinct passing tests
  plus the one intentional skip; default EBR tests use the repository's prior
  permissive-provenance/ignore-collector-leaks flags.

The custom backend is a mutex-serialized hazard-domain correctness fixture,
not a benchmarked production hazard-pointer implementation. It proves address
identity/tag handling, independent progress for unprotected nodes, synchronous
retirement, cross-thread/scoped payload behavior, domain ownership and callback
reinsertion/panic cleanup without calling EBR.

## Reproduce

```sh
cargo bench --bench intrusive_bench -- --noplot --sample-size 20 --warm-up-time 0.5 --measurement-time 1 --save-baseline refactor_after
cargo bench --bench intrusive_bench -- --noplot --sample-size 20 --warm-up-time 1 --measurement-time 3 --save-baseline verify_after 'epoch_pool/(intrusive|value_stack)/4w|epoch_transfer/(intrusive|value_stack)/8p_8c'
cargo bench --bench cache_profile
cargo bench --bench retirement_profile -- intrusive
cargo bench --bench retirement_profile -- value
```

Run the matched intrusive commands on the baseline commit with `*_before`
labels. Build before timing, run revisions sequentially, and retain the lockfile,
environment, raw estimates/samples, and slower outcomes. See
[CONTRIBUTING](../../CONTRIBUTING.md) for validation commands.
