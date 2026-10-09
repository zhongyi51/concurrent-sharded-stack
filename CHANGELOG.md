# Changelog

## 0.4.0 — unreleased

- Share one intrusive core between both public stacks.
- Add one `Guard` trait (`protect`, `unpin`, `retire`) and isolate
  default Crossbeam EBR in its backend module. Create operation-local guards
  with a standard factory closure; no associated guard/domain trait. Guards
  need not be Send/Sync; stack scopes call `unpin` even during unwinding.
- Reuse safely reclaimed empty value nodes in bounded per-shard caches; retain
  immediate payload delivery and support for borrowed/non-Send local values.
- Breaking intrusive API: remove `EpochAdapter`, `ConcurrentLinkOps`, and
  `Default`; use upstream traits and an unsafe constructor contract instead.
- `intrusive` is now a compatibility feature flag; the shared core always uses
  intrusive-collections. Add custom-domain/address-protection and cache tests.

## 0.3.1 — 2026-10-08

- Batch intrusive retirement instead of flushing an epoch bag for every node.
  This removes per-node bag allocation and reduces global collector contention.
  Call `intrusive::collect()` on each retiring thread before waiting or becoming
  idle; this also applies to dropped `Retired` tokens. Thread exit flushes too.
- Add allocation profiling and regression tests for partial-batch delivery,
  idle retiring threads, thread exit, and grace-period protection.

## 0.3.0 — 2026-10-08

- Add `IntrusiveShardedStack` using atomic heads, embedded links and epoch
  reclamation, with no shard mutex or wrapper-node allocation.
- Add explicit `ConcurrentLinkOps` and `EpochAdapter` safety contracts and
  `Retired::defer` for ownership delivery after a grace period.
- Enable the optional `intrusive` feature by default; disabling default features
  preserves the value-only API without depending on `intrusive-collections`.
- Add lifecycle, custom-link and compile-fail tests; test both feature modes.
- Reorganize docs/tests and benchmark completed callback delivery. The original
  value-stack API is unchanged.

## 0.2.2 — 2026-09-27

- Document weak empty scans and gradual closing, with deterministic tests.
- Drain remaining values if one payload destructor panics.
- Correct benchmark labels, pool sizing, worker synchronization and accounting.
- Check the Rust 1.85 minimum in CI.

## 0.2.1

- Fix README.

## 0.2.0

- Refactor shard scanning in `pop` to XOR probe order.

## 0.1.0

- Initial release.
