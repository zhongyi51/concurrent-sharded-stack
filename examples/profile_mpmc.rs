//! Object-pool style workload for `ConcurrentShardedStack`, intended to be
//! profiled by an external sampling profiler such as Intel VTune.
//!
//! Every worker thread is *both* a writer and a reader of the same stack: it
//! repeatedly acquires an object (`pop`) and immediately releases it (`push`),
//! the way a real connection / buffer pool is used by request handlers. This
//! exercises every interesting hot path in the stack *simultaneously*:
//!
//! * Each thread's own shard is hammered by its own `push`/`pop` (bitmap bit
//!   flipping between set/clear, CAS self-contention).
//! * Other threads occasionally steal from that shard (cross-shard `pop`
//!   walking the bitmap, cache-line ping-pong between shard heads).
//! * `epoch::pin` / `defer_destroy` overhead on every operation.
//!
//! There is **no** profiling code in this binary: VTune (Hotspots, or
//! Microarchitecture Exploration for IPC / cache breakdowns) launches it from
//! the outside and samples the process.
//!
//! Build & run on Windows (cmd / PowerShell):
//!
//! ```bat
//! cargo build --release --example profile_mpmc
//! :: steady-state mode (preferred for VTune — no ramp-up/teardown noise)
//! .\target\release\examples\profile_mpmc.exe --threads 32 --duration 30
//!
//! :: iteration mode (back-compat, useful for repeatable timings)
//! .\target\release\examples\profile_mpmc.exe --threads 32 --ops 1000000 --iters 10
//! ```
//!
//! Then in VTune:
//!
//! 1. New Project -> "Launch Application".
//! 2. Application: the `profile_mpmc.exe` path above.
//! 3. Arguments: e.g. `--threads 32 --duration 30`.
//! 4. Run a "Hotspots" analysis (Hardware Event-Based Sampling gives the best
//!    symbol resolution for atomics); use "Microarchitecture Exploration" if
//!    you want IPC, L1/L2 miss rate, and HITM (false-sharing) breakdowns.
//!
//! Notes for accurate VTune results:
//!
//! * Build profile is `release` with `debug = "full"` (see `Cargo.toml`), so
//!   VTune can resolve mangled Rust symbols and inline frames.
//! * `--duration` mode runs every worker on the same stack at full tilt for
//!   N seconds with **zero** setup/teardown inside the timed region — every
//!   sample comes from the real hot path.
//! * Console output around the hot loop is intentionally minimal so it
//!   doesn't pollute the profile.
//!
//! CLI flags (all optional):
//!
//! * `--threads N`           worker threads, each doing pop+push (default:
//!                           `available_parallelism()`).
//! * `--duration SECS`       steady-state mode: run every worker for SECS
//!                           seconds. Takes precedence over `--ops`/`--iters`.
//! * `--ops N`               iteration mode: pop+push ops per worker per
//!                           iteration (default: 1_000_000).
//! * `--iters N`             iteration mode: how many iterations to run
//!                           (default: 10).
//! * `--pool-per-thread N`   pre-filled pool size per worker thread
//!                           (default: 4).
//! * `--shard-count N`       override shard count (defaults to the largest
//!                           power-of-two `<= min(threads, usize::BITS)`).
//! * `--warmup`              iteration mode: run one untimed warmup iteration
//!                           first.

use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use concurrent_sharded_stack::ConcurrentShardedStack;

/// How many ops a worker performs between two `stop`-flag reads. Big enough
/// that the atomic load is amortised to noise, small enough that workers stop
/// promptly when the duration ends.
const STOP_CHECK_BATCH: usize = 256;

#[derive(Debug)]
struct Args {
    threads: usize,
    ops_per_worker: usize,
    iters: usize,
    pool_per_thread: usize,
    shard_count: Option<usize>,
    warmup: bool,
    duration: Option<Duration>,
}

impl Args {
    fn parse() -> Self {
        let default_threads = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);

        let mut args = Args {
            threads: default_threads,
            ops_per_worker: 1_000_000,
            iters: 10,
            pool_per_thread: 4,
            shard_count: None,
            warmup: false,
            duration: None,
        };

        let raw: Vec<String> = env::args().skip(1).collect();
        let mut i = 0;
        while i < raw.len() {
            match raw[i].as_str() {
                "--threads" => {
                    args.threads = raw[i + 1].parse().expect("invalid --threads");
                    i += 2;
                }
                "--ops" => {
                    args.ops_per_worker = raw[i + 1].parse().expect("invalid --ops");
                    i += 2;
                }
                "--iters" => {
                    args.iters = raw[i + 1].parse().expect("invalid --iters");
                    i += 2;
                }
                "--pool-per-thread" => {
                    args.pool_per_thread = raw[i + 1].parse().expect("invalid --pool-per-thread");
                    i += 2;
                }
                "--shard-count" => {
                    args.shard_count = Some(raw[i + 1].parse().expect("invalid --shard-count"));
                    i += 2;
                }
                "--warmup" => {
                    args.warmup = true;
                    i += 1;
                }
                "--duration" => {
                    let secs: u64 = raw[i + 1].parse().expect("invalid --duration");
                    args.duration = Some(Duration::from_secs(secs));
                    i += 2;
                }
                "--help" | "-h" => {
                    eprintln!(
                        "usage: profile_mpmc [--threads N] [--duration SECS] \
                         [--ops N] [--iters N] [--pool-per-thread N] \
                         [--shard-count N] [--warmup]"
                    );
                    std::process::exit(0);
                }
                other => panic!("unknown flag: {other}"),
            }
        }

        if args.threads < 1 {
            args.threads = 1;
        }
        if args.iters == 0 {
            args.iters = 1;
        }
        if args.pool_per_thread == 0 {
            args.pool_per_thread = 1;
        }

        args
    }
}

fn shard_hint(threads: usize) -> usize {
    threads.next_power_of_two().min(usize::BITS as usize)
}

/// Pre-fill the stack so every worker can `pop` on its very first attempt.
fn prefill(stack: &ConcurrentShardedStack<usize>, total: usize) {
    for i in 0..total {
        stack.push(i).expect("prefill push must not fail");
    }
}

/// One object-pool iteration with a *fixed* op budget per worker. Used by
/// iteration mode (`--ops` + `--iters`) where every run does identical work
/// so per-iteration timings are comparable.
#[inline(never)] // make the round trivially findable in VTune's call tree
fn run_fixed_iteration(
    threads: usize,
    ops_per_worker: usize,
    pool_per_thread: usize,
    shard_count: usize,
) -> (Duration, usize) {
    let stack: Arc<ConcurrentShardedStack<usize>> =
        Arc::new(ConcurrentShardedStack::with_concurrency(shard_count));
    prefill(&stack, threads * pool_per_thread);

    let started = Instant::now();

    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let stack = Arc::clone(&stack);
        handles.push(thread::spawn(move || {
            // Distinct seed per thread so pushed values aren't all identical
            // (only matters for clarity in heap dumps; perf-wise irrelevant).
            let mut value: usize = t.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for _ in 0..ops_per_worker {
                // Acquire an object; if the pool is momentarily empty due to
                // races, fabricate one (the pool grows by 1 — same behaviour
                // as `benches/stack_bench.rs::bench_object_pool`).
                let obj = stack.pop().unwrap_or(value);
                stack
                    .push(obj)
                    .expect("push must not fail in this benchmark");
                value = value.wrapping_add(1);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = started.elapsed();
    let total_ops = threads * ops_per_worker;

    // Keep deferred reclamation inside the timed region so `defer_destroy`
    // shows up in the profile too — it is a real per-op cost.
    drop(stack);
    let flush_started = Instant::now();
    while flush_started.elapsed() < Duration::from_millis(20) {
        crossbeam_epoch::pin().flush();
    }

    (elapsed, total_ops)
}

/// Steady-state object-pool run for a fixed wall-clock duration. Preferred
/// for sampling profilers: every thread is in the hot loop for the entire
/// timed region, no setup/teardown samples leak into the profile.
#[inline(never)]
fn run_duration(
    threads: usize,
    pool_per_thread: usize,
    shard_count: usize,
    duration: Duration,
) -> (Duration, usize) {
    let stack: Arc<ConcurrentShardedStack<usize>> =
        Arc::new(ConcurrentShardedStack::with_concurrency(shard_count));
    prefill(&stack, threads * pool_per_thread);

    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicUsize::new(0));

    let started = Instant::now();

    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let stack = Arc::clone(&stack);
        let stop = Arc::clone(&stop);
        let total_ops = Arc::clone(&total_ops);
        handles.push(thread::spawn(move || {
            let mut value: usize = t.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let mut local_ops: usize = 0;
            // Polling the stop flag every op would itself be a hot atomic
            // load and dominate the profile; check only between batches.
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..STOP_CHECK_BATCH {
                    let obj = stack.pop().unwrap_or(value);
                    stack.push(obj).expect("push must not fail");
                    value = value.wrapping_add(1);
                }
                local_ops = local_ops.wrapping_add(STOP_CHECK_BATCH);
            }
            // One global update per worker — the per-op counter would itself
            // become the hottest line in VTune otherwise.
            total_ops.fetch_add(local_ops, Ordering::Relaxed);
        }));
    }

    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = started.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);

    drop(stack);
    let flush_started = Instant::now();
    while flush_started.elapsed() < Duration::from_millis(20) {
        crossbeam_epoch::pin().flush();
    }

    (elapsed, ops)
}

fn print_result(label: &str, elapsed: Duration, ops: usize) {
    let mops = ops as f64 / elapsed.as_secs_f64() / 1.0e6;
    let ns_per_op = elapsed.as_nanos() as f64 / ops.max(1) as f64;
    println!(
        "{label:<10}: {elapsed:>12?}  {ops:>12} ops  {mops:>7.2} M ops/s  {ns_per_op:>6.1} ns/op"
    );
}

fn main() {
    let args = Args::parse();

    let shard_count = args.shard_count.unwrap_or_else(|| shard_hint(args.threads));

    println!("=== ConcurrentShardedStack object-pool workload ===");
    println!("threads        : {} (each does pop+push)", args.threads);
    println!("pool/thread    : {}", args.pool_per_thread);
    println!("pool prefill   : {}", args.threads * args.pool_per_thread);
    println!("shard count    : {}", shard_count);

    if let Some(duration) = args.duration {
        println!("mode           : steady-state");
        println!("duration       : {} s", duration.as_secs());
        println!();

        let (elapsed, ops) =
            run_duration(args.threads, args.pool_per_thread, shard_count, duration);

        println!("--- result ---");
        print_result("total", elapsed, ops);
    } else {
        println!("mode           : iteration");
        println!("ops/worker     : {}", args.ops_per_worker);
        println!("iterations     : {}", args.iters);
        println!("warmup         : {}", args.warmup);
        println!();

        if args.warmup {
            println!("warmup iteration...");
            let (warmup_elapsed, _) = run_fixed_iteration(
                args.threads,
                args.ops_per_worker,
                args.pool_per_thread,
                shard_count,
            );
            println!("warmup done in {:?}", warmup_elapsed);
            println!();
        }

        println!("--- timed iterations ---");
        let mut total_elapsed = Duration::ZERO;
        let mut total_ops = 0usize;
        for iter in 1..=args.iters {
            let (elapsed, ops) = run_fixed_iteration(
                args.threads,
                args.ops_per_worker,
                args.pool_per_thread,
                shard_count,
            );
            print_result(&format!("iter {iter:>2}"), elapsed, ops);
            total_elapsed += elapsed;
            total_ops += ops;
        }

        println!();
        println!("--- summary ---");
        print_result("total", total_elapsed, total_ops);
    }
}
