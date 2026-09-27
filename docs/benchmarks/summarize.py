#!/usr/bin/env python3
"""Archive and summarize two complete runs of benches/stack_bench.rs.

Run from the repository root after both Criterion baselines have finished:

    python3 docs/benchmarks/summarize.py

Only the Python standard library is required. Inputs are validated before any
output is written. Existing output files are replaced, so use --output-dir to
keep a new experiment separate from the published results.
"""

import argparse
import csv
import json
import math
from pathlib import Path
import sys


CONTAINERS = (
    "sharded",
    "lockfree_lifo",
    "mutex_vec_lifo",
    "segqueue_fifo",
    "arrayqueue_fifo",
)
THREAD_COUNTS = (1, 4, 8)
WORKLOADS = ("object_pool", "mpmc")


def positive_number(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label}: expected a number, got {value!r}")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label}: expected a finite positive number")
    return value


def read_json(path):
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def collect(criterion_dir, baselines, sample_count):
    records = []
    rows = []
    expected_ids = {
        f"{workload}/{container}/{threads if workload == 'object_pool' else f'{threads}p_{threads}c'}"
        for workload in WORKLOADS
        for container in CONTAINERS
        for threads in THREAD_COUNTS
    }
    benchmark_by_id = {}
    for baseline in baselines:
        found_ids = {
            path.parent.parent.relative_to(criterion_dir).as_posix()
            for path in criterion_dir.glob(f"*/*/*/{baseline}/benchmark.json")
        }
        if found_ids != expected_ids:
            raise ValueError(
                f"{baseline}: incomplete or unexpected case set; "
                f"missing={sorted(expected_ids - found_ids)}, "
                f"extra={sorted(found_ids - expected_ids)}"
            )
        for workload in WORKLOADS:
            for container in CONTAINERS:
                for threads in THREAD_COUNTS:
                    value = str(threads) if workload == "object_pool" else f"{threads}p_{threads}c"
                    case_id = f"{workload}/{container}/{value}"
                    directory = criterion_dir / case_id / baseline
                    benchmark = read_json(directory / "benchmark.json")
                    sample = read_json(directory / "sample.json")
                    estimates = read_json(directory / "estimates.json")
                    for key, expected in (
                        ("group_id", workload),
                        ("function_id", container),
                        ("value_str", value),
                        ("full_id", case_id),
                    ):
                        if benchmark[key] != expected:
                            raise ValueError(f"{directory}: unexpected benchmark {key}")
                    if benchmark_by_id.setdefault(case_id, benchmark) != benchmark:
                        raise ValueError(f"{case_id}: benchmark metadata differs between runs")
                    elements = benchmark["throughput"]["Elements"]
                    if isinstance(elements, bool) or not isinstance(elements, int) or elements <= 0:
                        raise ValueError(f"{directory}: expected positive Elements throughput")
                    if len(sample["iters"]) != sample_count or len(sample["times"]) != sample_count:
                        raise ValueError(f"{directory}: expected {sample_count} paired samples")
                    for key in ("iters", "times"):
                        for number in sample[key]:
                            positive_number(number, f"{directory}: sample {key}")
                    mean = estimates["mean"]
                    interval = mean["confidence_interval"]
                    if interval["confidence_level"] != 0.95:
                        raise ValueError(f"{directory}: expected a 95% mean confidence interval")
                    point = positive_number(mean["point_estimate"], f"{directory}: mean")
                    low = positive_number(interval["lower_bound"], f"{directory}: lower bound")
                    high = positive_number(interval["upper_bound"], f"{directory}: upper bound")
                    if low > high:
                        raise ValueError(f"{directory}: reversed confidence interval")
                    records.append({
                        "baseline": baseline,
                        "benchmark": benchmark,
                        "sample": sample,
                        "estimates": estimates,
                    })
                    # Elements / nanoseconds * 1e3 = millions of elements / second.
                    # Inverting the duration interval reverses its endpoints.
                    rows.append({
                        "baseline": baseline,
                        "workload": workload,
                        "container": container,
                        "threads_per_role": threads,
                        "total_worker_threads": threads if workload == "object_pool" else 2 * threads,
                        "elements_per_round": elements,
                        "unit": "cycles" if workload == "object_pool" else "items",
                        "mean_round_ns": point,
                        "mean_round_95ci_low_ns": low,
                        "mean_round_95ci_high_ns": high,
                        "million_units_per_second": elements * 1_000 / point,
                        "million_units_per_second_95ci_low": elements * 1_000 / high,
                        "million_units_per_second_95ci_high": elements * 1_000 / low,
                    })
    return records, rows


def markdown_tables(rows, baselines):
    print(
        f"Each cell is the range of the two independent run means ({' and '.join(baselines)}), "
        "not a confidence interval. Throughput is elements per round divided by "
        "Criterion's mean round duration. Per-run 95% intervals are in `results.csv`.\n"
    )
    for workload in WORKLOADS:
        if workload == "object_pool":
            print("Object pool — million acquire/return cycles per second.\n")
            labels = [f"{threads} worker{'s' if threads != 1 else ''}" for threads in THREAD_COUNTS]
        else:
            print("MPMC — million transferred items per second.\n")
            labels = [f"{threads}p + {threads}c ({2 * threads} threads)" for threads in THREAD_COUNTS]
        print("| Container | " + " | ".join(labels) + " |")
        print("|---|" + "---:|" * len(THREAD_COUNTS))
        for container in CONTAINERS:
            cells = []
            for threads in THREAD_COUNTS:
                values = [
                    row["million_units_per_second"]
                    for row in rows
                    if row["workload"] == workload
                    and row["container"] == container
                    and row["threads_per_role"] == threads
                ]
                cells.append(f"{min(values):.2f}–{max(values):.2f}")
            print(f"| `{container}` | " + " | ".join(cells) + " |")
        print()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--criterion-dir", type=Path, default=Path("target/criterion"))
    parser.add_argument("--output-dir", type=Path, default=Path("docs/benchmarks/2026-09-27-vm"))
    parser.add_argument("--baselines", nargs=2, default=["vm-a", "vm-b"], metavar=("RUN_A", "RUN_B"))
    parser.add_argument("--sample-count", type=int, default=20)
    args = parser.parse_args()
    if args.sample_count <= 0 or args.baselines[0] == args.baselines[1]:
        parser.error("sample count must be positive and baseline names must differ")
    if any(not name or name in (".", "..") or "/" in name or "\\" in name for name in args.baselines):
        parser.error("baseline names must be single directory names")
    try:
        records, rows = collect(args.criterion_dir, args.baselines, args.sample_count)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Invalid or incomplete Criterion data: {error}\n")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    archive = {
        "schema_version": 1,
        "baselines": args.baselines,
        "cases_per_baseline": 30,
        "samples_per_case": args.sample_count,
        "duration_unit": "nanoseconds",
        "throughput_formula": "benchmark.throughput.Elements * 1000 / estimates.mean.point_estimate",
        "throughput_unit": "million completed cycles (object_pool) or transferred items (mpmc) per second",
        "estimator_note": "Reciprocal of Criterion's mean round duration; not the slope estimator used in some Criterion console output.",
        "records": records,
    }
    with (args.output_dir / "results.json").open("w", encoding="utf-8") as target:
        json.dump(archive, target, separators=(",", ":"), allow_nan=False)
        target.write("\n")
    with (args.output_dir / "results.csv").open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Validated {len(rows)} case/run pairs with {args.sample_count} samples each.", file=sys.stderr)
    markdown_tables(rows, args.baselines)


if __name__ == "__main__":
    main()
