#!/usr/bin/env python3
"""
compare_spark_runs.py — aggregate two JSONL metrics files (CPU vs GPU Spark run)
and emit a comparison table as CSV + markdown.

Each line in the JSONL is a StreamingQueryProgress JSON object written by
MetricsFileListener in spark_stream.py.

Usage:
    python scripts/compare_spark_runs.py \\
        results/bench_<ts>/cpu_progress.jsonl \\
        results/bench_<ts>/gpu_progress.jsonl \\
        --out results/bench_<ts>/comparison
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def load_jsonl(path: Path) -> pd.DataFrame:
    records = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            flat = {
                "query_id": obj.get("id", ""),
                "batch_id": obj.get("batchId", -1),
                "timestamp": obj.get("timestamp", ""),
                "num_input_rows": obj.get("numInputRows", 0),
                "input_rows_per_sec": obj.get("inputRowsPerSecond", 0.0),
                "processed_rows_per_sec": obj.get("processedRowsPerSecond", 0.0),
                "trigger_ms": obj.get("durationMs", {}).get("triggerExecution", 0),
                "add_batch_ms": obj.get("durationMs", {}).get("addBatch", 0),
                "get_batch_ms": obj.get("durationMs", {}).get("getBatch", 0),
                "planning_ms": obj.get("durationMs", {}).get("queryPlanning", 0),
                "sink_name": str(obj.get("sink", {}).get("description", ""))[:40],
            }
            records.append(flat)
    if not records:
        print(f"WARNING: no records parsed from {path}", file=sys.stderr)
    return pd.DataFrame(records)


def summarise(df: pd.DataFrame, label: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    numeric = [
        "num_input_rows", "input_rows_per_sec", "processed_rows_per_sec",
        "trigger_ms", "add_batch_ms", "get_batch_ms", "planning_ms",
    ]
    rows = []
    for col in numeric:
        s = df[col].dropna()
        rows.append({
            "metric": col,
            "device": label,
            "mean": round(s.mean(), 2),
            "median": round(s.median(), 2),
            "p95": round(s.quantile(0.95), 2),
            "p99": round(s.quantile(0.99), 2),
            "n_batches": len(s),
        })
    return pd.DataFrame(rows)


def build_comparison_table(cpu: pd.DataFrame, gpu: pd.DataFrame) -> pd.DataFrame:
    rows = []
    metrics = [
        ("input_rows_per_sec",    "Input rate (rows/s)"),
        ("processed_rows_per_sec","Processing rate (rows/s)"),
        ("trigger_ms",            "Trigger execution (ms)"),
        ("add_batch_ms",          "Add-batch compute (ms)"),
        ("num_input_rows",        "Rows per batch"),
    ]
    for col, label in metrics:
        cpu_median = cpu.loc[cpu.metric == col, "median"].values
        gpu_median = gpu.loc[gpu.metric == col, "median"].values
        cpu_p95    = cpu.loc[cpu.metric == col, "p95"].values
        gpu_p95    = gpu.loc[gpu.metric == col, "p95"].values
        cpu_val = float(cpu_median[0]) if len(cpu_median) else float("nan")
        gpu_val = float(gpu_median[0]) if len(gpu_median) else float("nan")
        cpu_p95_val = float(cpu_p95[0]) if len(cpu_p95) else float("nan")
        gpu_p95_val = float(gpu_p95[0]) if len(gpu_p95) else float("nan")
        if cpu_val and gpu_val:
            # for throughput metrics higher is better; for latency lower is better
            if col in ("input_rows_per_sec", "processed_rows_per_sec", "num_input_rows"):
                ratio = f"{gpu_val / cpu_val:.2f}x"
            else:
                ratio = f"{cpu_val / gpu_val:.2f}x" if gpu_val else "—"
        else:
            ratio = "—"
        rows.append({
            "metric": label,
            "cpu_median": cpu_val,
            "gpu_median": gpu_val,
            "cpu_p95": cpu_p95_val,
            "gpu_p95": gpu_p95_val,
            "gpu_vs_cpu": ratio,
        })
    return pd.DataFrame(rows)


def to_markdown(df: pd.DataFrame) -> str:
    header = "| Metric | CPU median | GPU median | CPU p95 | GPU p95 | GPU vs CPU |"
    sep    = "|---|---:|---:|---:|---:|---:|"
    lines  = [header, sep]
    for _, row in df.iterrows():
        lines.append(
            f"| {row['metric']} "
            f"| {row['cpu_median']:,.2f} "
            f"| {row['gpu_median']:,.2f} "
            f"| {row['cpu_p95']:,.2f} "
            f"| {row['gpu_p95']:,.2f} "
            f"| **{row['gpu_vs_cpu']}** |"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare CPU vs GPU Spark run metrics")
    parser.add_argument("cpu_jsonl", help="JSONL from CPU run")
    parser.add_argument("gpu_jsonl", help="JSONL from GPU/RAPIDS run")
    parser.add_argument("--out", default=None,
                        help="Output path prefix (writes <out>.csv and <out>.md)")
    args = parser.parse_args()

    cpu_df = load_jsonl(Path(args.cpu_jsonl))
    gpu_df = load_jsonl(Path(args.gpu_jsonl))

    cpu_summary = summarise(cpu_df, "cpu")
    gpu_summary = summarise(gpu_df, "gpu")

    comparison = build_comparison_table(cpu_summary, gpu_summary)

    print("\n=== Spark CPU vs GPU comparison ===\n")
    print(comparison.to_string(index=False))
    print()

    md = to_markdown(comparison)
    print(md)

    if args.out:
        out = Path(args.out)
        comparison.to_csv(f"{out}.csv", index=False)
        Path(f"{out}.md").write_text(
            f"# Spark CPU vs GPU benchmark\n\n{md}\n\n"
            f"_CPU batches: {len(cpu_df)}  |  GPU batches: {len(gpu_df)}_\n"
        )
        print(f"\n→ {out}.csv")
        print(f"→ {out}.md")


if __name__ == "__main__":
    main()
