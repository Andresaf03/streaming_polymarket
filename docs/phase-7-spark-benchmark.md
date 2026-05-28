# Phase 7 Spark benchmark: CPU vs GPU RAPIDS streaming

**Date:** 2026-05-10  
**Hardware:** Intel i7-9700F + NVIDIA RTX 2060 6 GB, WSL2 Ubuntu 24.04  
**Script:** `scripts/run-spark-benchmark.sh` (automated, no manual captures)  
**Stack:** Kafka + Binance producer + Polymarket producer + `spark-stream` + `score-lstm`  
**Measurement:** 5 min steady-state per run (3 min warmup discarded), metrics from `StreamingQueryListener`

---

## Results

| Metric | CPU median | GPU median | CPU p95 | GPU p95 | GPU vs CPU |
|---|---:|---:|---:|---:|---:|
| Input rate (rows/s) | 1,055 | 1,009 | 2,706 | 1,720 | **0.96×** |
| Processing rate (rows/s) | 1,063 | 995 | 2,987 | 1,715 | **0.94×** |
| Trigger execution (ms) | 1,500 | 5,080 | 5,145 | 8,887 | **0.30×** |
| Add-batch compute (ms) | 992 | 3,923 | 4,308 | 7,786 | **0.25×** |
| Rows per batch | 1,813 | 5,364 | 7,556 | 8,891 | **2.96×** |

**Batches captured:** CPU = 462, GPU = 38 (same 5-min window)

---

## Interpretation

### Throughput is the same — latency is not

Input and processing rates are nearly identical (GPU is 4–6% slower). This sounds neutral, but the batch counts tell the real story: **CPU fired 462 micro-batches in 5 minutes; GPU fired only 38.** The GPU takes 3.4× longer per trigger (5,080 ms vs 1,500 ms) and compensates by accumulating 3× more rows per batch, arriving at roughly the same rows/s throughput.

For a streaming dashboard this distinction matters:

- **CPU:** processes each window ~every 1.5 s → Grafana sees fresh data every ~1.5 s  
- **GPU:** processes each window ~every 5 s → Grafana refresh lags by ~5 s

Lower latency wins for a real-time dashboard. CPU is the right choice.

### Why GPU is slower here

RAPIDS accelerates SQL operators that are compute-bound (large joins, heavy aggregations on millions of rows). This pipeline is I/O-bound:

- Kafka source: reading JSON messages from the broker
- JSON parsing and schema extraction
- Windowed aggregations over ~1,800 rows/batch (CPU) — small by GPU standards
- Kafka sink: writing results back

At this scale the RAPIDS kernel launch overhead (~ms per operator) costs more than the compute savings. The GPU does more work per batch (larger rows), but that's a side effect of being slow, not a benefit.

This is identical to the Phase 6 SARIMAX finding, now confirmed with a larger pipeline (4 streaming queries, 2 Kafka topics, LSTM scorer running in parallel).

### LSTM scorer device

During the GPU run, `score-lstm` auto-selected CUDA (RTX 2060). Its inference latency (0.38 ms per forward pass at B=6, measured separately in Phase 7 inference benchmark) is negligible relative to Spark's 5-second trigger cycle. The scorer is not the bottleneck and its GPU usage does not conflict with RAPIDS in a measurable way at this workload level.

---

## Conclusion

| Decision | Recommendation |
|---|---|
| Spark streaming (real-time dashboard) | **CPU** — 3.4× lower trigger latency |
| LSTM inference (scorer) | **GPU** — 452× faster than CPU at B=6 |
| SARIMAX inference (scorer) | **CPU** — GPU overhead dominates (Phase 6) |
| LSTM training | **GPU** — 12.8× faster per epoch (Colab T4 data) |

The general principle, now confirmed across three experiments on the same hardware:

> GPU wins when per-operation compute exceeds kernel launch overhead.  
> GPU loses when the workload is I/O-bound or per-operation compute is tiny.  
> Spark micro-batch streaming on small windows falls in the second category.  
> Deep learning inference and training fall in the first.

---

## Artifacts

| Path | Description |
|---|---|
| `results/spark_bench_20260510_132354/comparison.csv` | Raw comparison table |
| `results/spark_bench_20260510_132354/comparison.md` | Markdown table |
| `results/spark_bench_20260510_132354/cpu_progress.jsonl` | 462 CPU batch records |
| `results/spark_bench_20260510_132354/gpu_progress.jsonl` | 38 GPU batch records |
| `data/logs/bench_spark_cpu.log` | Spark CPU run log |
| `data/logs/bench_spark_gpu.log` | Spark GPU run log |
| `scripts/run-spark-benchmark.sh` | Reproducible benchmark script |
| `scripts/compare_spark_runs.py` | Metrics aggregation script |
