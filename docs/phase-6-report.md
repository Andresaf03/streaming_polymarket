# Phase 6 - CPU vs GPU streaming comparison

**Status:** final evidence captured on Windows CPU and WSL2 GPU.

---

## Headline

Spark with RAPIDS Accelerator runs on the RTX 2060 setup, but the CPU path is
still the better architecture for this live dashboard workload. The pipeline is
dominated by Kafka I/O, JSON parsing, streaming state, and Kafka/Grafana output;
GPU acceleration only helps selected compute operators inside the plan.

## Spark UI evidence

Spark UI showed 4 active streaming queries in both runs. Query names were not
set, so rows are kept in screenshot order.

| Metric | CPU local | GPU RAPIDS WSL2 |
|---|---:|---:|
| Observed duration | 19 min 02 s | 25 min 40 s |
| Active streaming queries | 4 | 4 |
| Avg input query 1 | 1,243.50 msg/s | 1,073.11 msg/s |
| Avg process query 1 | 6,066.96 msg/s | 1,076.45 msg/s |
| Avg input query 2 | 1,609.69 msg/s | 1,366.59 msg/s |
| Avg process query 2 | 1,637.53 msg/s | 1,452.32 msg/s |
| Avg input query 3 | 1,613.25 msg/s | 1,470.38 msg/s |
| Avg process query 3 | 1,636.49 msg/s | 1,437.46 msg/s |
| Avg input query 4 | 1,519.86 msg/s | 1,949.95 msg/s |
| Avg process query 4 | 1,527.75 msg/s | 1,346.71 msg/s |
| Average input across queries | 1,496.58 msg/s | 1,465.01 msg/s |
| Median process across queries | 1,637.01 msg/s | 1,392.09 msg/s |

The GPU run was stable in the final capture, so the report should not claim GPU
is unusable. The more defensible result is that GPU does not improve this
specific streaming workload.

## Interpretation

RAPIDS can accelerate pieces such as aggregates, projections, JSON expressions,
and shuffle. The live pipeline still depends on operators that remain CPU/I/O
bound: Kafka micro-batch scan, state store, watermarking, and Kafka sink. With
small 1-second batches, the fixed cost of GPU scheduling and RAM/VRAM movement
outweighs the arithmetic acceleration.

## SARIMAX dashboard fix

The SARIMAX dashboard issue was a Kafka partition mismatch, not a model failure.
`btc.sarimax-forecast.clean` had messages on partition 2 while Grafana queried
partition 0. `score_stream.py` now forces `partition=0` for dashboard-ready
SARIMAX messages.

From WSL2, run:

```bash
source .venv/bin/activate
score-stream --kafka-bootstrap localhost:9092 --debug
```

The `WINDOWS_HOST=$(...)` assignment is bash syntax. If PowerShell prints
`The term 'WINDOWS_HOST=$(...)' is not recognized`, the command was pasted into
PowerShell instead of WSL bash.

## Source artifacts

- `results/phase6_cpu_vs_gpu.csv`
- `results/hardware_comparison.md`
- `results/rapids_findings.md`
- `results/rapids_findings.csv`
