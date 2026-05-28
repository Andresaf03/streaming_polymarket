# Spark CPU vs GPU benchmark

| Metric | CPU median | GPU median | CPU p95 | GPU p95 | GPU vs CPU |
|---|---:|---:|---:|---:|---:|
| Input rate (rows/s) | 1,055.06 | 1,008.60 | 2,706.17 | 1,719.67 | **0.96x** |
| Processing rate (rows/s) | 1,062.67 | 995.15 | 2,987.11 | 1,714.54 | **0.94x** |
| Trigger execution (ms) | 1,499.50 | 5,079.50 | 5,145.05 | 8,887.30 | **0.30x** |
| Add-batch compute (ms) | 991.50 | 3,922.50 | 4,308.35 | 7,786.35 | **0.25x** |
| Rows per batch | 1,812.50 | 5,363.50 | 7,555.95 | 8,891.25 | **2.96x** |

_CPU batches: 462  |  GPU batches: 38_
