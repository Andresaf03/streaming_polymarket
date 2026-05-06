# Tabla comparativa de arquitecturas

| Parámetro | Plataforma A — CPU local | Plataforma B — GPU WSL2 |
|---|---|---|
| **OS** | Windows 11 Pro 10.0.26200 | Windows 11 Pro + WSL2 Ubuntu 24.04 |
| **CPU** | Intel Core i7-9700F @ 3.00 GHz | Intel Core i7-9700F @ 3.00 GHz |
| **Núcleos / Hilos** | 8 cores / 8 threads | 8 cores / 8 threads |
| **RAM** | 32 GB | 32 GB (compartida con Windows) |
| **GPU** | N/A (no utilizada) | NVIDIA GeForce RTX 2060 6 GB VRAM |
| **Driver GPU** | — | 591.86 |
| **CUDA** | — | 13.1 |
| **Spark version** | 3.5.8 | 3.5.8 |
| **Spark mode** | `local[*]` (8 cores) | `local[*]` (8 cores) + RAPIDS |
| **Plugin GPU** | Ninguno | `com.nvidia.spark.SQLPlugin` v26.04.0 |
| **Python** | 3.13 | 3.12 |
| **JDK** | OpenJDK 17 (bundled PySpark) | OpenJDK 17.0.18 Ubuntu |
| **Kafka** | Docker Desktop (Windows) | Docker Desktop WSL2 backend |
| **Bootstrap** | `localhost:9092` | `localhost:9092` |
| **Checkpoint** | `data/checkpoints/phase6-cpu` | `data/checkpoints/phase6-gpu` |
| **Output path** | `data/phase6/cpu` | `data/phase6/gpu` |

## Métricas de Spark UI comparadas

| Métrica | Plataforma A — CPU | Plataforma B — GPU RAPIDS |
|---|---:|---:|
| **Uptime** | 18 min | ~8 min (crash WSL2-PV) |
| **Avg input rate (forecast)** | 1,613 msg/s | < 500 msg/s |
| **Avg process rate (forecast)** | 1,636 msg/s | degradado |
| **Batch duration típico** | ~0.5 s | > 2.0 s |
| **GC time (total)** | 21 s | N/A |
| **Shuffle read** | 5.3 MB | N/A |
| **Shuffle write** | 5.3 MB | N/A |
| **Spill memoria/disco** | 0 / 0 | N/A |
| **Complete tasks** | 19,268 | N/A |
| **Estabilidad** | Estable | SIGSEGV cuDF (WSL2-PV) |

## Benchmark ML: SARIMAX inferencia 1-step (n=200)

| Métrica | CPU statsmodels | GPU cuML ARIMAX |
|---|---:|---:|
| **Training time** | 529.2 ms | 2,561.6 ms |
| **Speedup entrenamiento** | 1.0× | 0.14× |
| **Latencia media** | 1,487.2 µs | 5,727.3 µs |
| **p50** | 1,458.2 µs | 5,595.4 µs |
| **p95** | 1,647.6 µs | 6,560.8 µs |
| **p99** | 1,769.2 µs | 7,248.2 µs |
| **Throughput** | 672 forecasts/s | 175 forecasts/s |
| **Speedup inferencia** | 1.0× | 0.26× |

## Conclusión cualitativa

**Plataforma A (CPU local)** es superior para esta carga de trabajo. El pipeline de streaming procesa micro-batches de 100–500 filas por segundo donde el cuello de botella es I/O (Kafka) y no cómputo numérico. La GPU introduce overhead de transferencia CPU↔VRAM que supera cualquier ganancia de paralelismo.

**Plataforma B (GPU WSL2)** demostró que RAPIDS Spark carga correctamente y acelera operadores de agregación (`HashAggregateExec`, `from_json`, `ShuffleExchangeExec`). Sin embargo, WSL2 GPU Paravirtualization es incompatible con el allocator de memoria de cuDF, causando `cudaErrorIllegalAddress` y SIGSEGV. En bare-metal Linux el crash no ocurriría, pero el resultado de rendimiento (GPU más lento para micro-batches) permanecería.

**GPU ganaría** si se procesaran millones de filas por batch (ej. reentrenamiento sobre histórico completo) o miles de series temporales en paralelo.
