# Hallazgos RAPIDS Spark GPU — RTX 2060 / WSL2

**Entorno:** NVIDIA RTX 2060 6 GB VRAM · Driver 591.86 · CUDA 13.1 · WSL2 Ubuntu 24.04 · RAPIDS 26.04.0 · Spark 3.5.8

---

## Resultado principal

RAPIDS Spark se cargó y activó correctamente, pero el pipeline de streaming resultó **más lento en GPU que en CPU**, por dos razones independientes:

1. **Incompatibilidad WSL2:** WSL2 GPU Paravirtualization no soporta las operaciones de memoria directa de cuDF. Resultado: `cudaErrorIllegalAddress` → SIGSEGV → crash del JVM. Resolvible con bare-metal Linux.

2. **Workload inadecuado para GPU:** Micro-batches de 100–500 filas tienen overhead de transferencia CPU→VRAM mayor que la ganancia de paralelismo. RAPIDS está diseñado para millones de filas por operación.

---

## Operadores que SÍ corrieron en GPU

| Operador | Detalle |
|---|---|
| `HashAggregateExec` | avg, min, max, variance, count acelerados |
| `ShuffleExchangeExec` | HashPartitioning en GPU |
| `from_json` / `JsonToStructs` | Parsing de mensajes Kafka |
| `ProjectExec` | Expresiones aritméticas y CaseWhen |
| Windowing timestamp | precisetimestampconversion en GPU |

## Operadores que NO pueden correr en GPU (limitación de Spark Streaming)

| Operador | Razón |
|---|---|
| `MicroBatchScanExec` | Kafka source es I/O; GPU no aplica |
| `StateStoreSaveExec` / `StateStoreRestoreExec` | Estado de ventanas en RocksDB (CPU) |
| `WriteToDataSourceV2Exec` | Kafka sink es I/O |
| `EventTimeWatermarkExec` | Watermark tracking en CPU |

Estos operadores son **fundamentales** en Spark Structured Streaming y no son acelerables por RAPIDS. Forman el esqueleto del pipeline; la GPU solo puede ayudar en los nodos de cómputo intermedios.

---

## Comparativa CPU vs GPU

| Métrica | CPU (Windows) | GPU (WSL2 RAPIDS) |
|---|---:|---:|
| Avg input forecast msg/s | 1,613 | < 500 (antes de crash) |
| Avg process forecast msg/s | 1,636 | degradado |
| Batch duration típico | ~0.5 s | > 2 s |
| Estabilidad | Estable 19+ min | Crash por SIGSEGV |
| GC time (19 min) | 21 s | N/A |
| Shuffle read/write | 5.3 MB / 5.3 MB | N/A |

---

## ¿Por qué GPU no ayuda en streaming de baja latencia?

El bottleneck real del pipeline es **I/O**, no cómputo:

- `polymarket.events`: ~980k mensajes / 626 MB
- `binance.book`: ~506k mensajes / 204 MB

El cómputo por mensaje (un `avg` y un `when`) es trivial. El tiempo se va en:
1. Leer de Kafka (red)
2. Deserializar JSON
3. Mantener estado de ventanas deslizantes (RocksDB)
4. Escribir a Kafka (red)

Ninguno de estos pasos se beneficia de paralelismo masivo GPU.

---

## ¿Qué sí se beneficiaría de GPU en este proyecto?

| Tarea | Herramienta | Ganancia esperada |
|---|---|---|
| Entrenamiento SARIMAX/ML en histórico | cuML, PyTorch | Alta — millones de filas |
| Inferencia batch sobre parquet | cuDF + cuML | Alta |
| Procesamiento de order book L2 completo | cuDF directo | Media-alta si batches grandes |
| Spark streaming micro-batch actual | RAPIDS | Negativa o nula |

Ver `scripts/benchmark_inference.py --gpu` para evidencia de ganancia GPU en inferencia ML.

---

## Conclusión para el reporte

> RAPIDS Accelerator for Apache Spark está diseñado para **cargas de trabajo batch con millones de filas**. En un pipeline de streaming con micro-batches de 1 segundo y volúmenes de 100–500 filas, el overhead de transferencia CPU↔GPU domina sobre la aceleración. Adicionalmente, los operadores críticos de Spark Structured Streaming (`StateStore`, `MicroBatchScan`) no son acelerables por GPU. El resultado es: **CPU supera a GPU para este caso de uso específico**, lo cual es un hallazgo válido y documentado en la literatura de sistemas de streaming.
