# Hallazgos RAPIDS Spark GPU - RTX 2060 / WSL2

**Entorno:** NVIDIA RTX 2060 6 GB VRAM · Driver 591.86 · CUDA 13.1 · WSL2 Ubuntu 24.04 · RAPIDS 26.04.0 · Spark 3.5.8

---

## Resultado principal

RAPIDS Spark se cargó correctamente y la corrida final quedó estable durante
25 min 40 s con 4 queries activas. Esto corrige la conclusión anterior: el
resultado final ya no debe presentarse como "RAPIDS crashea en WSL2".

La conclusión útil para el reporte es:

> RAPIDS Spark funciona en esta máquina, pero para este streaming de baja
> latencia la CPU local es igual o mejor. El overhead de mover datos hacia GPU y
> los operadores de Structured Streaming que quedan en CPU pesan más que la
> aceleración de agregaciones.

---

## Comparativa CPU vs GPU

| Métrica | CPU Windows | GPU WSL2 RAPIDS |
|---|---:|---:|
| Duración observada | 19 min 02 s | 25 min 40 s |
| Active streaming queries | 4 | 4 |
| Promedio input, 4 queries | 1,496.58 msg/s | 1,465.01 msg/s |
| Mediana process, 4 queries | 1,637.01 msg/s | 1,392.09 msg/s |
| Query input más alto | 1,613.25 msg/s | 1,949.95 msg/s |
| Query process más alto | 6,066.96 msg/s | 1,452.32 msg/s |
| Estabilidad final | Estable | Estable |

La GPU tuvo una query con mayor input promedio, pero en conjunto no supera la
corrida CPU. La comparación se debe leer como evidencia de arquitectura, no
como un benchmark perfecto de laboratorio: las queries no tienen nombre en Spark
UI y las capturas fueron tomadas en ventanas distintas.

---

## Operadores que sí pueden correr en GPU

| Operador | Detalle |
|---|---|
| `HashAggregateExec` | avg, min, max, variance, count acelerables |
| `ShuffleExchangeExec` | HashPartitioning acelerable |
| `from_json` / `JsonToStructs` | Parsing JSON acelerable cuando el plan es compatible |
| `ProjectExec` | Expresiones aritméticas y `CaseWhen` acelerables |
| Windowing timestamp | Conversiones de timestamp compatibles con RAPIDS |

## Operadores que no conviene vender como GPU

| Operador | Razón |
|---|---|
| `MicroBatchScanExec` | Kafka source es I/O; GPU no aplica |
| `StateStoreSaveExec` / `StateStoreRestoreExec` | Estado de ventanas en CPU |
| `WriteToDataSourceV2Exec` | Kafka sink es I/O |
| `EventTimeWatermarkExec` | Watermark tracking en CPU |

Estos operadores son parte central de Spark Structured Streaming. Por eso la
GPU solo acelera islas de cómputo dentro del plan, no todo el pipeline.

---

## ¿Por qué GPU no ayuda mucho aquí?

El bottleneck real del pipeline es I/O y estado, no álgebra pesada:

1. Leer de Kafka.
2. Deserializar JSON.
3. Mantener ventanas con watermark.
4. Escribir Parquet y Kafka.
5. Refrescar Grafana desde topics limpios.

El cómputo por mensaje es pequeño. En batches de segundos, el costo fijo de
RAPIDS y la transferencia RAM/VRAM puede comerse cualquier ganancia.

---

## SARIMAX y dashboard

El `No data` de SARIMAX no era un problema de modelo ni de GPU. Kafka tenía
mensajes en `btc.sarimax-forecast.clean`, pero estaban en la partición 2. El
dashboard consulta partición 0 porque el plugin de Grafana trabaja por
partición. La corrección es que `score_stream.py` publique siempre en
`partition=0`.

Además, el comando:

```bash
WINDOWS_HOST=$(cat /etc/resolv.conf | grep nameserver | awk '{print $2}')
```

es sintaxis de bash. Si aparece el error `The term 'WINDOWS_HOST=$(...)' is not
recognized`, se ejecutó en PowerShell, no dentro de WSL. En esta configuración
Kafka anuncia `localhost:9092`, así que desde WSL lo más simple es:

```bash
score-stream --kafka-bootstrap localhost:9092 --debug
```

---

## Qué sí se beneficiaría de GPU

| Tarea | Herramienta | Expectativa |
|---|---|---|
| Batch histórico grande | cuDF / RAPIDS Spark | Buena |
| Muchas series temporales en paralelo | cuML / PyTorch | Buena |
| Procesamiento L2 profundo por lotes | cuDF directo | Media-alta |
| Streaming actual de 1 segundo | RAPIDS Spark | Nula o negativa |

Conclusión para el informe: **CPU gana para el dashboard live; GPU queda como
evidencia técnica válida de aceleración parcial y como camino para batch/ML
histórico.**
