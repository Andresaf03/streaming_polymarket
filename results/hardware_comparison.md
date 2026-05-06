# Tabla comparativa de arquitecturas

## Plataformas

| Parámetro | Plataforma A - CPU local | Plataforma B - GPU WSL2 |
|---|---|---|
| **OS** | Windows 11 Pro 10.0.26200 | Windows 11 Pro + WSL2 Ubuntu 24.04 |
| **CPU** | Intel Core i7-9700F @ 3.00 GHz | Intel Core i7-9700F @ 3.00 GHz |
| **Núcleos / Hilos** | 8 cores / 8 threads | 8 cores / 8 threads |
| **RAM** | 32 GB | 32 GB compartida con Windows |
| **GPU** | N/A | NVIDIA GeForce RTX 2060 6 GB VRAM |
| **Driver / CUDA** | N/A | 591.86 / CUDA 13.1 |
| **Spark version** | 3.5.8 | 3.5.8 |
| **Spark mode** | `local[*]` | `local[*]` + RAPIDS |
| **Plugin GPU** | Ninguno | `com.nvidia.spark.SQLPlugin` v26.04.0 |
| **Kafka bootstrap** | `localhost:9092` | `localhost:9092` |

## Métricas de Spark UI comparadas

Las capturas finales muestran 4 queries activas en ambos casos. Spark UI no
tenía `queryName`, así que las filas se conservan en el mismo orden de la UI.

| Métrica | CPU local | GPU RAPIDS WSL2 |
|---|---:|---:|
| **Duración observada** | 19 min 02 s | 25 min 40 s |
| **Queries activas** | 4 | 4 |
| **Avg input query 1** | 1,243.50 msg/s | 1,073.11 msg/s |
| **Avg process query 1** | 6,066.96 msg/s | 1,076.45 msg/s |
| **Latest batch query 1** | 76 | 44 |
| **Avg input query 2** | 1,609.69 msg/s | 1,366.59 msg/s |
| **Avg process query 2** | 1,637.53 msg/s | 1,452.32 msg/s |
| **Latest batch query 2** | 576 | 70 |
| **Avg input query 3** | 1,613.25 msg/s | 1,470.38 msg/s |
| **Avg process query 3** | 1,636.49 msg/s | 1,437.46 msg/s |
| **Latest batch query 3** | 579 | 70 |
| **Avg input query 4** | 1,519.86 msg/s | 1,949.95 msg/s |
| **Avg process query 4** | 1,527.75 msg/s | 1,346.71 msg/s |
| **Latest batch query 4** | 569 | 104 |
| **Promedio input, 4 queries** | 1,496.58 msg/s | 1,465.01 msg/s |
| **Mediana process, 4 queries** | 1,637.01 msg/s | 1,392.09 msg/s |
| **Estabilidad final** | Estable | Estable en la captura final |

## Benchmark ML: SARIMAX inferencia 1-step (n=200)

| Métrica | CPU statsmodels | GPU cuML ARIMAX |
|---|---:|---:|
| **Training time** | 529.2 ms | 2,561.6 ms |
| **Latencia media** | 1,487.2 µs | 5,727.3 µs |
| **p50** | 1,458.2 µs | 5,595.4 µs |
| **p95** | 1,647.6 µs | 6,560.8 µs |
| **p99** | 1,769.2 µs | 7,248.2 µs |
| **Throughput** | 672 forecasts/s | 175 forecasts/s |
| **Speedup inferencia** | 1.0x | 0.26x |

## Conclusión cualitativa

La **CPU local** sigue siendo la mejor arquitectura para esta carga. La GPU ya
corre con RAPIDS y la corrida final no reproduce el crash anterior, pero el
workload sigue siendo de micro-batches pequeños e I/O: Kafka source, JSON,
estado de ventanas y Kafka sink.

El resultado correcto para el informe no es "GPU no funciona"; es más preciso:
**GPU funciona, acelera algunos operadores Spark soportados, pero no supera a
CPU para este streaming de baja latencia**. La GPU sería más defendible para
batch histórico grande, muchas series temporales en paralelo o procesamiento
directo cuDF/cuML fuera del loop de 1 segundo del dashboard.

## Nota SARIMAX / Grafana

El panel SARIMAX quedó en `No data` aunque el scorer produjo mensajes porque el
topic `btc.sarimax-forecast.clean` tenía particiones heredadas y los mensajes
caían en la partición 2. El dashboard consulta partición 0. `score_stream.py`
ahora fuerza `partition=0`, igual que el sink de forecast limpio de Spark.
