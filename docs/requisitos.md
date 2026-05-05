# Requisitos del proyecto — estado actual

**Curso:** Arquitectura de Grandes Volúmenes de Datos — ITAM 2026
**Profesor:** Wilmer Efrén Pereira González

---

## Checklist de entregables

| # | Requisito | Estado | Dónde |
|---|---|---|---|
| 1 | Fuente de datos real en tiempo real ≥ 4,096 msg/s | ✅ | Polymarket CLOB + Binance WS |
| 2 | Kafka como broker intermedio | ✅ | `infra/docker-compose.yml` |
| 3 | Spark Structured Streaming con ventanas y watermarks | ✅ | `consumers/spark_stream.py` |
| 4 | Estadísticas en tiempo real: min, max, avg, varianza por rango de precio | ✅ | `windowed_stats()` → topic `stats.windowed` |
| 5 | Guardar datos a disco para entrenamiento posterior | ✅ | Parquet sink → `data/ticks/` |
| 6 | Modelo supervisado entrenado sobre datos acumulados | ✅ | SARIMAX(2,0,2) — `modeling/train_model.py` |
| 7 | Scoring / clasificación en tiempo real (segunda tanda de streaming) | ✅ | `consumers/score_stream.py` → `btc.sarimax-forecast` |
| 8 | Visualización animada de indicadores | ✅ | Grafana 11.3 — `http://localhost:3000` |
| 9 | Comparación en ≥ 2 arquitecturas con Spark UI | 🔄 | Scripts listos — faltan capturas + reporte |
| 10 | Tabla comparativa de hardware de cada arquitectura | ⬜ | Pendiente en `docs/phase-6-report.md` |
| 11 | Capturas de pantalla Spark UI (Jobs, Stages, Streaming, Tasks) | ⬜ | Pendiente — correr en Mac + Windows |
| 12 | Informe final (arquitecturas, datos, estadísticos, conclusiones) | ⬜ | Pendiente — `docs/phase-6-report.md` |

---

## Requisitos técnicos — detalle

### Fuente de datos (req. 1)
- **Exigido:** flujo continuo ≥ 4,096 eventos/segundo (equivalente a cascos EEG del ITAM)
- **Implementado:** Polymarket CLOB WebSocket + Binance WebSocket
- **Evidencia:** `throughput-probe --duration 60` reporta msg/s promedio; en mercados activos supera 10k/s

### Spark Structured Streaming (req. 3)
- **Exigido:** ventanas de tiempo + marcas de agua
- **Implementado:**
  - Tumbling window de 1 minuto por defecto (`--window "1 minute"`)
  - `withWatermark("ts", "30 seconds")` en el sink de stats
  - `withWatermark("ts", "10 seconds")` en el sink de forecast
- **Output modes:** `append` para Parquet; Kafka sinks con micro-batch

### Estadísticas en tiempo real (req. 4)
- **Exigido:** valores continuos agrupados por rangos; min, max, promedio, varianza
- **Implementado:** `windowed_stats()` agrupa por `(window, source, ident, price_bucket)`
  - Binance: `price_bucket = floor(price / 100) * 100` (rangos de $100 en BTC)
  - Polymarket: `price_bucket = floor(price * 10) / 10` (rangos de 0.1 en probabilidades)
  - Calcula: `price_min`, `price_max`, `price_avg`, `price_var`, `n`

### Modelo supervisado (req. 6)
- **Exigido:** entrenar sobre datos acumulados de la captura
- **Implementado:** SARIMAX(2,0,2) sobre ~707 snapshots de 1-min
  - Target: `log_return_5m` (retorno continuo 5 min adelante — regresión, no clasificación)
  - Exógenos: `poly_p_up`, `poly_p_up_change_5m`, `btc_volatility_5m`
  - Holdout RMSE: 2.45 bps vs 5.36 bps baseline Polymarket (54% mejor)
- **Artefactos:** `data/model.sarimax.pkl`, `data/training.parquet`, `data/feature_list.json`

### Scoring en tiempo real (req. 7)
- **Exigido:** reactivar el flujo y clasificar nuevos casos
- **Implementado:** `score_stream.py` consume Kafka en vivo, computa features cada minuto, emite forecast a `btc.sarimax-forecast`
- **Nota:** primera emisión ~10 min después del arranque (cold start: necesita 10 bars de historia)

### Visualización (req. 8)
- **Exigido:** PowerBI / Tableau / Qlik Sense (o equivalente)
- **Implementado:** Grafana 11.3 con `hamedkarbasi93-kafka-datasource` plugin
  - Dashboard "BTC × Polymarket live" en `http://localhost:3000`
  - Fuentes: topic `btc.forecast` (Polymarket-implied) + `btc.sarimax-forecast` (modelo)

### Comparación de arquitecturas (req. 9–11)
- **Exigido:** misma aplicación en ≥ 2 plataformas, Spark UI, ≥ 2 métricas, tabla comparativa, capturas de pantalla
- **Estado:** scripts listos, ejecución y documentación pendientes

**Métricas a capturar en Spark UI:**

| Métrica | Dónde en Spark UI | Por qué importa |
|---|---|---|
| Input rate / Processing rate | Streaming tab | Throughput del stream |
| Batch duration | Streaming tab → timeline | Latencia por micro-batch |
| Shuffle read/write | Stages tab | Operaciones costosas entre stages |
| Executor Run Time vs GC Time | Stages → Task Metrics | Eficiencia de cómputo |
| Spill to disk | Stages → Task Metrics | Presión de RAM |
| Scheduler Delay | Tasks tab | Tiempo esperando recursos |

**Arquitecturas a comparar:**

| | Mac M-series (CPU) | Windows RTX 2060 (GPU) |
|---|---|---|
| OS | macOS | Windows 11 + WSL2 |
| Spark mode | `local[*]` | `local[*]` |
| Kafka | Docker local | Docker local |
| ML inference | statsmodels SARIMAX + 3 exog (CPU) | cuML ARIMA + 3 exog (GPU) |
| Benchmark | `benchmark_inference.py` | `benchmark_inference.py --gpu` |

---

## Lo que falta para la entrega

### Inmediato (antes de correr)
```bash
# Reinstalar el paquete para registrar el entry point score-stream
pip install -e ".[all]"

# Verificar que score-stream carga el modelo existente
score-stream --debug
```

### Fase 6 — comparación (hacer en ambas máquinas)
```bash
# 1. Levantar stack y dejar correr 15+ min
./scripts/run-overnight.sh

# 2. Abrir Spark UI y tomar capturas en: Jobs, Stages, Streaming, Tasks
open http://localhost:4040

# 3. Correr benchmark de inferencia
python scripts/benchmark_inference.py          # Mac
python scripts/benchmark_inference.py --gpu    # Windows

# 4. Correr training comparison en GPU
python modeling/train_model_cuml.py            # Windows
```

### Informe (`docs/phase-6-report.md`)
Secciones requeridas por el enunciado:
1. Descripción de las dos arquitecturas (hardware specs, OS, Spark mode)
2. Estructura de los datos (Kafka topics, schemas, throughput medido)
3. Estadísticos calculados (windowed_stats output con ejemplos reales)
4. Diferencias observadas entre arquitecturas (tabla con métricas de Spark UI)
5. Benchmark de inferencia CPU vs GPU (output de `benchmark_inference.py`)
6. Conclusiones cualitativas y cuantitativas

### Demo al profesor
- Mostrar ejecución en vivo antes de última semana
- Prepararse para modificación en vivo (el profesor pedirá un cambio durante la evaluación)
- Tener pipeline levantado: producers + spark + scorer + Grafana

---

## Comandos de referencia rápida

```bash
# Stack completo
cd infra && docker compose up -d && cd ..
binance-producer &
polymarket-producer &
spark-stream --console &
score-stream &

# Verificar throughput (≥4096 msg/s requerido)
throughput-probe --duration 60

# Reentrenar modelo
build-dataset && train-model

# Benchmark CPU vs GPU
python scripts/benchmark_inference.py [--gpu]

# URLs
# http://localhost:3000  — Grafana dashboard
# http://localhost:4040  — Spark UI
# http://localhost:8080  — Kafka UI
```
