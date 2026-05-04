# [CLAUDE.md](http://CLAUDE.md)

This file provides guidance to Claude Code when working with this repository.

## IMPORTANTE: Este archivo NUNCA debe commitearse a git

`CLAUDE.md` está en `.gitignore`. Si git lo detecta como untracked, **no lo agregues a staging**.

***

## Objetivo del proyecto

Pipeline de streaming en tiempo real sobre Polymarket CLOB WebSocket:

1. Captura de eventos de order book y precio via WebSocket
2. Broker Kafka como buffer entre producer y Spark
3. Spark Structured Streaming para estadísticas en ventanas deslizantes
4. Entrenamiento de modelo supervisado sobre datos acumulados
5. Clasificación en tiempo real sobre el stream activo
6. Comparación de la misma aplicación en ≥2 arquitecturas (local vs AWS/Colab) usando Spark UI

**Asignatura:** Arquitectura de Grandes Volúmenes de Datos — ITAM 2026 **Enunciado:** `enunciado.docx` en la raíz del proyecto

***

## Estado del flujo

```
[✓] Fase 0  — ws_live.py: validación WebSocket CLOB en tiempo real
[✓] Fase 0b — bitcoin_5m.py: live feed de mercados Up-or-Down 5/15 min por asset
[✓] Fase 1  — Kafka producers: binance_producer.py + polymarket_producer.py
[✓] Fase 2  — Spark Structured Streaming: consumers/spark_stream.py
[✓] Fase 3  — Dashboard Grafana BTC × Polymarket: infra/grafana/
[✓] Fase 4  — Entrenamiento modelo supervisado: SARIMAX(2,0,2) — docs/phase-5-report.md
[✓] Fase 5  — score_stream.py: live SARIMAX → btc.sarimax-forecast (Kafka)
[ ] Fase 6  — Comparación CPU (Mac M-series) vs GPU (Windows RTX 2060)
```

### Artefactos del modelo (Fase 4)

| Archivo | Descripción |
|---|---|
| `data/model.sarimax.pkl` | SARIMAXResults serializado (statsmodels) |
| `data/feature_list.json` | Orden (2,0,2), lista de exog, σ_log=5.37 bps, AIC grid completo |
| `data/training.parquet` | 707 snapshots de 1-min usados para entrenar |
| `docs/phase-5-report.md` | Métricas completas: RMSE 2.45 bps vs 5.36 bps Polymarket baseline |

***

## Entorno

```bash
cd /Users/andres/Documents/ITAM/octavo_semestre/arquitectura/proyecto
source .venv/bin/activate
```

Python 3.11. Kafka y Spark deben estar corriendo antes de las fases 1+.

***

## Comandos

```bash
# Fase 0 — Live feed (ya funciona)
python ws_live.py                        # top-20 mercados por volumen
python ws_live.py --query bitcoin        # filtrar por keyword
python ws_live.py --query trump --top 5  # top-5 Trump markets
python ws_live.py --all                  # top-100 sin filtro

# Fase 0b — Up-or-Down 5-min (ya funciona)
python bitcoin_5m.py                     # BTC ventana activa
python bitcoin_5m.py --asset eth         # ETH 5-min
python bitcoin_5m.py --window 15         # BTC 15-min

# Fase 4 — Recolectar datos overnight (ya funciona)
bash scripts/run-overnight.sh    # levanta infra + producers + spark, caffeina laptop
bash scripts/stop-overnight.sh   # mata procesos y guarda PIDs

# Fase 4 — Construir dataset y entrenar
build-dataset                    # lee Parquet en data/, genera data/training.parquet
train-model                      # SARIMAX AIC grid + walk-forward → data/model.sarimax.pkl

# Fase 5 — Scoring en tiempo real (pendiente)
# score_stream.py leerá data/feature_list.json + data/model.sarimax.pkl
```

***

## Cómo correr el dashboard BTC × Polymarket (full setup)

**Prerequisitos:** Docker corriendo, venv activo, project root como CWD.

### Paso 1 — Infra (topics se crean solos via kafka-init)

```bash
cd infra && docker-compose up -d && cd ..
```

`kafka-init` corre automáticamente después de que Kafka está healthy, crea los 5 topics y muere.
No hay paso manual de creación de topics.

### Paso 2 — Producers (2 terminales separadas)

```bash
# Terminal A — Binance: BTC/ETH/SOL aggTrade + bookTicker + depth
source .venv/bin/activate && binance-producer

# Terminal B — Polymarket: BTC Up-or-Down 5-min rolling (default sin flags)
source .venv/bin/activate && polymarket-producer
```

`polymarket-producer` sin flags → `--asset btc --window 5` por default.

### Paso 3 — Spark consumer

```bash
# Terminal C — esperar a que ambos producers estén streamando
source .venv/bin/activate && spark-stream --console
```

### Paso 4 — Verificar

| URL | Qué ver |
|---|---|
| `http://localhost:3000` | Grafana → dashboard "BTC × Polymarket live" |
| `http://localhost:4040` | Spark UI → Streaming tab (input/processing rate) |
| `http://localhost:8080` | Kafka UI → topics, offsets, consumer lag |

### Gotcha: pyspark version

El venv debe tener `pyspark>=3.5,<4`. El conector Kafka en spark_stream.py es `spark-sql-kafka-0-10_2.12:3.5.1` (Scala 2.12); pyspark 4.x usa Scala 2.13 → incompatible.

```bash
pip show pyspark | grep Version
# 3.5.x correcto — si dice 4.x: pip install -e ".[all]"
```

### Gotcha: Grafana tarda ~8s en arrancar

Primera vez descarga e instala el plugin `hamedkarbasi93-kafka-datasource`. Abrir `localhost:3000` antes de eso da "Safari no puede conectar". Esperar y reintentar.

---

## Parar y reiniciar el pipeline

### Parar

```bash
# 1. Ctrl+C en terminales de producers y spark-stream

# 2. Infra — stop conserva volumes (topics + datos Grafana persisten)
cd infra && docker-compose stop && cd ..
```

### Reiniciar

```bash
# Infra — start reutiliza contenedores existentes, no recrea kafka-init
cd infra && docker-compose start && cd ..

# Producers + Spark (mismo orden)
source .venv/bin/activate && binance-producer          # Terminal A
source .venv/bin/activate && polymarket-producer       # Terminal B
source .venv/bin/activate && spark-stream --console    # Terminal C
```

`docker-compose start` no corre `kafka-init` de nuevo — los topics persisten en el volume `kafka_data`.

Si hiciste `down -v` (borró volumes): usar `up -d` en vez de `start` para que `kafka-init` recree los topics.

***

## Fuente de datos: Polymarket CLOB

| API            | URL                                                                            |
| -------------- | ------------------------------------------------------------------------------ |
| Gamma REST     | `https://gamma-api.polymarket.com` — metadata de mercados activos              |
| CLOB WebSocket | `wss://ws-subscriptions-clob.polymarket.com/ws/market` — stream en tiempo real |

### Protocolo WebSocket (ya validado)

```python
# Suscripción — enviar al conectar
{"assets_ids": ["token_id_1", "token_id_2"], "type": "market"}
```

### Estructura real de eventos (confirmada en ws\_live.py)

```
price_change   → {market, price_changes: [{asset_id, price, size, side, best_bid, best_ask}]}
book           → {asset_id, bids: [{price,size}], asks: [{price,size}], last_trade_price}
last_trade_price → {asset_id, price, size, side, fee_rate_bps, transaction_hash, timestamp}
```

**Gotcha:** al suscribirse llega un burst inicial de `book` snapshots (uno por token). No son cambios reales — son el estado inicial del order book.

***

## Arquitectura objetivo

```
Polymarket CLOB WebSocket
         │  ws_live.py / producer.py
         ▼
      Kafka                       topic: polymarket_events
      (broker local o Docker)     partición por asset_id
         │
         ▼
  Spark Structured Streaming      ventanas: 10s / 1min / 5min
  - Deserializar JSON             watermark: 30s
  - Features en ventana           output mode: append (stats) / update (modelo)
  - Stats: min, max, μ, σ²
  - Clustering online (opcional)
         │
    ┌────┴────┐
    ▼         ▼
  Parquet   Dashboard
  sink      (Plotly Dash / PowerBI)
    │
    ▼
  Modelo supervisado              SARIMAX(2,0,2) — statsmodels
  (entrenado offline)             endógeno: log_return_5m
                                  exógeno: poly_p_up, poly_p_up_change_5m, btc_volatility_5m
    │
    ▼
  score_stream.py                 predice log_return_5m → implied price target
  (Fase 5 pendiente)              misma línea de precio que Polymarket-implied en Grafana
```

***

## Schema Kafka (topic: polymarket\_events)

```json
{
  "event_type":  "price_change | book | last_trade_price",
  "asset_id":    "string",
  "market":      "string (hex address)",
  "question":    "string",
  "price":       "float | null",
  "best_bid":    "float | null",
  "best_ask":    "float | null",
  "size":        "float | null",
  "side":        "BUY | SELL | null",
  "bids_depth":  "int | null",
  "asks_depth":  "int | null",
  "ts_event":    "string (ISO8601 from CLOB)",
  "ts_ingest":   "string (ISO8601 producer timestamp)"
}
```

***

## Métricas de comparación Spark UI

| Métrica                      | Qué mide                          | Dónde ver                |
| ---------------------------- | --------------------------------- | ------------------------ |
| Input rate / Processing rate | Throughput del stream             | Spark UI → Streaming tab |
| Batch duration               | Latencia por micro-batch          | Streaming tab → timeline |
| Shuffle read/write           | Operaciones costosas entre stages | Stages tab               |
| Executor Run Time vs GC Time | Eficiencia de cómputo             | Stages → Task Metrics    |
| Spill to disk                | Presión de memoria RAM            | Stages → Task Metrics    |
| Scheduler Delay              | Tiempo esperando recursos         | Tasks tab                |

Spark UI local: `http://localhost:4040`

***

## Arquitecturas a comparar (Fase 6)

|                | Mac M-series (CPU)        | Windows RTX 2060 (GPU)                |
| -------------- | ------------------------- | ------------------------------------- |
| Spark mode     | `local[*]`                | `local[*]` (mismo código)             |
| ML inference   | statsmodels SARIMAX + exog | cuML ARIMA (endogenous-only, RAPIDS)  |
| ML training    | statsmodels SARIMAX       | cuML ARIMA (GPU)                      |
| Entorno        | macOS, .venv              | Windows + WSL2 + conda RAPIDS 24.x    |
| Benchmark tool | `scripts/benchmark_inference.py --gpu` |                            |

**Nota cuML**: RAPIDS cuML ARIMA (24.x) no soporta exógenos — la comparación documenta la diferencia (hardware + exog) en `docs/phase-6-report.md`.

***

## Agentes por fase

| Fase                      | Agente principal                      | Agentes secundarios |
| ------------------------- | ------------------------------------- | ------------------- |
| Diseño Kafka schema       | `architect`                           | `planner`           |
| Implementar producer      | `python-reviewer`                     | `code-reviewer`     |
| Implementar Spark job     | `architect` → `performance-optimizer` | `python-reviewer`   |
| Feature engineering       | `tdd-guide`                           | `ml-code-auditor`   |
| Entrenamiento ML          | `ml-code-auditor` primero             | `python-reviewer`   |
| Scoring UDF               | `performance-optimizer`               | `architect`         |
| Comparación arquitecturas | `performance-optimizer`               | `doc-updater`       |
| Build fail PySpark        | `java-build-resolver`                 | —                   |

***

## Decisiones de diseño y gotchas

- **Partición Kafka por `asset_id`**: garantiza orden dentro de un mercado, paralelismo entre mercados.
- **Watermark en Spark**: los eventos CLOB tienen timestamps del servidor (Polygon blockchain) que pueden llegar desordenados. Usar `withWatermark("ts_event", "30 seconds")`.
- **Output mode**: `append` para sinks Parquet; `update` para scoring en tiempo real. `complete` solo para agregaciones acotadas.
- **Modelo como broadcast variable**: serializar con `sc.broadcast(model)` para evitar re-envío por task.
- **Temporal split para entrenamiento**: nunca mezclar datos de ventanas futuras con el train set. Cortar por `ts_event`.
- **`args.param or default`**: NO usar — si el usuario pasa `0` se ignora. Usar `param if param is not None else default`.
- **Volume de mercados activos**: \~900 activos, \~200 con volumen significativo. Los top-20 por volumen (Tucker, elecciones, FIFA) generan \~10-50 msg/s en conjunto. En eventos live (elecciones, partidos) puede superar los 4,096 req/s del enunciado.
- **Mercados "Up or Down" rotativos**: los mercados de 5 minutos (`btc-updown-5m-{timestamp}`) crean un nuevo market ID por ventana — NO hardcodear. El timestamp del slug es siempre un múltiplo exacto de `window × 60` segundos, por lo que se calcula directamente: `ts = (int(time.time()) // 300) * 300` y se consulta `GET /markets?slug=btc-updown-5m-{ts}`. No usar paginación ni filtros de orden — es una sola llamada. Los mercados diarios ("Bitcoin Up or Down - April 14, 5PM ET", slug `bitcoin-up-or-down-*`) son distintos: tienen mucho más volumen (\~$100k) y resuelven una vez al día.
- **Producers con reconexión automática**: binance y polymarket producer tienen loop `_stream_with_reconnect()` con backoff exponencial (1s→60s cap). El backoff se resetea a 1s si la conexión duró >30s. `open_timeout` Binance es 30s (default 10s era insuficiente para el TLS handshake).
- **SARIMAX vs LightGBM**: LightGBM trata filas como IID — ignora autocorrelación de retornos. SARIMAX(p,0,q) modela AR+MA del retorno endógeno y usa Polymarket P(up) + volatilidad como exógenos. Con ~700 rows es el fit correcto; LightGBM necesita decenas de miles.
- **Walk-forward con `append(refit=False)`**: NO refitear SARIMAX en cada paso del holdout — 142 fits = 20+ min. Ajustar una vez sobre train, luego `fit.append(new_obs, refit=False)` actualiza estado del filtro de Kalman sin tocar parámetros. ~30s total.
- **Índice entero en SARIMAX**: `append()` requiere índice temporalmente contiguo. Los snapshots tienen gaps por `dropna` → `ValueError`. Solución: resetear a índice entero antes de pasar a statsmodels.
- **Parquet con batches vacíos**: Spark deja partes de 0 bytes cuando el batch crashea. `build_dataset.py` los ignora vía `pyarrow.dataset` (restaura columnas de partición Hive automáticamente).
- **σ_log persiste en feature_list.json**: `score_stream.py` lo usa para reportar el log-return en bps junto al forecast de precio.
- **btc.sarimax-forecast**: topic nuevo de 1 partición, emitido por `score_stream.py`. Schema: `{ts, btc_mid, poly_prob, log_return_pred, sarimax_forecast, sigma_log}`. Grafana puede añadir una tercera línea (SARIMAX) al dashboard existente.
- **train_model.py ahora usa `final_fit.save()`**: statsmodels nativo. Compatible con archivos existentes — `sm.load()` internamente usa `pickle.load()`, mismos formatos.
- **score_stream.py cold start**: acumula 10 minutos de historia antes de emitir el primer forecast. Normal.
- **Windows runtime**: `common/win_console.py` parchea UTF-8 y asyncio event loop para compatibilidad Windows. Se importa en entry points.

***

## Modelo multi-mercado (opcional — plus académico)

> **No obligatorio para el enunciado.** Es una mejora sobre el modelo supervisado base que aumenta el interés arquitectónico del proyecto.

**Idea:** usar la probabilidad implícita de mercados correlacionados como regresores exógenos para predecir la dirección del precio de Bitcoin. Todo el dato viene del mismo WebSocket CLOB — no se necesitan fuentes externas.

**Feature vector real implementado (Fase 4):**

```
target:   log_return_5m  = log(BTC_mid_T+5 / BTC_mid_T)  — regresión continua

features (exógenos SARIMAX):
  poly_p_up              # Polymarket P(BTC sube en ventana de 5 min)
  poly_p_up_change_5m    # delta de poly_p_up en los últimos 5 min
  btc_volatility_5m      # σ del BTC mid en los últimos 5 min
```

**Resultados holdout (707 rows, SARIMAX(2,0,2)):**

| Métrica | SARIMAX | Polymarket-implied | Zero-baseline |
|---|---|---|---|
| RMSE (bps) | **2.45** | 5.36 | ~5.37 |
| dir_acc | **85.1%** | 58.2% | 50% |

**Implementación en Spark:**

- Un solo topic Kafka con todos los assets
- En Spark: join de múltiples streams por ventana temporal compartida (`ts_event`)
- El feature vector se construye con `groupBy(window(ts_event, "1 minute")).pivot(asset_id).agg(...)`
- Modelo: SARIMAX (no LightGBM — ver gotcha arriba)
- **Reto arquitectónico real**: el join multi-stream a tiempo real es un caso de uso clásico de Spark Structured Streaming que justifica la elección de tecnología

**Riesgos:**

- Mercados correlacionados tienen baja liquidez → señal ruidosa en ventanas cortas
- Se necesitan al menos 2-3 días de datos para entrenar (Fase 4 debe correr antes)
- El join puede generar shuffles costosos — interesante para comparar en Spark UI local vs AWS
