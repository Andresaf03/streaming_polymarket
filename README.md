# Polymarket Real-Time Streaming

Pipeline de ingestión y análisis en tiempo real sobre los mercados de predicción de [Polymarket](https://polymarket.com), combinado con datos de precio de Binance, procesado con Spark Structured Streaming y un modelo de series de tiempo entrenado sobre el archivo acumulado.

---

## Arquitectura

```
 Binance WebSocket                Polymarket CLOB WebSocket
 BTC/ETH/SOL                      btc-updown-5m-* (rolling)
 aggTrade · bookTicker · depth20   price_change · book · last_trade_price
        │                                       │
        ▼                                       ▼
 binance_producer.py            polymarket_producer.py
 (reconexión + backoff exp.)    (modo rolling + backoff exp.)
        │                                       │
        └──────────────┬────────────────────────┘
                       ▼
            Kafka 3.9 (KRaft, Docker)
         ┌─────────────────────────────────┐
         │  binance.trades    (3 parts.)   │
         │  binance.book      (3 parts.)   │
         │  polymarket.events (3 parts.)   │
         └─────────────────────────────────┘
                       │
                       ▼
            spark_stream.py  (PySpark 3.5 local[*])
         ┌──────────────────────────────────────────┐
         │  parse envelope → extrae price events    │
         │  stats por ventana: min/max/avg/var       │
         │  forecast: BTC_mid + (2·P_up − 1)·σ/1s  │
         └──────────────────────────────────────────┘
              │               │              │
              ▼               ▼              ▼
        data/ticks/     stats.windowed   btc.forecast
        (Parquet        (Kafka topic)    (Kafka, 1 part.)
         source/date/                         │
         hour)                                ▼
              │                         Grafana 11.3
              ▼                         BTC × Polymarket live
        build-dataset
        train-model
              │
              ▼
        SARIMAX(2,0,2) — data/model.sarimax.pkl
```

---

## Stack

| Capa | Tecnología |
|---|---|
| Ingestión | Python 3.11 · asyncio · websockets · aiohttp |
| Broker | Apache Kafka 3.9 (KRaft, sin Zookeeper) |
| Stream processing | PySpark 3.5 · spark-sql-kafka-0-10_2.12 |
| Persistencia | Parquet (particionado por source/date/hour) |
| Dashboard | Grafana 11.3 · kafka-datasource plugin |
| Modelo | statsmodels SARIMAX · scikit-learn · pyarrow |
| Infra | Docker Compose (Kafka + Kafka UI + Grafana) |

---

## Estructura del repositorio

```
.
├── common/                      # Librería compartida entre todos los componentes
│   ├── envelope.py              #   Esquema canónico de evento {source,type,recv_ts,payload}
│   ├── kafka_sink.py            #   Wrapper async sobre aiokafka producer
│   ├── metrics.py               #   RateTracker: tasa de mensajes en ventana deslizante
│   ├── ssl_ctx.py               #   Contexto SSL con certifi (macOS + Linux + Windows)
│   ├── log.py                   #   Helper de progreso para Rich console
│   └── win_console.py           #   Parche UTF-8 + asyncio event loop (Windows)
├── producers/
│   ├── binance_producer.py      #   Binance WS → binance.trades + binance.book
│   ├── polymarket_producer.py   #   Polymarket CLOB WS → polymarket.events
│   └── polymarket_discovery.py  #   Gamma REST API: fetch_top_markets, fetch_updown_market
├── consumers/
│   └── spark_stream.py          #   Spark job: parse → stats → forecast → Parquet + Kafka
├── modeling/
│   ├── build_dataset.py         #   Parquet ticks → 1-min snapshots → training.parquet
│   └── train_model.py           #   AIC grid search + walk-forward SARIMAX → model.sarimax.pkl
├── tools/
│   └── throughput_probe.py      #   Validador de throughput Kafka (reporte msg/s por topic)
├── scripts/
│   ├── run-overnight.sh         #   Arranca toda la stack en background + caffeinate
│   └── stop-overnight.sh        #   Detiene procesos por PID y Docker
├── infra/
│   ├── docker-compose.yml       #   Kafka · kafka-init · Kafka UI · Grafana
│   └── grafana/                 #   Dashboards + datasources provisionados
├── data/
│   ├── training.parquet         #   707 snapshots de 1-min (features + target)
│   ├── model.sarimax.pkl        #   SARIMAXResults entrenado
│   └── feature_list.json        #   Orden (2,0,2) · exog · σ_log · AIC grid
├── docs/
│   └── phase-5-report.md        #   Métricas de holdout + decisiones de diseño
├── ws_live.py                   #   Explorador interactivo del CLOB (sin Kafka)
├── bitcoin_5m.py                #   Feed interactivo de mercados Up-or-Down
└── pyproject.toml
```

---

## Setup

```bash
git clone <repo>
cd proyecto

python -m venv .venv
source .venv/bin/activate         # Windows: .venv\Scripts\activate

# Solo ingestión (producers + tools)
pip install -e .

# Pipeline completo (Spark + ML)
pip install -e ".[all]"
```

> **Nota pyspark:** el venv debe tener `pyspark>=3.5,<4`. El conector Kafka es
> `spark-sql-kafka-0-10_2.12:3.5.1` (Scala 2.12); pyspark 4.x usa Scala 2.13 → incompatible.
>
> ```bash
> pip show pyspark | grep Version   # debe decir 3.5.x
> ```

---

## Correr el pipeline

### 1 — Infraestructura

```bash
cd infra && docker compose up -d && cd ..
```

`kafka-init` se ejecuta automáticamente cuando Kafka está healthy y crea los 5 topics. No hay paso manual.

| URL | Servicio |
|---|---|
| `http://localhost:3000` | Grafana (admin/admin) |
| `http://localhost:8080` | Kafka UI |
| `http://localhost:4040` | Spark UI (solo cuando spark-stream está corriendo) |

> Grafana tarda ~8 s en el primer arranque mientras descarga el plugin `hamedkarbasi93-kafka-datasource`.

### 2 — Producers (terminales separadas)

```bash
# Terminal A — Binance: BTC/ETH/SOL aggTrade + bookTicker + depth20
binance-producer

# Terminal B — Polymarket: BTC Up-or-Down 5-min (rolling, default)
polymarket-producer

# Variantes del producer Polymarket
polymarket-producer --asset eth --window 15     # ETH 15-min
polymarket-producer --query bitcoin --top 10   # top-10 mercados Bitcoin
polymarket-producer --all                       # top-100 sin filtro
```

Ambos producers tienen reconexión automática con backoff exponencial (1 s → 60 s cap, reset a 1 s si la conexión duró >30 s).

### 3 — Spark Structured Streaming + Live Scorer

```bash
# Terminal C
spark-stream                                   # solo Parquet + Kafka sinks
spark-stream --console                         # también imprime stats en consola
spark-stream --window "2 minutes" --watermark "1 minute"

# Terminal D (requiere data/model.sarimax.pkl — ejecutar train-model primero)
score-stream                                   # SARIMAX live → btc.sarimax-forecast
score-stream --debug                           # verbose: muestra cada bar cerrado
```

Spark escribe tres sinks en paralelo:
- **Parquet** en `data/ticks/` (particionado por `source/date/hour`)
- **Kafka** topic `stats.windowed` (estadísticas por ventana — consumido por Grafana)
- **Kafka** topic `btc.forecast` (BTC mid + Polymarket-implied price por segundo — consumido por Grafana)

### 4 — Recolección overnight (desatendida)

```bash
# Arranca toda la stack en background y evita que el laptop duerma
./scripts/run-overnight.sh

# Ver logs en vivo
tail -f data/logs/{binance,polymarket,spark}.log

# Detener todo
./scripts/stop-overnight.sh
```

El script escribe los PIDs en `data/run.pids` y usa `caffeinate -d` (macOS) para mantener el laptop activo. La recolección de una noche (~12 h) produce ~7 GB de ticks Parquet.

### Parar y reiniciar

```bash
# Parar (preserva volumes — topics y datos Grafana persisten)
cd infra && docker compose stop && cd ..

# Reiniciar (no recrea kafka-init, los topics existen)
cd infra && docker compose start && cd ..

# Si borraste volumes (down -v): usar up -d para que kafka-init recree topics
cd infra && docker compose up -d && cd ..
```

---

## Modelado

### Construir el dataset

```bash
build-dataset
# Opciones
build-dataset --ticks-path data/ticks --output data/training.parquet --label-horizon-min 5
```

Lee los Parquet de `data/ticks/`, resamplea BTC mid (bookTicker) y Polymarket P(up) a una grilla de 1 segundo, luego los proyecta sobre snapshots de 1 minuto. El target es `log_return_5m = log(BTC_mid_{T+5} / BTC_mid_T)` — regresión continua, no clasificación binaria.

**Features:**

| Feature | Descripción |
|---|---|
| `poly_p_up` | Polymarket P(BTC sube en la ventana de 5 min) |
| `poly_p_up_change_5m` | Delta de `poly_p_up` en los últimos 5 min |
| `btc_volatility_5m` | σ del log-return de BTC en los últimos 5 min |

Ignora automáticamente archivos de 0 bytes (batches crasheados de Spark).

### Entrenar el modelo

```bash
train-model
# Opciones
train-model --max-p 3 --max-q 3 --holdout-frac 0.2
```

1. **Selección de orden:** AIC grid search sobre p, q ∈ {0..max_p} × {0..max_q} (excluye ruido blanco puro).
2. **Validación:** walk-forward 1-step-ahead usando `fit.append(refit=False)` — fit una vez sobre train, actualiza estado del filtro de Kalman en cada paso. ~30 s en total.
3. **Salida:** `data/model.sarimax.pkl` + `data/feature_list.json` (orden, lista de exog, σ_log, AIC grid completo).

**Métricas de holdout (SARIMAX(2,0,2), 707 rows):**

| Serie | RMSE (bps) | MAE (bps) | dir_acc |
|---|---|---|---|
| **Modelo · holdout** | **2.45** | **1.92** | **85.1%** |
| Polymarket · holdout | 5.36 | 4.21 | 58.2% |
| Zero-baseline · holdout | 4.53 | 3.64 | — |

El output del modelo vive en el mismo eje que la línea de Polymarket-implied en Grafana (`forecast = BTC_mid + (2·P_up − 1) · σ_log`), permitiendo comparación visual directa.

---

## Exploradores interactivos

```bash
# Live feed del CLOB (sin Kafka, visualización directa)
python ws_live.py                        # top-20 mercados por volumen
python ws_live.py --query bitcoin        # filtrar por keyword
python ws_live.py --query trump --top 5  # top-5 Trump markets
python ws_live.py --all                  # top-100 sin filtro

# Feed de mercados Up-or-Down (detecta ventana activa automáticamente)
python bitcoin_5m.py                     # BTC 5-min
python bitcoin_5m.py --asset eth         # ETH 5-min
python bitcoin_5m.py --window 15         # BTC 15-min
```

---

## Herramientas

### Throughput probe

```bash
throughput-probe                    # prueba de 10 minutos (default)
throughput-probe --duration 60      # prueba rápida de 1 minuto
```

Consume los 3 topics de ingestión y reporta msg/s por topic, distribución de tipos de evento, y veredicto contra el umbral de 4,000 msg/s. Requiere producers corriendo.

---

## Kafka topics

| Topic | Particiones | Contenido |
|---|---|---|
| `polymarket.events` | 3 | price_change · book · last_trade_price del CLOB |
| `binance.trades` | 3 | aggTrade (precio y cantidad de cada trade) |
| `binance.book` | 3 | bookTicker (best bid/ask) + depth20 snapshots |
| `stats.windowed` | 3 | Estadísticas por ventana: min/max/avg/var/n |
| `btc.forecast` | 1 | BTC mid + Polymarket-implied forecast (1/s, emitido por Spark) |
| `btc.sarimax-forecast` | 1 | BTC mid + SARIMAX(2,0,2) predicted price (1/min, emitido por score_stream.py) |

Todos los mensajes siguen el envelope canónico definido en `common/envelope.py`:

```json
{
  "source":   "binance | polymarket",
  "type":     "aggTrade | bookTicker | price_change | ...",
  "recv_ts":  1234567890.123,
  "symbol":   "btcusdt",
  "asset_id": "...",
  "market":   { "question": "...", "slug": "...", "outcome": "Up | Yes" },
  "payload":  { ... }
}
```

---

## Fuentes de datos

| API | URL | Uso |
|---|---|---|
| Gamma REST | `https://gamma-api.polymarket.com` | Descubrimiento de mercados activos y sus `clobTokenIds` |
| CLOB WebSocket | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Stream en tiempo real de order book y trades |
| Binance WebSocket | `wss://stream.binance.com:9443/stream` | aggTrade · bookTicker · depth20 para BTC/ETH/SOL |

### Mercados Up-or-Down

Los mercados de 5 minutos rotan con cada ventana. El slug sigue el patrón `{asset}-updown-{window}m-{unix_ts}` donde el timestamp es siempre un múltiplo exacto de `window × 60`:

```bash
TS=$(python3 -c "import time; w=300; print((int(time.time())//w)*w)")
curl -s "https://gamma-api.polymarket.com/markets?slug=btc-updown-5m-$TS"
```

Activos disponibles: `btc`, `eth`, `sol`, `xrp`, `bnb`, `doge`, `hype`.

---

## Notas de diseño

- **Envelope único:** `common/envelope.py` define el schema que Spark parsea en `ENVELOPE_SCHEMA`. Un solo schema cubre todos los topics, sin lógica de dispatch por fuente en el job de Spark.
- **Partición Kafka por asset_id / symbol:** garantiza orden dentro de un mercado y paralelismo entre mercados.
- **Watermark en Spark:** los eventos CLOB tienen timestamps de servidor (Polygon blockchain) que pueden llegar desordenados. `withWatermark("ts_event", "30 seconds")` para el sink de stats; 10 s para el forecast de 1 s.
- **pyspark < 4:** el conector `spark-sql-kafka-0-10_2.12` es la build Scala 2.12. pyspark 4.x usa Scala 2.13 → incompatible. Fijado en `pyproject.toml`.
- **caseSensitive = true en Spark:** los campos de bookTicker de Binance son de una letra y case-sensitive (`b`/`B` para bid price/qty, `a`/`A` para ask). Sin esta config, `from_json` + `col()` lanza `AMBIGUOUS_REFERENCE_TO_FIELDS`.
- **SARIMAX vs LightGBM:** con ~700 rows de entrenamiento, LightGBM (IID, sin estructura temporal) sobreajusta. SARIMAX(p,0,q) modela autocorrelación AR+MA del retorno y usa Polymarket P(up) + volatilidad como exógenos — el fit correcto para esta escala.
- **Walk-forward con append(refit=False):** refitear SARIMAX en cada paso del holdout toma 20+ min. `append(refit=False)` actualiza el estado del filtro de Kalman sin reoptimizar parámetros — estándar en producción, ~30 s total.
- **Índice entero en SARIMAX:** `append()` requiere índice temporalmente contiguo; los snapshots tienen gaps por `dropna`. Se resetea a índice entero antes de pasar a statsmodels.
- **σ_log en feature_list.json:** `score_stream.py` lo usa para reportar el log-return en bps junto al forecast de precio.
- **btc.sarimax-forecast:** topic nuevo de 1 partición emitido por `score_stream.py`. Schema: `{ts, btc_mid, poly_prob, log_return_pred, sarimax_forecast, sigma_log}`. Cold start: primeras ~10 min sin forecasts mientras acumula historia.
- **train_model.py usa `final_fit.save()`:** formato statsmodels nativo. Compatible con archivos generados por versiones anteriores del script — `sm.load()` y `pickle.load()` leen el mismo formato.

---

## Comparación de arquitecturas (Fase 6)

| | Mac M-series (CPU) | Windows RTX 2060 (GPU) |
|---|---|---|
| Spark mode | `local[*]` | `local[*]` (mismo código) |
| ML inference | statsmodels SARIMAX + 3 exog | cuML ARIMA + 3 exog |
| ML training | statsmodels (~30 s) | cuML ARIMA + 3 exog en GPU |
| Entorno | macOS · .venv | Windows + WSL2 + conda RAPIDS 24.x |

```bash
# CPU (Mac) — sin dependencias extra
python scripts/benchmark_inference.py

# GPU (Windows WSL2) — requiere RAPIDS/cuML con ARIMA exógeno.
# Genera el comando exacto para tu driver/CUDA en: https://docs.rapids.ai/install/
python scripts/benchmark_inference.py --gpu

# GPU training comparison
python modeling/train_model_cuml.py
```

> **Nota cuML**: si `--gpu` falla indicando que `exog=` no está soportado, hay que
> actualizar RAPIDS/cuML a una versión con ARIMA exógeno antes de usar los números
> en el reporte. La comparación esperada usa la misma `y`, mismos exógenos y mismo orden.
