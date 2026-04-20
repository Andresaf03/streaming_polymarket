# Tentative Plan v2 — Polymarket Streaming Project

**Branch:** `dev/manu-exploration`
**Last updated:** 2026-04-20
**Supersedes:** README.md "Fases del proyecto" table

---

## Goals

1. Satisfy the rubric of Arquitectura de Grandes Volúmenes de Datos (Prof. Pereira).
2. Build something worth putting on a CV: real-time crypto prediction-market signal with cross-source features, calibration analysis, and anomaly detection.

Order of priority: rubric first, CV second. Every CV extension must also satisfy a rubric item.

---

## Data sources

| Source | Protocol | Auth | Expected rate | Role |
| --- | --- | --- | --- | --- |
| Polymarket Gamma REST | HTTPS | None | N/A (metadata fetch) | Discover active `{asset}-updown-{window}m-*` markets |
| Polymarket CLOB WebSocket | WSS | None | ~500-2k ev/s aggregate | Market-implied probability + order book |
| Binance Public WebSocket (btcusdt, ethusdt, solusdt) | WSS | None | ~1.5-3k ev/s aggregate | Spot microstructure (truth signal) |

**Target sustained throughput:** ≥4,000 msg/s combined — matches the EEG cadence baseline in the rubric. Verified from Mexico (Binance reachable, 260ms latency, no geo-block).

---

## Architecture

### Primary (local)

```
┌──────────────────────┐   ┌──────────────────────┐
│ polymarket_producer  │   │ binance_producer     │  Python asyncio
└──────────┬───────────┘   └──────────┬───────────┘
           │                          │
           └──────────┬───────────────┘
                      ▼
            ┌───────────────────┐
            │ Kafka (KRaft)     │  single broker, Docker
            │  polymarket.events │
            │  binance.trades    │
            │  binance.book      │
            └─────────┬─────────┘
                      ▼
         ┌────────────────────────────┐
         │ Spark Structured Streaming │  pyspark local[*]
         │  - windows + watermarks    │
         │  - min/max/mean/var        │
         └──────┬──────────────┬──────┘
                ▼              ▼
         ┌───────────┐  ┌───────────────┐
         │ Parquet   │  │ Kafka topic   │
         │ (ticks/)  │  │ stats.windowed │
         └─────┬─────┘  └───────┬───────┘
               │                ▼
               │         ┌──────────────┐
               │         │ Grafana Live │  sub-second push
               │         └──────────────┘
               ▼
        ┌─────────────────┐
        │ train_model.py  │  LightGBM, offline
        │  target: BTC ↑  │  features: Binance CVD,
        │  in next window │  OB imbalance, Poly mid
        └────────┬────────┘
                 ▼
        ┌─────────────────┐
        │ score_stream.py │  load model, stream-score
        │                 │  → predictions topic
        └────────┬────────┘
                 ▼
            Grafana panel:
            model prob vs market prob
            (edge signal)
```

### Secondary (remote) — EMR Serverless

Same PySpark job submitted via `aws emr-serverless start-job-run`. Kafka sourced from Confluent Cloud free tier (outbound from EMR, no VPC peering needed). Parquet sinks to S3 instead of local disk. Spark UI via EMR Studio.

### Tertiary (optional) — Databricks AWS Marketplace trial

If time and motivation remain after EMR Serverless is working: import the same job into a Databricks notebook, rerun, collect Spark UI. Strongest CV signal of the three, weakest ROI on time.

---

## Code discipline

Every script that runs on all three architectures takes the same CLI/env contract:

```
--kafka-bootstrap   $KAFKA_BOOTSTRAP   # default: localhost:9092
--output-path       $OUTPUT_PATH       # default: data/ticks/          (matches .gitignore)
--checkpoint-path   $CHECKPOINT_PATH   # default: data/checkpoints/spark/ (matches .gitignore)
```

No hardcoded paths. No hardcoded broker addresses. Enforced from commit #1.

---

## Rubric mapping

| Rubric requirement | Component |
| --- | --- |
| Spark Structured Streaming + Kafka | `spark_stream.py` consuming from Kafka topics |
| Fuente ≥4k msg/s | Polymarket + Binance combined (probe validates) |
| Ventanas + watermarks | `window($"ts", "5 minutes", "1 minute")` + `withWatermark("ts", "30 seconds")` |
| Estadísticos por rango (min/max/μ/σ²) | Aggregations keyed by bucketed price range |
| Persistencia a disco | Parquet partitioned by `date=/hour=` |
| Modelo supervisado | `train_model.py` — LightGBM, target = BTC direction in next 5-min window |
| Scoring en stream | `score_stream.py` — loads model, `foreachBatch` predict, writes to `predictions` topic |
| ≥2 arquitecturas + Spark UI | Local MacBook + EMR Serverless, screenshots of Jobs/Stages/Streaming tabs |
| Visualización animada | Grafana Live, sub-second refresh. Defensible under "pueden usar PowerBI/Tableau/Qlik" (permissive phrasing) — Grafana is industry-equivalent and runs on macOS, which PowerBI Desktop does not |
| Tabla hardware comparativa | MacBook M-series vs EMR Serverless workers |
| ≥2 métricas Spark UI | Input/processing rate, batch duration, shuffle read/write, executor run time |

## CV extensions (on top of rubric)

| Extension | Satisfies rubric item | Marginal cost |
| --- | --- | --- |
| Calibration plot — empirical outcome frequency vs Polymarket implied probability, bucketed | Counts as an "indicador estadístico" | ~2 hrs |
| Edge signal — LightGBM prob vs Polymarket prob, flag when \|edge\| > threshold | Counts as "scoring en tiempo real" | ~0 hrs extra (already scoring) |
| DBSCAN/LOF anomaly detection on order-book features | The rubric explicitly mentions STREAM-LOF, OLOF, DBSCAN as valid non-supervised online techniques | ~1 day |

---

## Tooling decisions

| Question | Decision | Why |
| --- | --- | --- |
| Streaming broker | Kafka (KRaft mode, no Zookeeper) | Rubric requires Kafka; KRaft is the 2026 default |
| Spark API | Structured Streaming (DataFrame) | Matches class notes; `window` + `withWatermark` shown in lecture |
| Sink format | Parquet | Matches class notes. Defer Delta Lake unless live dashboard shows read/write race conditions |
| Aggregate delivery to dashboard | Kafka topic → Grafana Kafka plugin | Avoids a separate time-series DB. One fewer service |
| Dashboard | Grafana OSS (Docker) | Free, sub-second push, runs on macOS, industry-standard for ops/trading dashboards |
| ML library | LightGBM primary; MLlib optional bonus | CV value. Class notes teach MLlib but rubric accepts any supervised learner |
| Docker | docker-compose for Kafka + Grafana | Reproducibility. Professor's notes don't show Docker but don't forbid it |
| Secondary arch | EMR Serverless (~$15-20, 2h setup, full Spark UI) | Cheapest genuinely-distributed option with AWS brand signal |
| Tertiary arch | Databricks Trial via AWS Marketplace, only if time allows | Strongest CV brand. Requires own AWS account, ~$5-10 EC2 cost, DBUs covered by $400 trial credit |

## Tooling explicitly rejected

- **PowerBI** — does not exist on macOS; Desktop refresh floor is 1 minute on non-Premium
- **TimescaleDB** — added complexity (extra Docker service) for no measurable benefit; Kafka plugin into Grafana is simpler
- **Streamlit** — redundant with Grafana for v1; revisit only if presentation demands a dedicated model-scoring page
- **Plotly Dash** — not listed in the rubric's permitted viz tools; Grafana is a safer defense
- **Delta Lake / Iceberg / Hudi** — overkill at our volume (~1-5 GB columnar). Flip config only if concurrent-reader issues appear
- **EMR on EC2** — ~5h setup vs 2h for EMR Serverless, same CV value
- **Dataproc Serverless** — technically cheapest distributed option but weaker AWS-ecosystem signal; defer unless EMR Serverless hits a blocker

---

## Phase breakdown

### Phase 1 — Local producers + Kafka (1-2 days)

- [ ] `infra/docker-compose.yml` — Kafka KRaft single broker + kafka-ui + Grafana
- [ ] `producers/binance_producer.py` — asyncio WS client for `btcusdt|ethusdt|solusdt` × `aggTrade|bookTicker|depth20@100ms`, publishes to Kafka
- [ ] `producers/polymarket_producer.py` — evolved from `ws_live.py` + `bitcoin_5m.py`. Same Gamma REST discovery, same CLOB WebSocket subscribe/decode logic. Adds Kafka publisher; Rich console becomes `--debug` flag. Market-selection flags merged: `--query` (top-N keyword), `--all` (top-100), `--asset btc --window 5` (rotating up-down market)
- [ ] `tools/throughput_probe.py` — subscribes to both sources for 10 minutes, reports sustained msg/s and distribution by event type
- [ ] End-to-end smoke test: producers → Kafka → `kafka-console-consumer` prints events

**Exit criterion:** sustained ≥4k msg/s over 10 minutes, both producers running stably, topics visible in kafka-ui.

### Phase 2 — Spark Structured Streaming (2-3 days)

- [ ] `spark_stream.py` — reads all three topics, parses JSON, unions events, windowed aggregations (1-min tumbling + 5-min sliding)
- [ ] Watermark = 30 seconds
- [ ] Sinks: Parquet to `data/ticks/` partitioned by `date=/hour=`; Kafka topic `stats.windowed` for Grafana
- [ ] Min/max/mean/var by bucketed price range for BTC
- [ ] Checkpoint path configured

**Exit criterion:** job runs stably for 30 min, Parquet files appear, stats topic receives updates every batch.

### Phase 3 — Grafana dashboard (0.5-1 day)

- [ ] Grafana provisioned with Kafka datasource plugin (`grafana-kafka-datasource`)
- [ ] Panels: msg/s per topic, BTC last price (Binance), Polymarket BTC-up probability, order book imbalance, windowed stats
- [ ] Auto-refresh 1s
- [ ] Dashboard JSON committed under `infra/grafana/dashboards/`

**Exit criterion:** live demo runs for 10 min without visible read/write jitter.

### Phase 4 — Data collection (0.5 day + wall time)

- [ ] Run all producers + Spark job for ≥4 hours during a BTC-active window (US trading hours)
- [ ] Accumulate ≥2M events in Parquet
- [ ] Snapshot the data (backup copy outside `data/`)

### Phase 5 — Model training (1 day)

- [ ] `train_model.py` — reads Parquet, constructs feature matrix per 1-min row: Binance CVD, OB imbalance, spread, 5-min return; Polymarket mid, spread, depth
- [ ] Target: BTC 5-min return direction (binary) from Binance mid
- [ ] LightGBM training, holdout split by time
- [ ] Save `model.lgb` + feature list
- [ ] **Calibration plot** — empirical outcome vs Polymarket implied probability, 10 buckets

### Phase 6 — Real-time scoring (1 day)

- [ ] `score_stream.py` — loads model, consumes live features from Kafka, predicts per batch, writes to `predictions` topic
- [ ] Grafana panel: model prob vs market prob, edge = model - market
- [ ] Alert threshold for \|edge\| > 0.05

### Phase 7 — CV bonus: anomaly detection (1 day, optional)

- [ ] DBSCAN or LOF on rolling window of OB features
- [ ] Emit anomalies to `anomalies` topic
- [ ] Grafana annotation layer on main panels

### Phase 8 — EMR Serverless (1-2 days)

- [ ] Confluent Cloud free tier — create cluster, create same topics, point producers at it for one short run
- [ ] S3 bucket for Parquet sink
- [ ] Package `spark_stream.py` + deps as a zip, upload
- [ ] EMR Serverless application — submit job
- [ ] Collect Spark UI screenshots: Jobs, Stages, Streaming, Environment, Executors
- [ ] Hardware table + 2-metric comparison

### Phase 9 — Informe + video (1-2 days)

- [ ] Informe with architecture diagrams, hardware table, Spark UI screenshots, calibration plot, edge analysis
- [ ] 10-min video demo: Grafana ticking + model edge signal
- [ ] Update `README.md` — phases, viz decision, final stack

### Phase 10 — Databricks (optional, 1 day)

Only if Phases 1-9 are green and there's still calendar room.

---

## Cost estimate

| Item | Cost |
| --- | --- |
| Local stack (Docker, Python, Spark, Grafana) | $0 |
| Polymarket + Binance data | $0 |
| Confluent Cloud free tier (for EMR run) | $0 within $400 trial |
| EMR Serverless (~1-2 hours of job time total) | $15-20 |
| S3 storage (~5 GB, short-lived) | <$1 |
| Databricks (optional) | $5-10 EC2 if Marketplace path |
| **Ceiling (EMR only)** | **~$20** (~$10 each) |
| **Ceiling (EMR + Databricks)** | **~$30** (~$15 each) |

---

## Risks & mitigations

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Polymarket throughput below 4k/s | Medium | Medium | Probe early (Phase 1). If short, broaden Binance subscriptions (more symbols × more streams) |
| Binance geo-blocked | Low | High | Already validated reachable from Mexico |
| Kafka KRaft config fragile | Low | Low | Use Confluent's reference docker-compose image |
| Spark SS checkpoint corruption | Low | Medium | Use a clean checkpoint dir per run; document restart procedure |
| EMR Serverless first-job setup pain | Medium | Medium | Budget a full day. Use CloudShell to avoid local AWS CLI drift |
| Trial clock on EMR — 14-day Confluent limit | Medium | Low | Run EMR experiments in one concentrated session, not spread across weeks |
| Informe + video underbudgeted | High | High | Treat Phase 9 as its own week |

---

## Open questions

- Do we want to run both Polymarket and Binance producers on the **same** local machine, or split across two MacBooks? Same machine is simpler; split would let us quote a more realistic hardware table.
- Does the professor want a specific Spark version? Default to 3.5.x unless he specifies.
- Does "animación en tiempo real" require a canned recording in the informe, or is live demo during the presentation enough?

---

## Out of scope (explicitly)

- Placing actual trades on Polymarket (requires L2 signed orders and a funded Polygon wallet — legal + financial scope explosion)
- Multi-cluster Kafka
- Schema registry / Avro
- CI/CD
- Unit tests beyond smoke tests
- Any production-grade observability (Prometheus, OpenTelemetry)
