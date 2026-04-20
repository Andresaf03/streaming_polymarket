# Phase 3 — Live BTC × Polymarket dashboard

**Branch:** `feat/phase-3-grafana-dashboards`
**Status:** working end-to-end; dashboard layout is a first pass, not final.

---

## What we delivered

### New live forecast (Spark Structured Streaming)
- `consumers/spark_stream.py::forecast_stats()` — a second streaming query that
  joins Binance BTC mid + Polymarket BTC 5-min `P(up)` inside a single 1-second
  `groupBy` (conditional aggregation; no stream-stream join needed).
- Computes `implied target = BTC_mid + (2·P_up − 1) × σ` where σ is the
  hardcoded 5-minute BTC return stddev (default `$50`, `--sigma` flag-overridable;
  will move to a rolling estimate in Phase 5).
- Writes one record per second to new Kafka topic `btc.forecast`.

### New Grafana dashboard
- `infra/grafana/dashboards/btc-live.json` — auto-provisioned via
  `infra/grafana/provisioning/`. Loads on `docker compose up`, no manual clicks.
- Kafka datasource (`hamedkarbasi93-kafka-datasource`) wired to `kafka:29092`.
- Current panels:
  1. Four stat cards (BTC mid, P(up), σ, implied drift)
  2. Main chart: BTC mid (solid white) + implied 5-min target (dotted blue),
     both on the $ axis
  3. Polymarket P(up) timeseries with a 0.5 reference line

### Polymarket producer — auto-rediscovery
- `polymarket_producer.py` now has two modes:
  - **Static** (`--query` / `--all` / default): one discovery + one long-lived
    WebSocket, unchanged behavior.
  - **Rolling** (`--asset btc --window 5`): each cycle re-fetches the active
    market, resubscribes, and streams until `slug_window_end + 5s` via
    `asyncio.wait_for`. The outer loop then rediscovers the next window.
- Fixes the "dashboard goes dark every 5 min" bug that surfaced during smoke
  test.

### Cleanup landed alongside (from architecture audit)
- `log_progress()` deduplicated to `common/log.py` and re-exported from
  `common/__init__.py`. Both producers now share one implementation.
- `forecast_stats` refactored to consume the normalized `events` DataFrame
  instead of the raw `parsed` one — price extraction lives in exactly one
  place (`extract_price_events`).
- `extract_price_events` now projects `market_slug` and `market_outcome` as
  top-level columns, making the forecast filter readable.
- New CLI flags on `spark-stream`: `--forecast-watermark`,
  `--forecast-slug-prefix`. The slug prefix is no longer hardcoded, so a
  15-min variant is a flag change instead of a code change.
- Polymarket envelopes now carry `market.outcome` ("Yes"/"No" or "Up"/"Down").
  `polymarket_discovery.build_asset_map` parses the Gamma `outcomes` array.

---

## Bugs hit during development

| # | Symptom | Root cause | Fix |
|---|---|---|---|
| 1 | `AMBIGUOUS_REFERENCE_TO_FIELDS` on `payload.b` | Binance uses single-letter case-sensitive fields (`b`/`B`/`a`/`A`); Spark SQL defaults to case-insensitive resolution | `spark.sql.caseSensitive=true` in the session config |
| 2 | `btc.forecast` emitted only `btc_mid` + `sigma`, never `poly_prob` | Filter expected `outcome == "Yes"` but BTC up/down markets use `"Up"/"Down"` | Accept both via `.isin("Up", "Yes")` |
| 3 | `poly_prob` goes dark ~5 min after startup | Producer subscribes to asset IDs at launch; those IDs belong to a specific 5-min window, and the market closes at window end | Auto-rediscovery loop (see above) |
| 4 | Grafana dashboard showed "No data" despite Spark publishing | Forecast topic auto-created with 3 partitions; messages round-robined; the Kafka plugin queries a single partition (0) per target | Spark sink now pins `partition = 0` for forecast records |
| 5 | Plugin kept erroring "context deadline exceeded" after the topic was deleted and recreated | Plugin's consumer held stale partition state | One-time Grafana container restart; won't recur now that we don't re-create the topic |
| 6 | Main chart unreadable — `drift` series (−$30) rendered on the same $ axis as `btc_mid` ($75k), compressing BTC to a flat line | `hideFrom` overrides didn't prevent Grafana from autosizing to the drift range | Switched to explicit `filterFieldsByName` transforms per panel — each panel now keeps only the fields it needs |

---

## Known limitations (by design, not bugs)

- **Dashboard is a first pass.** Layout, colors, panel choices all
  revisitable. The implied-target line is compelling but the UX is not yet
  tuned for a demo.
- **σ is hardcoded at $50.** Phase 5 will replace with a rolling estimate
  from the training pipeline.
- **No model prediction line yet.** Phase 6 adds a third line to the main
  chart once the LightGBM scorer is writing to Kafka.
- **No calibration plot, no edge signal panel.** These live better in a
  Jupyter notebook / Streamlit page than in Grafana — per our earlier
  discussion.
- **Grafana Kafka plugin quirks.** If the forecast topic is ever recreated
  (e.g. rebuilding the whole stack from scratch), Grafana may need one
  restart to drop stale consumer state.
- **Single-broker Kafka, single-partition forecast.** Fine for a class
  project; wouldn't fly in production.

---

## Files changed in this phase

```
common/log.py                                      new
common/__init__.py                                 edit (export log_progress)
producers/polymarket_discovery.py                  edit (parse outcomes)
producers/polymarket_producer.py                   edit (auto-rediscover, use log_progress)
producers/binance_producer.py                      edit (use log_progress)
consumers/spark_stream.py                          edit (forecast_stats, sinks, flags)
infra/docker-compose.yml                           edit (allow unsigned plugin)
infra/grafana/provisioning/datasources/kafka.yaml  new
infra/grafana/provisioning/dashboards/dashboards.yaml  new
infra/grafana/dashboards/btc-live.json             new
docs/phase-3-report.md                             new (this file)
```

---

## How to run (quick reference)

```bash
# once per machine
python -m venv .venv && source .venv/bin/activate
pip install -e '.[spark]'

# bring up infra
docker compose -f infra/docker-compose.yml up -d

# three processes in separate terminals
binance-producer
polymarket-producer --asset btc --window 5
spark-stream --sigma 50 \
  --output-path data \
  --checkpoint-path data/checkpoints/spark

# open dashboard
open http://localhost:3000/d/btc-live
```

Grafana uses anonymous viewer access — no login required for read.

---

## Next

- **Phase 4** or **Phase 5**: pick up model training on the accumulated
  Parquet ticks. Once the trainer produces a calibrated probability, the
  dashboard gets a third line for free.
- Optional cleanup left over from the architecture audit (low severity):
  `recv_ts` vs `ts` naming; `RateTracker.maxlen` vs `window_seconds`
  semantics; promoting the sink list to a config-driven structure to
  unblock Phase 6's multiple scorers.
