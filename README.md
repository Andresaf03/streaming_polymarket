# Polymarket Real-Time Streaming

**Arquitectura de Grandes Volúmenes de Datos — ITAM 2026**

Sistema de streaming en tiempo real sobre los mercados de predicción de [Polymarket](https://polymarket.com), usando su API CLOB (Central Limit Order Book) como fuente de datos de alta frecuencia.

***

## Motivación

Polymarket opera como un exchange de contratos binarios sobre Polygon (blockchain). El CLOB emite eventos de order book en tiempo real: actualizaciones de bids/asks, cambios de precio y trades ejecutados. En mercados líquidos (Bitcoin, elecciones, Fed) puede superar los 4,000 eventos/segundo agregados entre tokens, equivalente a la cadencia de los cascos EEG del ITAM (4,096 lecturas/s).

***

## Arquitectura objetivo

```
Polymarket CLOB WebSocket
         │
         ▼
      Kafka                ← broker de mensajes, retención configurable
         │
         ▼
  Spark Structured         ← ventanas deslizantes, watermarks
  Streaming (PySpark)      ← estadísticas en tiempo real
         │
    ┌────┴────┐
    ▼         ▼
  Parquet   Dashboard      ← persistencia + visualización (PowerBI/Plotly)
  (fase 1)
    │
    ▼
  Modelo supervisado       ← entrenado sobre datos acumulados
  (LightGBM / LogReg)
    │
    ▼
  Clasificación en         ← scoring sobre el stream activo
  tiempo real
```

***

## Fases del proyecto

| Fase | Descripción                                        | Estado |
| ---- | -------------------------------------------------- | ------ |
| 0    | Validación WebSocket — este repo                   | ✅      |
| 1    | Kafka producer + consumer básico                   | ⬜      |
| 2    | Spark Structured Streaming + ventanas              | ⬜      |
| 3    | Estadísticas / clustering online (K-Means, DBSCAN) | ⬜      |
| 4    | Entrenamiento modelo supervisado                   | ⬜      |
| 5    | Scoring en tiempo real                             | ⬜      |
| 6    | Comparación de arquitecturas (local vs AWS/Colab)  | ⬜      |

***

## Fase 0: Validación rápida

### Setup

```bash
cd proyecto/
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Correr el live feed

```bash
# Top-20 mercados por volumen (sin filtro)
python ws_live.py

# Mercados de Bitcoin
python ws_live.py --query bitcoin

# Top-5 mercados de Trump
python ws_live.py --query trump --top 5

# Top-100 sin filtro
python ws_live.py --all
```

### Mercado de alta frecuencia (Up or Down 5 min)

```bash
# BTC 5-min (detecta la ventana activa automáticamente)
python bitcoin_5m.py

# Variantes
python bitcoin_5m.py --asset eth
python bitcoin_5m.py --asset sol
python bitcoin_5m.py --window 15       # ventana de 15 min
```

Activos disponibles: `btc`, `eth`, `sol`, `xrp`, `bnb`, `doge`, `hype`.

### Qué muestra

```
PRICE  Will Bitcoin close above $100k on April 30?   BUY @ 0.62 ×500    4.2 msg/s  total=412
BOOK   Will Bitcoin close above $100k on April 30?   bid=0.62(8) ask=0.63(12)
TRADE  Will Bitcoin close above $100k on April 30?   @ 0.625
```

Cada línea incluye timestamp, tipo de evento, mercado, precio, y tasa de mensajes por segundo.

***

## Fuente de datos: Polymarket CLOB

| API            | URL                                              | Uso                          |
| -------------- | ------------------------------------------------ | ---------------------------- |
| Gamma REST     | `https://gamma-api.polymarket.com`               | Metadata de mercados activos |
| CLOB WebSocket | `wss://ws-subscriptions-clob.polymarket.com/ws/` | Stream en tiempo real        |

### Tipos de eventos

| Evento             | Descripción                             | Frecuencia                          |
| ------------------ | --------------------------------------- | ----------------------------------- |
| `book`             | Snapshot completo del order book        | Al suscribirse y en cambios grandes |
| `price_change`     | Cambio en un nivel de precio específico | Alta — cada orden nueva             |
| `last_trade_price` | Precio del último trade ejecutado       | Por cada match                      |

### Protocolo WebSocket

```json
// Suscripción (enviar al conectar) — formato real validado
{"assets_ids": ["token_id_1", "token_id_2"], "type": "market"}
```

Cada mercado en Polymarket tiene uno o dos `clobTokenIds` (Yes/No). Se obtienen del endpoint `GET /markets` de la Gamma API.

***

## Métricas de comparación (Spark UI)

Para la comparación de arquitecturas se medirán:

- **Input rate / Processing rate** — tasa de ingesta vs procesamiento
- **Batch duration** — latencia por micro-batch
- **Shuffle read/write** — operaciones costosas entre stages
- **Executor Run Time vs GC Time** — eficiencia de cómputo
- **Spill to disk** — señal de presión de memoria

***

## Arquitecturas a comparar

|            | Local (MacBook M-series) | AWS EMR / Google Colab |
| ---------- | ------------------------ | ---------------------- |
| Cores      | 10-12                    | configurable           |
| Memoria    | 16-32 GB                 | hasta 64+ GB           |
| Spark mode | local\[\*]               | cluster / standalone   |
| GPU        | MPS (opcional)           | T4 / A10G (opcional)   |

***

## Mercados de referencia

Los mercados de alta frecuencia usados en este proyecto son de tipo **"Up or Down"** — predicciones binarias sobre si un activo sube o baja en una ventana de tiempo corta.

### Importante: IDs rotativos

Los mercados de 5 minutos crean un **nuevo market ID por ventana** — no se puede hardcodear un ID permanente. El slug sigue el patrón `{asset}-updown-{window}m-{unix_timestamp}` donde el timestamp es **exactamente un múltiplo de `window × 60`**, lo que permite calcularlo directamente:

```bash
# Mercado activo de Bitcoin 5 min — cálculo directo por timestamp
TS=$(python3 -c "import time; w=300; print((int(time.time())//w)*w)")
curl -s "https://gamma-api.polymarket.com/markets?slug=btc-updown-5m-$TS" \
  | python3 -c "import json,sys; m=json.load(sys.stdin); print(m[0]['question'] if m else 'not found')"
```

### Mercados usados como ejemplo en el proyecto

| Mercado                         | Slug pattern                    | Resolución       | Volumen típico | Notas                                           |
| ------------------------------- | ------------------------------- | ---------------- | -------------- | ----------------------------------------------- |
| **Bitcoin Up or Down 5 min**    | `btc-updown-5m-{timestamp}`     | Cada 5 min       | \~$10          | Mercado primario del proyecto — alta frecuencia |
| **Bitcoin Up or Down 15 min**   | `btc-updown-15m-{timestamp}`    | Cada 15 min      | \~$10          | Variante de menor frecuencia                    |
| **Ethereum Up or Down 5 min**   | `eth-updown-5m-{timestamp}`     | Cada 5 min       | \~$10          | Regresor para modelo multi-mercado              |
| **Solana Up or Down 5 min**     | `sol-updown-5m-{timestamp}`     | Cada 5 min       | \~$10          | Regresor adicional                              |
| **Bitcoin Up or Down diario**   | `bitcoin-up-or-down-*-et`       | 1×/día           | \~$100k        | Mayor liquidez — más eventos por segundo        |
| **Bitcoin above $74k (MES DD)** | `bitcoin-above-74k-on-april-15` | 15 Apr 16:00 UTC | \~$99k         | conditionId: ver abajo                          |

También existen variantes para XRP, BNB, DOGE y HYPE con los mismos patrones de slug (`xrp-updown-5m-*`, `bnb-updown-5m-*`, etc.). Todos validados.

### Cómo encontrar un mercado específico por URL de Polymarket

1. Abre el mercado en `polymarket.com`
2. El `slug` está en la URL: `polymarket.com/event/{slug}`
3. Consulta: `curl "https://gamma-api.polymarket.com/markets?slug={slug}"`

***

## Requisitos del enunciado

- [x] Fuente de streaming real (Polymarket CLOB WebSocket, pública, sin auth)
- [ ] Kafka como broker intermedio
- [ ] Spark Structured Streaming con ventanas y watermarks
- [ ] Estadísticas en tiempo real (min, max, media, varianza por rango de precio)
- [ ] Modelo supervisado entrenado sobre datos acumulados
- [ ] Scoring en tiempo real sobre el stream activo
- [ ] Comparación en ≥2 arquitecturas con Spark UI
- [ ] Visualización animada (PowerBI / Plotly Dash)
