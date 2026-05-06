# Runbook final - Windows RTX 2060, dashboard y comparacion Spark CPU vs GPU

Objetivo: ejecutar el proyecto completo desde Windows, ver el dashboard en vivo, capturar evidencia para el enunciado y comparar la misma app Spark en CPU contra Spark con RAPIDS Accelerator en GPU.

Fuentes oficiales para RAPIDS Spark:

- RAPIDS Accelerator for Apache Spark: https://docs.nvidia.com/spark-rapids/index.html
- Local / on-prem setup: https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/on-premise.html
- Descargas y compatibilidad: https://nvidia.github.io/spark-rapids/docs/download.html
- Configuracion: https://nvidia.github.io/spark-rapids/docs/configs.html

## 0. Que se corrigio antes de correr

- `spark-stream --rapids` activa `com.nvidia.spark.SQLPlugin` y `spark.rapids.sql.enabled=true`.
- `btc.forecast.clean` usa `sigma_log` desde `data/feature_list.json`, alineado con el modelo.
- `KafkaSink` usa `acks="all"` y envia de forma concurrente; drena confirmaciones y hace `flush()` al cierre.
- Producers tienen `--dry-run` para separar limite de fuente vs limite de Kafka.
- Producers muestran `last1s`, `interval` y `avg`:
  - `last1s`: mensajes del ultimo segundo.
  - `interval`: promedio desde el ultimo log.
  - `avg`: promedio global desde que arranco el proceso.
- `polymarket-producer --assets ...` permite correr varios mercados Up-or-Down coherentes con Binance.
- `score-stream` resuelve `data/model.sarimax.pkl` y `data/feature_list.json` desde la raiz del repo aunque lo lances desde otra carpeta.
- `score-stream` consume `btc.forecast.clean` por default y reutiliza el `ts` exacto de Spark, para que SARIMAX quede alineado verticalmente con BTC/Polymarket. La opcion vieja `--ts-offset` ya no existe.
- `spark-stream` vuelve a publicar filas parciales en `btc.forecast.clean`: si llega BTC pero no Polymarket en ese segundo, conserva `btc_mid` y rellena `poly_prob`/forecast/drift con el ultimo Polymarket valido durante `--poly-stale-after` segundos. Esto mantiene la curva BTC fluida y evita que Grafana pierda Polymarket al reconectar.
- Grafana lee el forecast limpio desde `btc.forecast.clean` y SARIMAX desde `btc.sarimax-forecast.clean`; `btc.forecast.live`/`btc.sarimax-forecast.live` quedan como topics legados para no mezclar datos viejos o streams duplicados.
- `score-stream` publica `btc.sarimax-forecast.clean` en `partition=0`. Esto importa porque el datasource Kafka de Grafana consulta una partición explícita; si el topic heredado tiene 3 particiones y el scorer cae en partición 2, el panel queda en `No data` aunque Kafka sí tenga mensajes.
- Grafana refresca cada 1 s. Los paneles live leen los topics con `autoOffsetReset: "lastN"` y `lastN: 7200`, para que una reconexion/minimizado/reload reconstruya la historia reciente desde Kafka en vez de empezar a dibujar desde cero.
- Grafana mantiene inferencia BTC y agrega tabla `Multi-asset live stats (stats.windowed)`.

## 1. Ambiente base en Windows

PowerShell en la raiz del repo:

```powershell
cd C:\Users\andre\OneDrive\Desktop\CODE\streaming_polymarket
.\.venv\Scripts\activate
python -m pip install -U pip
pip install -e ".[all]"
```

Verifica:

```powershell
python -c "import aiohttp, websockets, aiokafka, rich, statsmodels, pyarrow, pyspark; print('deps ok')"
python -c "import pandas as pd; df=pd.read_parquet('data/training.parquet'); print(len(df), int(df.isna().sum().sum()))"
python -c "import statsmodels.api as sm; print(type(sm.load('data/model.sarimax.pkl')).__name__)"
```

Si Python 3.13 complica paquetes, usa Python 3.12:

```powershell
py -3.12 -m venv .venv312
.\.venv312\Scripts\activate
python -m pip install -U pip
pip install -e ".[all]"
```

## 2. Infraestructura

Abre Docker Desktop y espera a que diga que el engine esta corriendo.

```powershell
docker compose -f .\infra\docker-compose.yml up -d
docker compose -f .\infra\docker-compose.yml ps
```

URLs:

- Kafka UI: http://localhost:8080
- Grafana: http://localhost:3000 (`admin/admin`)
- Spark UI CPU: http://localhost:4040 cuando `spark-stream` este vivo

Si cambiaste el dashboard JSON y Grafana no lo refresca, reinicia Grafana:

```powershell
docker compose -f .\infra\docker-compose.yml restart grafana
```

## 3. Fase A - Pipeline vivo y dashboard BTC

Abre cinco terminales PowerShell, todas en el repo y con el venv activado.

Terminal 1 - Binance 5 activos:

```powershell
binance-producer --symbols btcusdt ethusdt solusdt xrpusdt bnbusdt --streams aggTrade bookTicker depth20@100ms
```

Terminal 2 - Polymarket 5 activos Up-or-Down:

```powershell
polymarket-producer --assets btc eth sol xrp bnb --window 5
```

Terminal 3 - Spark CPU para dashboard:

```powershell
spark-stream --master "local[*]" --console --forecast-topic btc.forecast.clean --output-path data/live/cpu --checkpoint-path data/checkpoints/live-cpu-v6
```

Terminal 4 - Scoring SARIMAX:

```powershell
score-stream --debug
```

Terminal 5 - Throughput:

```powershell
throughput-probe --duration 60
```

Que debes ver:

- Grafana: panel BTC mid, P(up), SARIMAX P(up), implied target, SARIMAX target y drifts.
- Grafana: tabla `Multi-asset live stats` con etiquetas legibles como `BTCUSDT`, `BTC Up`, `ETH Down`, etc.
- `score-stream`: consume `btc.forecast.clean` y emite SARIMAX con el mismo `ts` de los puntos que usa para inferencia. Con defaults emite live con `feature_mode=warm`; despues de ~10 min pasa a `feature_mode=exact`. Para reproducir el comportamiento metodologico estricto, usa `score-stream --strict-history --debug`.
- Producers: usa `interval` y `avg`, no solo `last1s`, para medir sostenido.

Notas para que el dashboard no se rompa:

- Debe haber solo un `spark-stream` escribiendo a `btc.forecast.clean`. Si ves lineas dobles o puntos triangulados, casi seguro hay dos Spark vivos escribiendo forecast.
- Debe haber un `score-stream` vivo escribiendo a `btc.sarimax-forecast.clean`. Si el panel SARIMAX queda en `No data`, revisa offsets por partición:

```powershell
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic btc.forecast.clean
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic btc.sarimax-forecast.clean
```

  Grafana lee partición 0. Si ves SARIMAX creciendo solo en partición 1 o 2, estás corriendo una versión vieja del scorer; reinícialo con la versión que fuerza `partition=0`.
- `Window ended, rediscovering...` en `polymarket-producer` es normal cada 5 minutos. El producer salta al siguiente mercado; mientras no haya update Polymarket, Spark sigue publicando BTC-only y usa el ultimo `poly_prob` valido hasta `--poly-stale-after` segundos. Si de verdad no vuelve Polymarket despues de esa ventana, la linea puede pausarse, pero no debe desaparecer lo ya dibujado.
- Si alguna linea parece borrarse despues de un refresh, confirma que los targets live del dashboard usan `autoOffsetReset: "lastN"` con `lastN: 7200`, no `LATEST`/`EARLIEST`.
- Si acabas de cambiar el dashboard, reinicia Grafana y espera la ventana visible actual para que salgan datos viejos del rango:

```powershell
docker compose -f .\infra\docker-compose.yml restart grafana
```

- Si quieres una demo totalmente limpia sin borrar volumenes, usa siempre un checkpoint nuevo para Spark, por ejemplo `data/checkpoints/live-cpu-v4`.

## 4. Diagnostico de throughput

Si el throughput no llega a 4000 msg/s, primero separa fuente vs Kafka.

Prueba fuente Polymarket sin Kafka:

```powershell
polymarket-producer --assets btc eth sol xrp bnb --window 5 --dry-run
```

Prueba fuente Binance sin Kafka:

```powershell
binance-producer --symbols btcusdt ethusdt solusdt xrpusdt bnbusdt --streams aggTrade bookTicker depth20@100ms --dry-run
```

Interpretacion:

- `dry-run avg` parecido a `throughput-probe`: limite real de la fuente en ese periodo.
- `dry-run avg` alto pero `throughput-probe` bajo: cuello Kafka/Docker/Windows.
- `last1s` alto pero `avg` bajo: hay rafagas, no throughput sostenido.

Si los 5 activos correctos no alcanzan 4000/s, documenta ese valor como caso analitico real y ejecuta un stress test separado:

```powershell
binance-producer --symbols btcusdt ethusdt solusdt xrpusdt bnbusdt dogeusdt hypeusdt --streams aggTrade bookTicker depth20@100ms
polymarket-producer --assets btc eth sol xrp bnb doge hype --window 5
throughput-probe --duration 60
```

No uses `polymarket-producer --all` como evidencia principal del modelo BTC; si lo usas, etiquetalo como stress test de arquitectura.

## 5. Capturas para la demo/dashboard

Captura estas pantallas mientras todo corre:

- Grafana dashboard completo.
- Tabla multi-asset de Grafana.
- `throughput-probe --duration 60`.
- Terminal `score-stream --debug` con forecasts SARIMAX.
- Kafka UI mostrando topics `binance.book`, `binance.trades`, `polymarket.events`, `stats.windowed`, `btc.forecast.clean`, `btc.sarimax-forecast.clean`.

## 6. Fase B - Spark CPU para comparacion

Para evidencia limpia, detén el `spark-stream` de la fase A y corre una corrida dedicada CPU. Mantén producers vivos.

```powershell
spark-stream --master "local[*]" --console --forecast-topic btc.forecast.clean --output-path data/phase6/cpu --checkpoint-path data/checkpoints/phase6-cpu
```

Dejalo 10-15 minutos.

Captura Spark UI:

- Streaming: input rate, processing rate, batch duration, pending batches.
- Jobs: duracion de jobs.
- Stages: shuffle read/write, spill memory/disk, executor run time, GC time.
- Environment: Spark version, Java, Python, master, configs.

Guarda tambien el throughput de la misma ventana:

```powershell
throughput-probe --duration 60
```

## 7. Preparar WSL2 para Spark GPU

RAPIDS Spark se corre en Linux/WSL2, no en Windows Python puro.

PowerShell:

```powershell
wsl --install -d Ubuntu-24.04
wsl --status
nvidia-smi
```

Salida verificada en este equipo:

```
Distribución predeterminada: Ubuntu
Versión predeterminada: 2
WSL1 no es compatible con la configuración actual del equipo.
```

```
Tue May  5 21:16:40 2026
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 591.86                 Driver Version: 591.86         CUDA Version: 13.1     |
+-----------------------------------------+------------------------+----------------------+
| GPU  Name                  Driver-Model | Bus-Id          Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |           Memory-Usage | GPU-Util  Compute M. |
|=========================================+========================+======================|
|   0  NVIDIA GeForce RTX 2060      WDDM  |   00000000:01:00.0  On |                  N/A |
| 27%   45C    P0             30W /  160W |    1580MiB /   6144MiB |      1%      Default |
+-----------------------------------------------------------------------------------------+
```

CUDA 13.1 esta disponible desde Windows. Para que aparezca desde WSL2 basta con que el driver WDDM de Windows soporte WSL2 GPU Paravirtualization (nvidia-smi funciona dentro de Ubuntu sin instalar driver adicional en WSL).

Dentro de Ubuntu:

```bash
nvidia-smi
cd ~
git clone /mnt/c/Users/andre/OneDrive/Desktop/CODE/streaming_polymarket ./streaming_polymarket
cd ~/streaming_polymarket
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -e ".[all]"
```

Nota: usar `git clone` en lugar de `cp -r` es órdenes de magnitud más rápido para copiar desde el filesystem de Windows. Ubuntu no tiene `python` por defecto; `python3 -m venv` crea el venv y dentro de él ya existe el comando `python`.

Si quieres usar Docker/Kafka desde WSL, habilita la integracion WSL de Docker Desktop y prueba:

```bash
docker compose -f ./infra/docker-compose.yml ps
```

RAPIDS por Maven usa el default del codigo:

```text
com.nvidia:rapids-4-spark_2.12:26.04.0
```

Si Maven falla, usa jar local:

```bash
mkdir -p ~/spark-rapids
curl -L -o ~/spark-rapids/rapids-4-spark_2.12-26.04.0.jar \
  https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0.jar
```

## 8. Fase C - Spark GPU RAPIDS

Mantén producers vivos y corre en WSL2.

Para scoring SARIMAX desde WSL2 no necesitas calcular `WINDOWS_HOST` en esta
configuración: Kafka anuncia `localhost:9092` en `docker-compose.yml`. Usa:

```bash
source .venv/bin/activate
score-stream --kafka-bootstrap localhost:9092 --debug
```

Si usas la variable `WINDOWS_HOST`, créala dentro de bash/WSL, no en
PowerShell:

```bash
WINDOWS_HOST=$(awk '/nameserver/ {print $2; exit}' /etc/resolv.conf)
score-stream --kafka-bootstrap "$WINDOWS_HOST:9092" --debug
```

Si PowerShell muestra `The term 'WINDOWS_HOST=$(...)' is not recognized`, ese
comando se ejecutó en la terminal equivocada. Ojo además: si arrancas Kafka con
`KAFKA_ADVERTISED_LISTENERS=...EXTERNAL://localhost:9092`, preferir
`localhost:9092` evita que el cliente reciba metadata inconsistente.

Opcion Maven:

```bash
spark-stream --master "local[*]" --rapids --rapids-explain ALL --console \
  --forecast-topic btc.forecast.clean \
  --output-path data/phase6/gpu \
  --checkpoint-path data/checkpoints/phase6-gpu
```

Opcion jar local:

```bash
spark-stream --master "local[*]" --rapids \
  --rapids-jar "$HOME/spark-rapids/rapids-4-spark_2.12-26.04.0.jar" \
  --rapids-explain ALL --console \
  --forecast-topic btc.forecast.clean \
  --output-path data/phase6/gpu \
  --checkpoint-path data/checkpoints/phase6-gpu
```

Evidencia obligatoria GPU:

- Spark Environment contiene `spark.plugins=com.nvidia.spark.SQLPlugin`.
- Spark Environment contiene `spark.rapids.sql.enabled=true`.
- Logs o SQL plan muestran operadores `Gpu*` o razones `NOT_ON_GPU`.
- `nvidia-smi` muestra proceso Java/Spark usando la RTX 2060 durante micro-batches.

Captura las mismas pantallas de Spark UI que en CPU.

## 9. Fase D - Benchmark ML opcional

Esto no reemplaza Spark UI; solo complementa la historia de GPU.

```bash
python scripts/benchmark_inference.py --n 200
python scripts/benchmark_inference.py --gpu --n 200
python modeling/train_model_cuml.py
```

Si cuML no soporta `exog=` en tu version, no uses esos numeros; instala una version RAPIDS compatible o reporta que esa parte quedo no ejecutada.

## 10. Tabla final para `docs/phase-6-report.md`

| Metrica | Spark CPU | Spark GPU RAPIDS |
|---|---:|---:|
| OS | Windows 11 Pro 10.0.26200 | Windows 11 Pro + WSL2 Ubuntu 24.04 |
| CPU | Intel Core i7-9700F | Intel Core i7-9700F |
| RAM | 32 GB | 32 GB compartida |
| GPU | N/A | NVIDIA RTX 2060 6 GB VRAM |
| Driver / CUDA | N/A | 591.86 / CUDA 13.1 |
| Spark version | 3.5.8 | 3.5.8 |
| Spark mode | `local[*]` | `local[*]` + RAPIDS |
| Plugin Spark | ninguno | `com.nvidia.spark.SQLPlugin` |
| Kafka input | mismos topics | mismos topics |
| Producers | mismos comandos | mismos comandos |
| Duracion observada | 19 min 02 s | 25 min 40 s |
| Active streaming queries | 4 | 4 |
| Promedio input, 4 queries | 1,496.58 msg/s | 1,465.01 msg/s |
| Mediana process, 4 queries | 1,637.01 msg/s | 1,392.09 msg/s |
| Query input mas alto | 1,613.25 msg/s | 1,949.95 msg/s |
| Estabilidad final | estable | estable |
| Throughput probe avg | pendiente | pendiente |
| Evidencia GPU | N/A | Spark Environment + `Gpu*` plan + `nvidia-smi` |

CSV fuente: `results/phase6_cpu_vs_gpu.csv`. Conclusión: GPU corre, pero CPU
gana o empata para el dashboard live porque el workload es micro-batch + I/O.

## 11. Cierre

Para Canvas/demo necesitas:

- Dashboard Grafana funcionando.
- Forecast SARIMAX vivo.
- Throughput real documentado.
- Si no llega a 4000/s con el caso analitico, explicacion fuente vs stress test.
- Spark UI CPU capturado.
- Spark UI GPU RAPIDS capturado.
- Tabla comparativa llena.
- Conclusiones: CPU vs GPU, limitaciones, y que RAPIDS acelera operadores soportados pero Kafka/Python/operadores no soportados pueden quedar en CPU.
