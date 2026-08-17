# 📈 WikiPulse — real-time Wikipedia edit-spike detector

A weekend-sized **data-engineering** project: a streaming pipeline that ingests
Wikipedia's live edit firehose, computes per-wiki edit rates in tumbling
windows, flags statistical **spikes** (a wiki suddenly being edited far more
than usual — often a sign of breaking news), and serves it on a live dashboard.

It's built to demonstrate the parts of streaming that actually matter:
**windowing, watermarks, late events, idempotent sinks, and cost hygiene** —
not clever ML. The spike "model" is deliberately dumb (mean + k·σ).

```
 Wikipedia EventStreams (SSE, no auth)
         │   src/producer.py          at-least-once produce, keyed by wiki
         ▼
   ┌───────────────┐  Kafka topic: wiki.edits
   │  Kafka broker │  ── local Redpanda (dev)  OR  Confluent Cloud (prod)
   └───────────────┘
         │   src/pipeline.py          Quix Streams + tested SpikeDetector
         ▼   tumbling 60s windows, keyed by wiki, watermark-based close
   ┌───────────────┐  Kafka topic: wiki.metrics
   │  Kafka broker │
   └───────────────┘
         │   src/sink.py              idempotent INSERT OR REPLACE
         ▼
   DuckDB file  OR  MotherDuck (cloud, concurrency-friendly)
         │   src/app.py
         ▼
   Streamlit live dashboard  (edit-rate chart + spike panel)
```

## Why these choices

| Decision | Reason |
|----------|--------|
| **Wikipedia EventStreams** | Live, high-volume, **no API key** — you spend the weekend on stream semantics, not auth. |
| **Redpanda for dev** | Speaks the Kafka API, single container, free — the *exact same client code* runs against Confluent Cloud. Only `.env` changes. |
| **Quix Streams** | Kafka-native stream processing you `pip install` — no separate cluster (unlike Flink/ksqlDB). |
| **Spike = mean + k·σ over a rolling baseline** | Correctly treats *sustained* busy wikis (Wikidata) as normal and only flags *deviations*. A plain threshold would false-alarm constantly. |
| **DuckDB / MotherDuck** | Zero-infra local file for dev; MotherDuck is a drop-in cloud warehouse (`md:` prefix) for a genuinely-live dashboard. |

## Quickstart — fully local (no cloud account)

```bash
make install          # venv + deps
make broker-up        # local Redpanda in Docker
cp .env.example .env  # defaults already point at local Redpanda
make test             # 13 unit tests (no broker needed)

# three terminals (or run in background):
make pipeline         # 1. stream processor
make sink             # 2. DuckDB sink
make producer         # 3. Wikipedia -> Kafka

make dashboard        # open http://localhost:8501
```

> **Tip:** the default window is 60s, so the first metrics take a minute to
> appear. For a fast demo, set `WINDOW_SECONDS=10` in `.env`.

> **Local DuckDB concurrency:** DuckDB is single-writer *per process*. While the
> sink is writing the local file, the dashboard (read-only) may briefly lock —
> it shows a friendly notice, not a crash. For a truly live local dashboard,
> use **MotherDuck** (below), which handles concurrent readers.

## Going to the cloud (free tiers)

Two independent switches, both just `.env` edits.

### Broker → Confluent Cloud (managed Kafka)

**This part needs you** — account creation and API keys can't be automated.

1. Sign up at <https://confluent.cloud> (free trial includes credits).
2. **Create cluster** → *Basic* type → pick a cloud/region → launch.
3. Left nav → **API keys** → *Create key* → scope to the cluster. Copy the
   **key** and **secret**.
4. Cluster overview → copy the **Bootstrap server** (e.g.
   `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`).
5. Put them in `.env`:
   ```ini
   KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
   KAFKA_SECURITY_PROTOCOL=SASL_SSL
   KAFKA_SASL_MECHANISM=PLAIN
   KAFKA_SASL_USERNAME=<API_KEY>
   KAFKA_SASL_PASSWORD=<API_SECRET>
   ```
6. Run the same `make pipeline / sink / producer`. The topics auto-create.

> 💸 **A running cluster bills by the hour.** When you're done: `make teardown`,
> then delete the cluster in the Confluent console (see `scripts/teardown.sh`).

### Sink → MotherDuck (cloud DuckDB)

1. Sign up at <https://motherduck.com> (free tier), copy your **access token**.
2. In `.env`:
   ```ini
   DUCKDB_DATABASE=md:wikipulse
   MOTHERDUCK_TOKEN=<your-token>
   ```
3. The sink and dashboard now read/write the cloud database — the dashboard can
   run live alongside the sink.

## Layout

```
src/
  config.py      .env-driven config; PLAINTEXT <-> SASL_SSL in one place
  schema.py      normalize raw EventStreams JSON -> EditEvent   (unit-tested)
  windowing.py   tumbling-window spike detector, pure/clock-free (unit-tested)
  producer.py    SSE -> wiki.edits
  pipeline.py    wiki.edits -> windowing -> wiki.metrics  (Quix Streams)
  sink.py        wiki.metrics -> DuckDB/MotherDuck (idempotent)
  app.py         Streamlit dashboard
  kafka_util.py  topic creation helper
tests/           schema + windowing tests (13, no broker required)
docker-compose.yml   local Redpanda + console (http://localhost:8080)
scripts/teardown.sh  stop infra; Confluent cost-off instructions
Makefile             one-liners for every step
```

## Tuning (`.env`)

| Var | Meaning | Default |
|-----|---------|---------|
| `WINDOW_SECONDS` | tumbling window size | 60 |
| `SPIKE_BASELINE_WINDOWS` | trailing windows for the rolling mean/σ | 15 |
| `SPIKE_K` | flag when `count > mean + K·σ` | 3.0 |
| `SPIKE_MIN_COUNT` | ignore low-volume wikis below this | 10 |

## Tests

```bash
make test        # or: python -m pytest -q
```

The interesting logic — window closing on watermark advance, per-wiki
isolation, spike vs. steady traffic, low-volume guards — is tested with
**no broker and no clock**: synthetic `(wiki, timestamp)` events in, window
metrics out.

## Ideas to extend

- Replace the σ-threshold with EWMA or a seasonal baseline (time-of-day).
- Add a **dead-letter topic** for malformed events instead of dropping them.
- Enrich spikes with the article titles driving them (join the edit stream).
- dbt models over the MotherDuck tables for a proper marts layer.
- Alerting: push spikes to a Slack/Telegram webhook.
