# FinGuard

A multi-agent AML (anti-money-laundering) investigation platform for a simulated EU neobank. Transactions stream
through Kafka and are filtered cheaply by rules and ML. Only ambiguous, high-risk alerts go to a team of specialist
LLM agents. The agents investigate transaction graphs, KYC documents, sanctions lists and adverse media, then draft
an evidence-cited suspicious transaction report for a human reviewer.

## Getting started

```bash
uv sync
uv run pre-commit install
uv run pytest tests/unit                          # fast, no Docker
uv run pytest tests/integration                   # Postgres and Kafka in testcontainers (needs Docker)
FINGUARD_ENV=dev uv run finguard-api              # API on :8000, docs at /docs
FINGUARD_ENV=dev uv run finguard-migrate upgrade head
FINGUARD_ENV=dev uv run finguard-topics           # creates Kafka topics with their retry and DLQ topics
FINGUARD_ENV=dev uv run finguard-outbox-relay     # publishes outbox rows to Kafka
```

Python 3.12+ and [uv](https://docs.astral.sh/uv/) are required. `finguard-migrate` is the Alembic command line
configured from FinGuard's settings (`FINGUARD_DATABASE__URL`), so `history`, `downgrade` and `revision
--autogenerate` work the same way.

## Local stack

The infrastructure runs in Docker Compose; FinGuard's own processes run on the host with `uv run`.

```bash
docker compose -f deploy/compose/compose.yaml up -d --wait                          # core (~1.5 GB RAM)
docker compose -f deploy/compose/compose.yaml --profile observability up -d --wait  # + observability (~5.5 GB)
docker compose -f deploy/compose/compose.yaml --profile observability down          # stop (add -v to wipe data)
```

Keep `--profile observability` on `down` even after a core-only `up`, or containers started with the profile
keep running.

| Service | Profile | Address on the host | Local credentials |
| --- | --- | --- | --- |
| Postgres 18 with pgvector and pg_trgm (extensions are created by migrations) | core | `localhost:5432`, database `finguard` | `finguard` / `finguard` |
| Kafka (KRaft) | core | `localhost:9092` | none |
| Apicurio Registry | core | http://localhost:8080 (Confluent-compatible API at `/apis/ccompat/v7`) | none |
| Redis | core | `localhost:6379` | password `finguard` |
| RustFS (S3-compatible) | core | S3 API http://localhost:9000, console http://localhost:9001/rustfs/console/ | `finguard` / `finguard-dev-secret` |
| Neo4j Community | core | http://localhost:7474, `bolt://localhost:7687` | `neo4j` / `finguard-dev` |
| OpenTelemetry Collector | observability | OTLP gRPC `localhost:4317`, HTTP `localhost:4318` | none |
| Langfuse | observability | http://localhost:3000 | `admin@finguard.local` / `finguard-dev`; project keys `pk-lf-finguard-dev` / `sk-lf-finguard-dev` |
| Prometheus | observability | http://localhost:9090 | none |
| Grafana | observability | http://localhost:3001 | anonymous admin |

The collector sends traces to Langfuse and serves metrics for Prometheus to scrape. Ollama runs on the host, not in
Compose. All ports are bound to `127.0.0.1`, and the credentials are for this local stack only.

## Watching a message flow

Outside prod, the API has a diagnostic ping that travels the whole messaging path: the API writes it to the outbox,
the relay publishes it to Kafka, and a consumer handles it, retrying or dead-lettering it when asked to fail.

Prepare the stack once:

```bash
docker compose -f deploy/compose/compose.yaml up -d --wait
export FINGUARD_ENV=dev FINGUARD_LOG_FORMAT=console
uv run finguard-migrate upgrade head
uv run finguard-topics
```

Then start each process in its own terminal, with the same two variables exported. Shorter retry delays than the
default 30 s and 300 s make the retries quicker to watch:

```bash
uv run finguard-api
uv run finguard-outbox-relay
FINGUARD_KAFKA__RETRY_DELAYS_S='[5, 10]' uv run finguard-ping-consumer
```

Send pings and follow them:

```bash
curl -s -X POST localhost:8000/api/v1/dev/pings -H 'Content-Type: application/json' -d '{}'
curl -s -X POST localhost:8000/api/v1/dev/pings -H 'Content-Type: application/json' \
  -d '{"fail": "transient", "times": 4}'
curl -s -X POST localhost:8000/api/v1/dev/pings -H 'Content-Type: application/json' \
  -d '{"fail": "permanent"}'
curl -s localhost:8000/api/v1/dev/pings/<ping_id>
```

The status shows where the ping is and every attempt so far:

| Status | Meaning |
| --- | --- |
| `queued` | In the outbox; the relay hasn't published it yet |
| `published` | In Kafka, not handled yet |
| `retrying` | Failed; `topic` shows whether it's retried in place (`diagnostics.pings`) or waits on a retry topic |
| `handled` | The consumer handled it and recorded it in the inbox |
| `dead_lettered` | Failed permanently, or on every retry topic; it's on `diagnostics.pings.dlq` |
| `parked` | Kafka refused it for good, so the relay set it aside |

A ping with `"times": 4` fails three times in place, waits 5 s on `diagnostics.pings.retry.1`, fails once more there
and then succeeds. The consumer's log shows each `consumer.handler_failed` and the `consumer.retry_scheduled` move.
To read the dead-letter topic with its error headers:

```bash
docker exec finguard-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic diagnostics.pings.dlq --from-beginning --timeout-ms 5000 --formatter-property print.headers=true
```

The status endpoint assumes the API runs with the same retry settings as the consumer.

## Repository layout

| Path | Contents |
| --- | --- |
| `src/finguard/` | The application package: entrypoints, core, infrastructure, platform and the bounded-context modules |
| `contracts/` | JSON Schemas for Kafka messages |
| `agents/` | Versioned prompts, agent configs and knowledge notes |
| `datagen/` | Synthetic data generator |
| `evals/` | Evaluation harness, datasets and reports |
| `ui/` | Streamlit review console |
| `deploy/` | docker-compose, images, Helm, kind and Terraform |
| `docs/` | Architecture decision records and runbooks |
