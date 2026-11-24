# FinGuard

A multi-agent AML (anti-money-laundering) investigation platform for a simulated EU neobank. Transactions stream
through Kafka and are filtered cheaply by rules and ML. Only ambiguous, high-risk alerts go to a team of specialist
LLM agents. The agents investigate transaction graphs, KYC documents, sanctions lists and adverse media, then draft
an evidence-cited suspicious transaction report for a human reviewer.

## Getting started

```bash
uv sync
uv run pre-commit install
uv run pytest
FINGUARD_ENV=dev uv run finguard-api   # API on :8000, docs at /docs
```

Python 3.12+ and [uv](https://docs.astral.sh/uv/) are required.

## Local stack

The infrastructure runs in Docker Compose; FinGuard's own processes run on the host with `uv run`.

```bash
docker compose -f deploy/compose/compose.yaml up -d --wait                          # core (~1.5 GB RAM)
docker compose -f deploy/compose/compose.yaml --profile observability up -d --wait  # + observability (~4 GB)
docker compose -f deploy/compose/compose.yaml --profile observability down          # stop (add -v to wipe data)
```

| Service | Profile | Address on the host | Local credentials |
| --- | --- | --- | --- |
| Postgres 18 + pgvector + pg_trgm | core | `localhost:5432`, database `finguard` | `finguard` / `finguard` |
| Kafka (KRaft) | core | `localhost:9092` | none |
| Apicurio Registry | core | http://localhost:8080 (Confluent-compatible API at `/apis/ccompat/v7`) | none |
| Redis | core | `localhost:6379` | password `finguard` |
| RustFS (S3-compatible) | core | S3 API http://localhost:9000, console http://localhost:9001/rustfs/console/ | `finguard` / `finguard-dev-secret` |
| Neo4j Community | core | http://localhost:7474, `bolt://localhost:7687` | `neo4j` / `finguard-dev` |
| OpenTelemetry Collector | observability | OTLP gRPC `localhost:4317`, HTTP `localhost:4318` | none |
| Langfuse | observability | http://localhost:3000 | `admin@finguard.local` / `finguard-dev` |
| Prometheus | observability | http://localhost:9090 | none |
| Grafana | observability | http://localhost:3001 | anonymous admin |

The collector sends traces to Langfuse and serves metrics for Prometheus to scrape. Ollama runs on the host, not in
Compose. All ports are bound to `127.0.0.1`, and the credentials are for this local stack only.

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
