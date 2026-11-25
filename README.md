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

Python 3.12+ and [uv](https://docs.astral.sh/uv/) are required, plus the ICU library, which PyICU builds against
for name transliteration. On macOS, run `brew install icu4c pkg-config` and, before the first `uv sync`,
`export PKG_CONFIG_PATH="$(brew --prefix icu4c)/lib/pkgconfig"`. On Debian or Ubuntu, run
`apt-get install libicu-dev pkg-config`. `finguard-migrate` is the Alembic command line
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
| Tempo | observability | inside Compose only; Grafana's Tempo datasource reads it | none |

The collector sends traces to Tempo (viewed in Grafana) and Langfuse, and serves metrics for Prometheus to scrape. Ollama runs on the host, not in
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

Send pings and follow them. The routes need a caller, which the dev auth stub takes from the `X-Dev-User-Id` header
(any UUID):

```bash
CALLER='X-Dev-User-Id: 0196d5a0-0000-7000-8000-000000000001'
curl -s -X POST localhost:8000/api/v1/dev/pings -H "$CALLER" -H 'Content-Type: application/json' -d '{}'
curl -s -X POST localhost:8000/api/v1/dev/pings -H "$CALLER" -H 'Content-Type: application/json' \
  -d '{"fail": "transient", "times": 4}'
curl -s -X POST localhost:8000/api/v1/dev/pings -H "$CALLER" -H 'Content-Type: application/json' \
  -d '{"fail": "permanent"}'
curl -s -H "$CALLER" localhost:8000/api/v1/dev/pings/<ping_id>
```

The status reports what has been recorded, together with every attempt so far:

| Status | Meaning |
| --- | --- |
| `queued` | In the outbox; the relay hasn't published it yet |
| `published` | In Kafka, not handled yet |
| `failing` | At least one attempt failed, and it hasn't been handled yet |
| `handled` | The consumer handled it and recorded it in the inbox |
| `dead_lettered` | It failed permanently, so the consumer sent it to `diagnostics.pings.dlq` |
| `parked` | Kafka refused it for good, so the relay set it aside |

A ping with `"times": 4` fails three times in place, waits 5 s on `diagnostics.pings.retry.1`, fails once more there
and then succeeds. Which retry topic a failing ping waits on, and whether it ran out of retries, is in the consumer's
log (`consumer.handler_failed`, `consumer.retry_scheduled`, `consumer.dead_lettered`) and on the topics themselves.
To read the dead-letter topic with its error headers:

```bash
docker exec finguard-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic diagnostics.pings.dlq --from-beginning --timeout-ms 5000 --formatter-property print.headers=true
```

### Seeing the whole flow as one trace

Start the stack with `--profile observability`, and export `FINGUARD_TELEMETRY__ENABLED=true` next to the other
variables before starting the three processes. Each ping then becomes one trace: the API request, the relay's
`send diagnostics.pings`, the consumer's `process diagnostics.pings` with one `handle` span per attempt, and a
`process diagnostics.pings.retry.1` child for every retry hop. Failed attempts are marked as errors.

Every log line written inside a span carries its `trace_id`. In Grafana (http://localhost:3001), open Explore, pick
the Tempo datasource and paste the `trace_id`, or search with TraceQL, for example
`{resource.service.name="finguard-ping-consumer" && status=error}`.

## Message contracts

`contracts/` holds one JSON Schema per message type and version, generated from the Pydantic models
(`uv run finguard-contracts generate`); see [contracts/README.md](contracts/README.md). To publish them to the local
Apicurio Registry and check a change against what's registered:

```bash
FINGUARD_ENV=dev uv run finguard-contracts register
FINGUARD_ENV=dev uv run finguard-contracts check-compatibility
```

## Building a world

The generator in `datagen/` (its own workspace package, installed by `uv sync`) builds a seeded, deterministic world
from the [IBM AMLworld dataset](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml).
Download `HI-Small_Trans.csv`, `HI-Small_Patterns.txt` and `HI-Small_accounts.csv` from Kaggle into
`data/raw/amlworld/`, and the OpenSanctions files into `data/raw/opensanctions/` (see
[Sanctions and PEP watchlist](#sanctions-and-pep-watchlist)), then:

```bash
uv run finguard-datagen build --seed 42    # writes data/worlds/seed-42/
```

AMLworld bank `12`, National Bank of the East, plays FinGuard's neobank (`--our-bank` picks another). The transaction
CSV pads bank ids with varying zeros (`012` and `0012` are both bank 12), so ids are stored without the padding. The
world keeps only the transactions an account at our bank is party to, since those are all the neobank would see:

| File | Contents |
| --- | --- |
| `transactions.parquet` | One row per transaction, in time order: original amounts and currencies plus EUR amounts from a fixed FX table |
| `accounts.parquet` | Every account in those transactions, identified as `<bank>:<account>`, flagged when it's held at our bank, with its customer |
| `customers.parquet` | One customer per AMLworld entity at our bank: sole traders and companies, with name, country, industry and onboarding date |
| `kyc_profiles.parquet` | What each customer declared at onboarding: expected monthly volume, annual turnover, account purpose, and the risk rating the onboarding rule gave |
| `persons.parquet` | The people behind sole traders, with Ukrainian names kept in Cyrillic next to their official romanisation |
| `addresses.parquet`, `customer_addresses.parquet` | Each customer's registered address and, for sole traders, the owner's home |
| `devices.parquet`, `customer_devices.parquet` | The devices each customer uses to sign in, and when each was first seen |
| `companies.parquet`, `officers.parquet`, `shareholdings.parquet` | Company customers and the holding companies behind them, with registration numbers, directors and who owns what share |
| `ground_truth/transaction_labels.parquet` | The AMLworld laundering label per transaction and the attempt it belongs to, if any |
| `ground_truth/laundering_attempts.parquet` | Each laundering attempt from the pattern file that reaches our bank: typology, size, and how many of its transactions we see |
| `ground_truth/account_labels.parquet` | Per account: how many laundering transactions it's party to, and in which attempts |
| `ground_truth/customer_labels.parquet` | Per customer: laundering-linked or not, and whether its profile fits its activity, doesn't (and why), or is an innocent look-alike |
| `ground_truth/shared_attribute_labels.parquet` | Why an address or device is used by several customers: a laundering ring, or a virtual office, relatives or a bookkeeper |
| `ground_truth/company_labels.parquet` | Per company: a plain structure, a shell-like one (offshore holding, nominee director, circular ownership) or an innocent look-alike |
| `ground_truth/screening_labels.parquet` | Names planted close to real OpenSanctions entries: true matches (same birth date and nationality) and namesakes, with how each name was varied |
| `manifest.json` | Seed, AMLworld variant, our bank, FX rates, their hash and row counts |

Everything under `ground_truth/` is for evaluation only and never reaches the running system.

Bank 12's AMLworld entities are almost all partnerships, sole proprietorships and corporations, so FinGuard is an SME
business neobank. Declared volumes follow each customer's real flows. About 60% of laundering-linked customers get a
profile that doesn't fit their activity, and 10% of the others get an equally odd one for an innocent reason, so a
profile alone never gives the label away. In the same way, customers in one laundering ring often share a device or an
address, but far more innocent customers share one too. Half of the laundering-linked companies have a shell-like
ownership structure; 8% of the others have one for an innocent reason, with nominee directors drawn from the same pool.

Some people get a name close to a real sanctioned or politically exposed person: a different transliteration, the
name order swapped, a middle name dropped or a typo. True matches, which take the listed birth date and nationality,
fall mostly on people around laundering-linked customers; namesakes, born years apart, can be anyone. A few companies
get a namesake of a listed organisation. OpenSanctions publishes daily, so the manifest records the hashes of the
files a world was built from.

### Replaying a world into Kafka

`replay` plays the bank's core system: it sends each transaction as a `transaction.booked` message to `tx.raw`, in
booking order, keyed by our customer's account. Messages carry the original amounts and currencies; ingestion
converts them to EUR. With the stack up and the topics created (`finguard-topics`):

```bash
uv run finguard-datagen replay --seed 42                      # one simulated day per minute
uv run finguard-datagen replay --seed 42 --speedup inf        # as fast as Kafka acknowledges (~1,800/s locally)
uv run finguard-datagen replay --seed 42 --speedup inf --max-rate 200 --limit 5000
```

HI-Small books 95% of its transactions in the first nine of its 18 days, so a paced replay is busiest early on.

## Sanctions and PEP watchlist

Screening matches names against [OpenSanctions](https://www.opensanctions.org/) (free for non-commercial use,
CC BY-NC). Download the `sanctions` and `peps` collections' `targets.simple.csv`, then replace the watchlist:

```bash
mkdir -p data/raw/opensanctions && cd data/raw/opensanctions
curl -fL -o sanctions.targets.simple.csv https://data.opensanctions.org/datasets/latest/sanctions/targets.simple.csv
curl -fL -o peps.targets.simple.csv https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv
cd - && FINGUARD_ENV=dev uv run finguard-watchlist-import    # ~45 s for ~790k entities
```

People and organisations are kept; vessels, aircraft, wallets and securities are not. Each name and alias is stored
in every common Latin spelling: ICU's generic romanisation, plus BGN/PCGN for Cyrillic (Шевченко is both "Sevcenko"
and "Shevchenko"). Each spelling is also stored casefolded without accents or punctuation, with its tokens sorted
(so name order doesn't matter), and as phonetic keys. Trigram indexes on these forms let screening find candidates by
similarity. The import replaces the whole list in one transaction.

## Health, readiness and metrics

Every process serves three endpoints for Kubernetes probes and Prometheus. The API serves them on its own port; the
workers on a small ops port each (`FINGUARD_OPS__PORT` overrides it):

| Process | Address |
| --- | --- |
| `finguard-api` | http://localhost:8000 |
| `finguard-outbox-relay` | http://localhost:9101 |
| `finguard-ping-consumer` | http://localhost:9102 |

| Endpoint | Answers |
| --- | --- |
| `/healthz` | 200 while the process is alive (liveness). A worker answers 503 once its main loop has stopped going round, for example when stuck in an await |
| `/readyz` | 200 when its dependencies answer (database, plus Kafka for the workers), 503 with the failing check otherwise. A worker also answers 503 as soon as SIGTERM arrives |
| `/metrics` | Prometheus metrics: HTTP requests and latency, outbox published, parked, pending and oldest pending age, consumer deliveries by outcome, handler failures, handling time and lag |

With the observability profile, Prometheus (http://localhost:9090) scrapes all three processes every 5 s under the
`finguard` job. Try `finguard_outbox_oldest_pending_age_seconds` or
`sum by (outcome) (finguard_consumer_deliveries_total)`. The relay measures its backlog every 5 s, also while
publishing fails; while the relay itself is down those gauges disappear, so alert on
`up{process="outbox-relay"} == 0` as well.

On SIGTERM a worker reports not ready, stops taking new messages, finishes (or abandons uncommitted) the one in hand,
commits its offset and closes its connections. The API stops accepting connections at once and finishes the requests
in flight, for up to `FINGUARD_API__GRACEFUL_SHUTDOWN_TIMEOUT_S`.

## Repository layout

| Path | Contents |
| --- | --- |
| `src/finguard/` | The application package: entrypoints, core, infrastructure, platform and the bounded-context modules |
| `contracts/` | JSON Schemas for Kafka messages |
| `agents/` | Versioned prompts, agent configs and knowledge notes |
| `datagen/` | Synthetic data generator (`finguard-datagen`), a separate workspace package |
| `evals/` | Evaluation harness, datasets and reports |
| `ui/` | Streamlit review console |
| `deploy/` | docker-compose, images, Helm, kind and Terraform |
| `docs/` | Architecture decision records and runbooks |
