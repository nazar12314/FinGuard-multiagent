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
```

Python 3.12+ and [uv](https://docs.astral.sh/uv/) are required.

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
