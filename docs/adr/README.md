# Architecture decision records

Each ADR lives in `NNN-short-title.md` and uses the template below (MADR). Write the full ADR when its area is first
implemented, and keep this index in sync with the decision log in Notion.

| ADR | Decision | Status |
| --- | --- | --- |
| 001 | Modular monolith codebase, hexagonal modules, multiple deployables | Accepted |
| 002 | Kafka for events and agent tasks | Accepted |
| 003 | LangGraph with a Postgres checkpointer for orchestration | Accepted |
| 003b | Temporal for the case lifecycle, LangGraph inside activities | Planned |
| 004 | Specialist agents as separate Kafka workers | Accepted |
| 005 | LLMs only on escalated alerts, never per transaction | Accepted |
| 006 | One Postgres, a schema per module, no cross-schema queries | Accepted |
| 007 | Tools exposed as MCP servers with per-agent authorization | Accepted |
| 008 | JSON Schema contracts + registry, backward-compatible evolution | Accepted |
| 009 | pgvector instead of a dedicated vector database | Accepted |
| 010 | Self-hosted Langfuse via OpenTelemetry | Accepted |
| 011 | Hybrid LLM: local vLLM for small-tier agents, API for synthesizer and critic | Accepted |
| 012 | PII redacted or pseudonymised before external LLM APIs | Accepted |
| 013 | kind for local Kubernetes; short-lived cloud cluster only for demos | Accepted |
| 014 | Record/replay adapters for LLM and tool ports | Accepted |

## Template

```markdown
# NNN. Title

- Status: proposed | accepted | superseded by NNN
- Date: YYYY-MM-DD

## Context and problem

## Considered options

## Decision

## Consequences

## Revisit when
```
