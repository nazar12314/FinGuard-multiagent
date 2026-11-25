from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Topic:
    name: str
    # Fixed up front: adding partitions later moves keys and breaks per-key order.
    partitions: int = 6


TX_RAW = Topic("tx.raw")
TX_NORMALIZED = Topic("tx.normalized")
ALERTS_CREATED = Topic("alerts.created")
CASES_EVENTS = Topic("cases.events")
AGENT_RESULTS = Topic("agent.results")
DOCUMENTS_UPLOADED = Topic("documents.uploaded")
DOCUMENTS_PARSED = Topic("documents.parsed")
# Dev-only pings that exercise the outbox, relay and consumer runtime end to end.
DIAGNOSTICS_PINGS = Topic("diagnostics.pings", partitions=1)

# The per-agent task topics join once the agent types exist.
TOPICS = (
    TX_RAW,
    TX_NORMALIZED,
    ALERTS_CREATED,
    CASES_EVENTS,
    AGENT_RESULTS,
    DOCUMENTS_UPLOADED,
    DOCUMENTS_PARSED,
    DIAGNOSTICS_PINGS,
)
TOPIC_NAMES = frozenset(topic.name for topic in TOPICS)
