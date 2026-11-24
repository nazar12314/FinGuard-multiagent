# 002. Kafka for events and agent tasks

- Status: accepted
- Date: 2026-11-24

## Context and problem

FinGuard moves transactions, alerts, case events and agent tasks between independently scaled processes. It needs
per-case ordering, consumer groups that scale on lag, and the ability to replay history, for example to re-run
detection over past transactions. The owner's previous project used SQS. Kafka's ecosystem (Strimzi, KEDA,
schema registries) and learning it are deliberate goals of the project too.

## Considered options

1. SQS, as in the audio project.
2. RabbitMQ.
3. Redis Streams.
4. Apache Kafka in KRaft mode.

## Decision

Option 4. Delivery is at least once, and messages for the same key (case, account, document) stay in order on one
partition. Database effects happen effectively once: the inbox mark and the handler's writes commit together, so a
redelivered message is skipped. Effects outside Postgres must be idempotent on their own.

- **Producing.** Services never publish directly. They write the message to the transactional outbox in the same
  database transaction as the state change, and the outbox relay publishes it. This relies on the single database
  of ADR-006.
- **Topics** are declared in code (`finguard.platform.topics`) and created by `finguard-topics`, together with
  their retry and dead-letter topics. Brokers don't create topics on first use, and the outbox refuses messages
  for unknown topics, so a mistyped name fails in the writer's transaction.
- **Consuming.** One shared runtime runs every consumer. It deduplicates through the inbox table in the handler's
  transaction and commits the offset only after the message is handled or handed on.
- **Failures.** A transient error is retried in place a few times with backoff, which keeps per-key order through
  short blips. Then the message moves to `<topic>.retry.1`, `<topic>.retry.2`, and so on. Each tier has a fixed
  delay, so a partition is paused rather than blocked while it waits. After the last tier, the message goes to
  `<topic>.dlq`. Permanent errors (business-rule failures, malformed messages) go to the DLQ at once. Retry and
  dead-letter copies carry the consumer group, the error and the original position in headers, and other
  consumer groups skip retry copies that aren't theirs. The error header holds the exception type, plus the message
  only for FinGuard's own errors, because driver and validation errors quote the data they reject.

## Consequences

- Replays and new consumer groups can read the retained history.
- A message that reaches a retry topic can be handled after later messages for the same key. Handlers load the
  current state from the database, because messages carry ids rather than copied data, so this is tolerated.
- Running Kafka costs more than a managed queue, and partition counts have to be chosen up front: adding
  partitions later moves keys between partitions.
- Dead-lettered messages need a re-drive path. That arrives with the admin tooling.
- Consumers read only the configured retry tiers, so a tier's topic must be drained before its delay is removed.

## Revisit when

Throughput stays tiny and operating cost matters more than replay.
