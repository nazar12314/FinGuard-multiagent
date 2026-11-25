# 008. JSON Schema contracts in a registry, evolved backward-compatibly

- Status: accepted
- Date: 2026-11-25

## Context and problem

Producers and consumers of a Kafka message are deployed independently, so a change to a message can break a
consumer that hasn't been upgraded. The contract for every message has to be explicit, published, and checked
before a change merges.

## Considered options

1. Avro or Protobuf with a schema registry.
2. JSON Schema with a schema registry.
3. No registry; agree on Pydantic models in code.

## Decision

Option 2. Messages stay readable JSON, Pydantic validates them in code, and the registry still enforces the rules.

- **Source.** Each message type declares a `MessageContract`: its type, version and the Pydantic model of its
  `data`. `finguard-contracts generate` writes `contracts/<type>.v<version>.json`, the schema of the whole message
  on the wire (the envelope with `type` and `version` fixed, plus `data`). CI fails when a committed file is out of
  date, so the files and the code can't drift.
- **Registry.** Apicurio Registry, through its Confluent-compatible API. The subject is `<type>.v<version>`, because
  several message types share a topic. Every subject has the BACKWARD rule: a new schema must accept data written
  under the previous one.
- **CI.** A job starts Apicurio, registers the base branch's contract files and asks the registry whether the
  branch's files are compatible. A breaking change needs a new version, which is a new contract and subject. A
  contract removed since the base fails the job unless it is declared retired.
- **Draft 7.** Every file declares JSON Schema draft 7. Apicurio reads a schema without `$schema` as draft 4, where
  exclusive bounds and `const` mean something else, so it would reject or wave through changes to them.
- **Closed data models.** Under the registry's rules an open object can't gain a field compatibly, because old data
  could already carry that name with any type. So every `data` model, nested ones included, sets `extra="forbid"`,
  which lets it add optional fields. Fixed-length tuples are refused too: draft 7 has no `prefixItems`, so the
  registry can't see their elements change. The envelope stays open so that consumers ignore fields they don't know; adding an envelope field
  is therefore a breaking change for the registry and should be rare.

## Consequences

- A consumer can rely on the published schema of every message it reads, and a breaking change fails CI.
- JSON is larger and slower to parse than Avro or Protobuf.
- The registry's JSON Schema rules decide what counts as compatible, not hand-written checks.

## Revisit when

Payload size or throughput becomes a concern, at which point Avro or Protobuf would pay for themselves.
