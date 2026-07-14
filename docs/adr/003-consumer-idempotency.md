# ADR-003: Consumer-Side Idempotency Over Kafka Exactly-Once Semantics

**Status:** Accepted  
**Date:** 2026-07-13

## Context

Kafka guarantees at-least-once delivery by default. A consumer may
receive the same message more than once due to rebalancing, broker
restarts, or consumer crashes before offset commit. The payment consumer
must not process the same event twice, as doing so would result in
duplicate `PaymentResultEvent` publications and incorrect ledger entries.

## Decision

We implement consumer-side idempotency using a `processed_events` table.
Each consumed message generates a deterministic event ID derived from
`topic + partition + offset`. Before processing, the consumer attempts
to insert this ID into `processed_events`. If the insert succeeds, the
message is processed. If it fails with a unique constraint violation,
the message is a duplicate and is skipped.

This approach is offset-based: two messages with identical payloads but
different offsets are treated as distinct events. This is intentional and
correct — Kafka offset uniquely identifies a position in the partition log.

## Consequences

**Positive:**
- Works with any Kafka delivery guarantee (at-least-once is sufficient).
- No Kafka broker configuration changes required.
- Idempotency check survives consumer restarts; the `processed_events`
  table is durable.

**Negative:**
- Adds a database write per consumed message, increasing latency slightly.
- `processed_events` grows indefinitely; a TTL-based cleanup job would be
  needed in production.

## Alternatives Considered

**Kafka Exactly-Once Semantics (EOS):** Requires `enable.idempotence=true`
on the producer, `isolation.level=read_committed` on all consumers, and
transactional producers. Adds significant complexity to the producer
configuration and forces all consumers in the system to use
`read_committed`. Rejected in favor of the simpler consumer-side approach.

**Application-level deduplication by business key:** Check if a payment
with the same `idempotencyKey` was already processed. Rejected because
this conflates Kafka message deduplication with business-level idempotency.
The two concerns are addressed separately: Kafka dedup via `processed_events`,
business idempotency via `request_fingerprint` (see REST API layer).