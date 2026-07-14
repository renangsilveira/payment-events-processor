# ADR-001: Outbox Pattern Over Direct Kafka Publish

**Status:** Accepted  
**Date:** 2026-07-13

## Context

When a payment is created, the system must both persist the payment to the
database and publish a `PaymentEvent` to Kafka. These are two distinct
infrastructure operations. Without coordination, a failure between them
produces inconsistent state: a payment saved but not published, or a
Kafka message sent for a payment that was never persisted.

## Decision

We use the Transactional Outbox Pattern. The `PaymentService.createPayment`
method writes both the `Payment` entity and an `OutboxEvent` record in a
single database transaction. A separate `OutboxPublisher` component polls
the `outbox_events` table on a scheduled interval and publishes pending
events to Kafka, marking each as published upon success.

## Consequences

**Positive:**
- Atomicity is guaranteed at the database level. Either both the payment
  and the outbox event are committed, or neither is.
- Kafka publish failures are retried automatically on the next polling cycle
  without data loss.
- The circuit breaker on `OutboxPublisher` prevents retry storms when Kafka
  is unavailable.

**Negative:**
- Introduces eventual consistency: there is a delay (up to the polling
  interval) between payment creation and Kafka publication.
- Requires a dedicated `outbox_events` table and a polling scheduler.
- ShedLock is required to prevent concurrent polling in multi-instance
  deployments.

## Alternatives Considered

**Direct Kafka publish in the same transaction:** Not possible. Kafka is not
an XA-capable resource; it cannot participate in a JDBC transaction.

**Kafka transactions with `read_committed`:** Would require all consumers
to use `isolation.level=read_committed` and significantly increases Kafka
broker load. Does not address the atomicity problem with the database write.

**Change Data Capture (Debezium):** Reads the Postgres WAL to emit events.
Eliminates the polling overhead but adds significant operational complexity
(Debezium cluster, Kafka Connect) disproportionate to this service's scope.