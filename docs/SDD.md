# System Design Document — payment-events-processor

## Overview

`payment-events-processor` is a backend service that accepts payment requests via REST, processes them asynchronously through Kafka, and exposes payment status via both REST and gRPC. It is designed to demonstrate production-grade distributed systems patterns relevant to fintech backends: guaranteed message delivery, consumer-side idempotency, real-time stream aggregation, and resilience under downstream failure.

---

## Components

### REST API (`PaymentController`)

Accepts `POST /payments` with an `Idempotency-Key` header. Validates the request, persists the `Payment` entity, and writes an `OutboxEvent` atomically in a single database transaction. Returns the payment immediately with status `PENDING`.

Idempotency is enforced at two levels:
- **Kafka level:** `ProcessedEvent` table prevents duplicate message processing.
- **REST level:** Same `Idempotency-Key` + same payload → 200 cached. Same key + different payload → 409.

### Outbox Publisher (`OutboxPublisher`)

A `@Scheduled` component that polls `outbox_events` for `PENDING` records and publishes them to `payment.events.v1`. ShedLock prevents concurrent execution in multi-instance deployments. A Resilience4j circuit breaker wraps the Kafka send; when the broker is unavailable, the circuit opens after 5 failures with >50% error rate, preventing retry storms.

### Payment Event Consumer (`PaymentEventConsumer`)

A `@KafkaListener` consuming from `payment.events.v1`. Before processing each message, it derives a deterministic event ID (`topic + partition + offset`) and attempts to insert it into `processed_events`. Duplicate constraint violations indicate the message was already processed; the consumer skips it gracefully.

`@RetryableTopic` provides exponential backoff retry (attempts: 4, delay: 1s → 2s → 4s) and routes permanently failed messages to `payment.events.v1-dlt`.

### Kafka Streams Topology (`PaymentStreamsTopology`)

A streaming topology that consumes from `payment.events.v1`, groups events by currency, and aggregates `count` and `totalAmountCents` in a Hopping Window (size: 60s, advance: 30s, grace: 5s). Aggregates are materialized in a local state store (`payment-stats-by-currency`).

`GET /payments/stats` queries this store via Interactive Queries. **Note:** In a multi-instance deployment, Interactive Queries return only the local partition's data; full aggregation would require RPC federation between instances using `KafkaStreams.streamsMetadataForStore()`.

### gRPC Service (`PaymentStatusGrpcService`)

Implements `PaymentStatusService.GetPaymentStatus` defined in `payment_status.proto`. Queries `PaymentRepository` directly for point reads. A Resilience4j retry policy (3 attempts, 500ms wait) wraps the client stub for transient `StatusRuntimeException` errors.

---

## Data Model

```sql
-- Core payment record
CREATE TABLE payments (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    amount_cents        BIGINT NOT NULL,
    currency            VARCHAR(3) NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    idempotency_key     VARCHAR(255) UNIQUE NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Outbox events pending Kafka publication
CREATE TABLE outbox_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id   UUID NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    payload        JSONB NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count    INT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- Kafka message deduplication
CREATE TABLE processed_events (
    event_id       UUID PRIMARY KEY,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Sequence: Payment Creation → Kafka → Result
Client          REST API        Database        OutboxPublisher     Kafka
│                │               │                  │               │
│─POST /payments▶│               │                  │               │
│                │──BEGIN TX────▶│                  │               │
│                │──INSERT payment──────────────────▶               │
│                │──INSERT outbox_event──────────────▶              │
│                │──COMMIT TX───▶│                  │               │
│◀───201─────────│               │                  │               │
│                │               │                  │               │
│                │        (5s polling interval)      │               │
│                │               │◀─SELECT PENDING──│               │
│                │               │──rows────────────▶               │
│                │               │                  │──produce────▶ │
│                │               │                  │◀─ack─────────│
│                │               │◀─UPDATE published│               │

---

## Resilience Design

| Failure Scenario | Behavior |
|---|---|
| Kafka broker unavailable during publish | Circuit breaker opens after 5 failures; `OutboxEvent` remains `PENDING` and is retried on next polling cycle |
| Consumer fails to process event | `@RetryableTopic` retries up to 4 times with exponential backoff; routes to DLT after max retries |
| Duplicate Kafka delivery | `ProcessedEvent` unique constraint rejects duplicate; consumer skips gracefully |
| gRPC server transient error | Resilience4j retry (3 attempts, 500ms) on `StatusRuntimeException` |

---

## Observability

Custom Micrometer gauges exposed at `/actuator/prometheus`:

| Metric | Description |
|---|---|
| `payment.outbox.pending.count` | Number of outbox events pending Kafka publication. Non-zero sustained value indicates publisher lag or circuit breaker open. |
| `payment.outbox.failed.count` | Number of outbox events that exceeded max retries. Any non-zero value requires manual intervention. |

Grafana dashboard (`docker/grafana/provisioning/dashboards/payment-events-dashboard.json`) is provisioned automatically on `docker-compose up` with three panels: Outbox Lag, DLQ Rate, and Consumer Throughput.