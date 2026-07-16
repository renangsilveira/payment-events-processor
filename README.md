# payment-events-processor

Event-driven payment processing system built with Java 21, Spring Boot 3.4, and Apache Kafka. Demonstrates production-grade patterns for distributed transaction consistency in a fintech context: Outbox Pattern, consumer-side idempotency, Dead Letter Queue handling, Kafka Streams real-time aggregation, and gRPC for internal synchronous queries.

[![CI](https://github.com/renangsilveira/payment-events-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/renangsilveira/payment-events-processor/actions/workflows/ci.yml)

---

## Architecture
┌─────────────────────────────────────────────────────────────────────┐
│                        payment-events-processor                      │
│                                                                      │
│  POST /payments                                                      │
│       │                                                              │
│       ▼                                                              │
│  ┌──────────┐   atomic TX   ┌─────────────┐                         │
│  │ Payment  │──────────────▶│ OutboxEvent │                         │
│  │ (Postgres)│              │  (Postgres) │                         │
│  └──────────┘              └──────┬──────┘                         │
│                                   │ @Scheduled + ShedLock           │
│                                   ▼                                  │
│                          ┌──────────────────┐                       │
│                          │  OutboxPublisher  │◀── CircuitBreaker    │
│                          │  (Resilience4j)  │                       │
│                          └────────┬─────────┘                       │
│                                   │ Avro + Schema Registry           │
│                                   ▼                                  │
│                       ┌─────────────────────┐                       │
│                       │  payment.events.v1   │  Kafka topic         │
│                       └──────────┬──────────┘                       │
│                                  │                                   │
│              ┌───────────────────┼───────────────────┐              │
│              │                   │                   │              │
│              ▼                   ▼                   ▼              │
│   ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐    │
│   │ PaymentEvent     │  │ KafkaStreams  │  │  Retry Topics    │    │
│   │ Consumer         │  │ Topology     │  │  → DLQ (-dlt)    │    │
│   │ (idempotency)    │  │ (hopping     │  │  (@RetryableTopic│    │
│   └────────┬─────────┘  │  windows)    │  └──────────────────┘    │
│            │            └──────┬───────┘                           │
│            ▼                   ▼                                    │
│  payment.results.v1   GET /payments/stats                           │
│                       (Interactive Queries)                         │
│                                                                      │
│  GET /payments/status ──▶ gRPC PaymentStatusService                │
│                           (Retry via Resilience4j)                  │
└─────────────────────────────────────────────────────────────────────┘

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.4 |
| Messaging | Apache Kafka (KRaft) |
| Schema | Avro + Confluent Schema Registry |
| Streaming | Kafka Streams (Hopping Windows) |
| Database | PostgreSQL + Flyway |
| Sync RPC | gRPC (`net.devh` starter) |
| Resilience | Resilience4j (Circuit Breaker + Retry) |
| Observability | Micrometer + Prometheus + Grafana |
| Testing | JUnit 5, Testcontainers, JaCoCo |
| Build | Gradle (Kotlin DSL) |

---

## Key Patterns

- **Transactional Outbox:** Payment and OutboxEvent written atomically; a scheduled publisher reads and publishes to Kafka. Guarantees at-least-once delivery without XA transactions. See [ADR-001](docs/adr/001-outbox-pattern.md).
- **Consumer-side idempotency:** Each Kafka message generates a deterministic ID from `topic + partition + offset`. Stored in `processed_events` before processing; duplicate constraint prevents double-processing. See [ADR-003](docs/adr/003-consumer-idempotency.md).
- **Stripe-style idempotency:** REST API accepts `Idempotency-Key` header. Same key + same payload returns cached response (200); same key + different payload returns 409.
- **Dead Letter Queue:** `@RetryableTopic` with exponential backoff (1s → 2s → 4s, max 4 attempts). Failed messages route to `payment.events.v1-dlt`.
- **Kafka Streams:** Hopping window aggregation (60s window, 30s advance) grouped by currency. Results exposed via Interactive Queries at `GET /payments/stats`.
- **Circuit Breaker:** Wraps Kafka publish in `OutboxPublisher`. Opens after 50% failure rate in a 10-call sliding window; half-opens after 30s to probe recovery.

---

## Local Setup

### Prerequisites

- JDK 21
- Docker and Docker Compose

### Start infrastructure

```bash
docker-compose up -d
```

Starts PostgreSQL, Kafka (KRaft), Schema Registry, Prometheus, and Grafana. Grafana dashboard is provisioned automatically at `http://localhost:3000` (admin/admin).

### Run the application

```bash
./gradlew bootRun
```

### Run tests

Unit tests (no Docker required):

```bash
./gradlew test
```

Integration tests (CI only — requires Docker on Linux):

```bash
./gradlew integrationTest
```

---

## API Examples

### Create a payment

```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: pay-001" \
  -d '{"amount": 50.00, "currency": "USD"}'
```

```json
{
  "id": "3b7ade8d-1493-4ec0-9f0b-6cfb6c4fb581",
  "amount": 50.00,
  "currency": "USD",
  "status": "PENDING",
  "idempotencyKey": "pay-001",
  "createdAt": "2026-07-13T01:54:16.323488Z"
}
```

### Query real-time aggregation stats

```bash
curl http://localhost:8080/payments/stats
```

```json
{
  "stats": [
    {
      "currency": "USD",
      "count": 3,
      "totalAmountCents": 15000,
      "windowStart": "2026-07-13T01:30:00Z",
      "windowEnd": "2026-07-13T01:31:00Z"
    }
  ]
}
```

### Query payment status via gRPC

```bash
grpcurl -plaintext \
  -d '{"payment_id": "3b7ade8d-1493-4ec0-9f0b-6cfb6c4fb581"}' \
  localhost:9090 \
  com.renan.paymentevents.grpc.PaymentStatusService/GetPaymentStatus
```

```json
{
  "payment_id": "3b7ade8d-1493-4ec0-9f0b-6cfb6c4fb581",
  "status": "PENDING",
  "amount_cents": "5000",
  "currency": "USD",
  "created_at": "2026-07-13T01:54:16.323488Z",
  "found": true
}
```

### Observability

- Prometheus metrics: `http://localhost:8080/actuator/prometheus`
- Grafana dashboard: `http://localhost:3000` (admin/admin)
- Swagger UI: `http://localhost:8080/swagger-ui.html`

---

## Architecture Decision Records

- [ADR-001: Outbox Pattern over direct Kafka publish](docs/adr/001-outbox-pattern.md)
- [ADR-002: Avro and Schema Registry over JSON](docs/adr/002-avro-schema-registry.md)
- [ADR-003: Consumer-side idempotency over Kafka EOS](docs/adr/003-consumer-idempotency.md)
- [ADR-004: gRPC for internal payment status queries](docs/adr/004-grpc-internal-status.md)

---

## Project Structure
src/
├── main/java/com/renan/paymentevents/
│   ├── api/payment/          # REST controllers and DTOs
│   ├── config/               # Kafka, gRPC, Resilience4j configuration
│   ├── domain/
│   │   ├── consumer/         # PaymentEventConsumer, PaymentResultPublisher
│   │   ├── idempotency/      # ProcessedEvent entity and repository
│   │   ├── outbox/           # OutboxEvent, OutboxPublisher
│   │   └── payment/          # Payment entity, PaymentService
│   ├── grpc/                 # gRPC service implementation
│   ├── observability/        # Custom Micrometer metrics
│   └── streams/              # Kafka Streams topology
├── main/proto/               # Protobuf definitions
├── main/resources/
│   ├── avro/                 # Avro schema definitions
│   ├── db/migration/         # Flyway migrations
│   └── grafana/              # Grafana dashboard JSON
└── integrationTest/          # Testcontainers integration tests (CI only)docs/
├── SDD.md                    # System Design Document
├── portfolio-positioning.md  # Interview talking points
└── adr/                      # Architecture Decision Records

---

## Author

**Renan Silveira** — Backend Engineer  
[linkedin.com/in/renangsilveira](https://linkedin.com/in/renangsilveira) · [github.com/renangsilveira](https://github.com/renangsilveira)

---

## License

[#license](#license)

MIT © Renan G. Silveira
