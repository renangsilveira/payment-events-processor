# payment-events-processor

Event-driven payment processing system built with Java 21, Spring Boot 3, and Apache Kafka, demonstrating production-grade patterns for distributed transaction consistency: Outbox Pattern, consumer-side idempotency, Dead Letter Queue handling, Kafka Streams aggregation, and gRPC for internal synchronous queries.

## Status

🚧 Work in progress — built incrementally, commit by commit, as part of a backend portfolio focused on event-driven architecture.

## Tech Stack

- **Language:** Java 21
- **Framework:** Spring Boot 3.4
- **Messaging:** Apache Kafka (KRaft mode)
- **Schema Management:** Avro + Confluent Schema Registry
- **Database:** PostgreSQL + Flyway
- **Streaming:** Kafka Streams
- **Sync RPC:** gRPC
- **Resilience:** Resilience4j
- **Observability:** Micrometer + Prometheus + Grafana
- **Testing:** JUnit 5, Testcontainers, JaCoCo (80% coverage gate)

## Local Setup

### Prerequisites

- JDK 21
- Docker and Docker Compose

### Running the infrastructure

```bash
docker-compose up -d
```

This starts:
- PostgreSQL on `localhost:5432`
- Kafka on `localhost:29092` (host) / `kafka:9092` (internal)
- Schema Registry on `localhost:8081`

### Running the application

```bash
./gradlew bootRun
```

### Running tests

```bash
./gradlew test
```

## Architecture

Full design rationale, domain model, and technical decisions documented in:

- [`docs/SDD.md`](./docs/SDD.md) *(added in later phase)*
- Architecture Decision Records in `docs/ADR-*.md` *(added in later phase)*

## License

MIT