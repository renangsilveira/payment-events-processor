# Portfolio Positioning — payment-events-processor

This document captures the key talking points, design decisions, and
technical depth markers for use in engineering interviews. It maps each
feature to the type of question it is designed to answer.

---

## "Walk me through this project"

> "This is a payment processing service that demonstrates how to guarantee
> message delivery in an event-driven system without distributed transactions.
> A payment comes in via REST, gets persisted atomically with an outbox record,
> and a scheduler publishes the event to Kafka. The consumer checks idempotency
> before processing to handle Kafka's at-least-once delivery guarantee. The
> system includes Kafka Streams for real-time currency aggregation, a gRPC
> endpoint for internal status queries, Resilience4j for circuit breaking and
> retry, and a Grafana dashboard provisioned automatically with docker-compose."

---

## "Why the Outbox Pattern and not direct Kafka publish?"

> "Kafka is not an XA-capable resource, so you can't include a Kafka publish
> inside a JDBC transaction. If you write to the database and then publish to
> Kafka separately, you have a window where the database write succeeds but
> Kafka fails — your payment exists but no downstream system knows about it.
> The Outbox Pattern closes this window by writing both the payment and a
> pending event record in a single transaction. The publisher reads the table
> and publishes; if Kafka is down, the event stays pending and retries on the
> next cycle. No data loss, no eventual inconsistency."

---

## "How do you handle duplicate Kafka messages?"

> "Each consumed message generates a deterministic event ID from
> `topic + partition + offset`. Before any processing happens, the consumer
> tries to insert that ID into a `processed_events` table. If the insert
> succeeds, the message is new. If it throws a unique constraint violation,
> it's a duplicate and we skip it. This is offset-based deduplication —
> two messages with the same payload but different offsets are treated as
> distinct events, which is correct: the offset is the unique identity in
> Kafka. I deliberately kept this separate from the business-level
> idempotency in the REST API, which checks `Idempotency-Key` + payload
> fingerprint."

---

## "Tell me about the Kafka Streams part"

> "The topology groups `PaymentEvent` messages by currency and aggregates
> count and total amount in a Hopping Window — 60 seconds wide, advancing
> every 30 seconds. This means a payment at T=15s appears in both the
> 0–60s window and the 30–90s window, giving overlapping snapshots.
> Aggregates are materialized in a local state store and exposed via
> Interactive Queries at `GET /payments/stats`. I documented a known
> limitation in the code: in a multi-instance deployment, each instance
> only has the state for its assigned partitions. Full aggregation would
> require RPC federation using `KafkaStreams.streamsMetadataForStore()`."

---

## "Why gRPC for the status endpoint if you already have REST?"

> "REST is already there for external clients. The gRPC endpoint demonstrates
> protocol selection judgment — choosing the right tool for the use case.
> For internal synchronous queries between services at high frequency, gRPC
> over HTTP/2 gives you binary serialization, multiplexed connections, and
> compile-time contract validation via Protobuf. The Protobuf schema is a
> versioned contract that breaks at compile time if a consumer uses a field
> that no longer exists — safer than JSON where field renames are invisible
> until runtime."

---

## "How did you handle testing with Kafka and Postgres?"

> "Integration tests use Testcontainers with static container instances
> started once per JVM and shared across all test contexts. The main
> challenge was preventing Spring from calling `container.stop()` when
> individual test contexts closed — which would kill a shared container
> needed by subsequent tests. The solution was to not expose containers
> as `@ServiceConnection` Spring beans, and instead register connection
> properties manually via `DynamicPropertyRegistrar`. Each context gets
> a unique Kafka consumer group ID and Kafka Streams state directory to
> prevent interference. The circuit breaker behavior is tested with a
> Mockito spy — no Docker needed."

---

## "What would you do differently in production?"

> "A few things. First, the `processed_events` table grows indefinitely;
> a TTL-based cleanup job or partitioning by date is needed. Second,
> Interactive Queries on Kafka Streams only return local partition data —
> in a multi-instance deployment I'd add RPC federation or use a dedicated
> read model updated from a changelog topic. Third, the Outbox Publisher
> is polling-based; for lower latency I'd evaluate Debezium on the WAL.
> And I'd replace `@SpyBean` in the DLQ test with a dedicated test
> double that doesn't require `@DirtiesContext`, which has a cost in
> context startup time."

---

## Differentiators for Miami Fintech Market

- **Cross-border payment context:** Kafka Streams aggregation by currency
  is directly applicable to FX volume monitoring and settlement batch
  triggers common in cross-border rails.
- **Outbox + idempotency:** This combination is the standard approach in
  payment systems where exactly-once guarantees cannot be bought with
  distributed transactions (Stripe, Adyen, and most modern payment
  processors use this pattern or a variant).
- **Observability first:** Dashboard JSON is version-controlled and
  provisioned automatically — not "I have Grafana set up locally" but
  "clone the repo and run `docker-compose up`."
- **Two protocols:** REST for external clients, gRPC for internal queries.
  Demonstrates awareness that protocol choice depends on consumer type
  and communication pattern, not default.