# ADR-002: Avro with Schema Registry Over JSON

**Status:** Accepted  
**Date:** 2026-07-13

## Context

Kafka messages are consumed by independent services that must agree on
the shape of the data. Without a shared contract, producers and consumers
are tightly coupled at the code level: any field rename or type change
requires coordinated deployments across all consumers.

## Decision

We use Apache Avro for Kafka message serialization and Confluent Schema
Registry to manage schema versions. Schemas are defined in `.avsc` files
(`PaymentEvent.avsc`, `PaymentResultEvent.avsc`) and compiled to Java
classes at build time via the Avro Gradle plugin. The Schema Registry
enforces compatibility rules, rejecting schema changes that would break
existing consumers.

In integration tests, Confluent Schema Registry is replaced by Apicurio
Registry (in-memory mode) via its Confluent-compatible API endpoint
(`/apis/ccompat/v7`). This eliminates the ~700 MB image pull and reduces
container startup time from minutes to seconds.

## Consequences

**Positive:**
- Schema changes are validated before deployment via compatibility checks.
- Binary encoding reduces message size compared to JSON.
- Generated Java classes provide compile-time type safety for producers
  and consumers.

**Negative:**
- Avro requires a running Schema Registry; the system cannot produce or
  consume messages without it.
- Schema evolution rules (BACKWARD, FORWARD, FULL compatibility) must be
  understood by all engineers modifying event schemas.

## Alternatives Considered

**JSON with no schema:** Zero infrastructure overhead. Rejected because
field renames and type changes are invisible until runtime, making
consumer breakage unpredictable in a multi-service environment.

**JSON Schema / Protobuf:** Both are viable. Protobuf was evaluated for
the gRPC interface (see ADR-004) where it is the natural choice. Avro
was preferred for Kafka due to mature tooling in the Confluent ecosystem
and wider adoption in fintech data pipelines.