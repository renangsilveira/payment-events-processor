# ADR-004: gRPC for Internal Payment Status Queries

**Status:** Accepted  
**Date:** 2026-07-13

## Context

Internal services need to query payment status synchronously. The system
already exposes a REST API (`GET /payments/{id}`) for external clients.
A second synchronous interface was introduced to demonstrate protocol
selection judgment: choosing the right tool for the communication pattern
rather than defaulting to REST for all service-to-service calls.

## Decision

We implement `PaymentStatusService.GetPaymentStatus` as a gRPC endpoint
defined in `payment_status.proto`. The service is exposed on a separate
port (default: 9090) via `net.devh:grpc-server-spring-boot-starter`.
The contract is defined in Protobuf, which generates strongly-typed client
and server stubs at build time.

## Consequences

**Positive:**
- Binary protocol reduces payload size and serialization overhead compared
  to JSON over HTTP.
- Protobuf schema provides a strict, versioned contract for internal
  consumers, validated at compile time.
- HTTP/2 multiplexing allows multiple in-flight requests on a single
  connection, improving throughput for high-frequency internal polling.

**Negative:**
- gRPC is not human-readable; debugging requires tools like `grpcurl`
  rather than `curl`.
- The Protobuf Gradle plugin configuration is non-trivial in Kotlin DSL
  (requires `buildscript {}` block rather than the `plugins {}` block).
- Browser clients cannot call gRPC directly without a proxy (e.g.,
  gRPC-Web or Envoy).

## Alternatives Considered

**REST (additional endpoint):** Would duplicate the existing `GET /payments`
endpoint with no meaningful differentiation. Rejected because the goal is
to demonstrate protocol selection, not to add redundant endpoints.

**GraphQL:** Appropriate when clients need flexible field selection across
multiple resources. Not justified for a single-resource point query.

**Internal REST with protobuf body:** A hybrid approach that provides binary
serialization over HTTP/1.1. Rejected because it forfeits HTTP/2
multiplexing and requires custom content-type handling without gaining the
tooling benefits of native gRPC.