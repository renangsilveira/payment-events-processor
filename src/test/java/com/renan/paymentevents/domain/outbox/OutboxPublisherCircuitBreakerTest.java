package com.renan.paymentevents.domain.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.renan.paymentevents.avro.PaymentEvent;
import com.renan.paymentevents.config.ResilienceConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxPublisherCircuitBreakerTest {

    private OutboxPublisher outboxPublisher;
    private OutboxEventRepository outboxEventRepository;
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private CircuitBreaker circuitBreaker;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        outboxEventRepository = mock(OutboxEventRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);

        // Use the same config as production to test realistic behavior
        circuitBreaker = new ResilienceConfig().kafkaPublishCircuitBreaker();

        outboxPublisher = new OutboxPublisher(
                outboxEventRepository,
                kafkaTemplate,
                new ObjectMapper(),
                circuitBreaker
        );
    }

    @Test
    void circuitBreakerOpensAfterFailureThreshold() {
        // Simulate Kafka broker unavailable — KafkaTemplate.send() returns a failed future
        CompletableFuture<SendResult<String, PaymentEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failedFuture);

        List<OutboxEvent> events = buildEvents(10);
        when(outboxEventRepository.findByStatus(OutboxStatus.PENDING)).thenReturn(events);

        // Run publisher — failures will accumulate against the circuit breaker
        outboxPublisher.publishPendingEvents();

        // After minimumNumberOfCalls (5) with >50% failure rate, circuit should be OPEN
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    @SuppressWarnings("unchecked")
    void circuitBreakerRemainsClosedOnSuccess() {
        CompletableFuture<SendResult<String, PaymentEvent>> successFuture =
                CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(successFuture);

        List<OutboxEvent> events = buildEvents(5);
        when(outboxEventRepository.findByStatus(OutboxStatus.PENDING)).thenReturn(events);

        outboxPublisher.publishPendingEvents();

        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    private List<OutboxEvent> buildEvents(int count) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(i -> OutboxEvent.createPending(
                        UUID.randomUUID(),
                        "PaymentCreated",
                        """
                        {"paymentId":"%s","amountCents":1000,"currency":"USD","idempotencyKey":"key-%d"}"""
                                .formatted(UUID.randomUUID(), i)
                ))
                .toList();
    }
}