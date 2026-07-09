package com.renan.paymentevents.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.grpc.StatusRuntimeException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class ResilienceConfig {

    // Circuit breaker for Kafka publish in OutboxPublisher
    // Opens after 50% failure rate in a 10-call sliding window,
    // stays open for 30s before transitioning to HALF_OPEN for probing.
    @Bean
    public CircuitBreaker kafkaPublishCircuitBreaker() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(3)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();

        return CircuitBreakerRegistry.of(config)
                .circuitBreaker("kafkaPublish");
    }

    // Retry for gRPC client calls — 3 attempts with 500ms fixed wait.
    // StatusRuntimeException is the gRPC equivalent of HTTP 5xx errors.
    @Bean
    public Retry grpcRetry() {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryOnException(e -> e instanceof StatusRuntimeException)
                .build();

        return RetryRegistry.of(config)
                .retry("grpcStatusQuery");
    }
}