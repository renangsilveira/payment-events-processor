package com.renan.paymentevents.observability;

import com.renan.paymentevents.domain.outbox.OutboxEventRepository;
import com.renan.paymentevents.domain.outbox.OutboxStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class PaymentMetrics {

    private static final String OUTBOX_PENDING_GAUGE = "payment.outbox.pending.count";
    private static final String OUTBOX_FAILED_GAUGE = "payment.outbox.failed.count";

    public PaymentMetrics(MeterRegistry registry, OutboxEventRepository outboxEventRepository) {

        // Gauge: number of outbox events stuck in PENDING state
        // High value = OutboxPublisher is lagging or circuit breaker is open
        Gauge.builder(OUTBOX_PENDING_GAUGE, outboxEventRepository,
                        repo -> repo.countByStatus(OutboxStatus.PENDING))
                .description("Number of outbox events pending publication to Kafka")
                .register(registry);

        // Gauge: number of outbox events permanently failed (exceeded max retries)
        // Any non-zero value requires manual intervention
        Gauge.builder(OUTBOX_FAILED_GAUGE, outboxEventRepository,
                        repo -> repo.countByStatus(OutboxStatus.FAILED))
                .description("Number of outbox events that exceeded max retries")
                .register(registry);
    }
}