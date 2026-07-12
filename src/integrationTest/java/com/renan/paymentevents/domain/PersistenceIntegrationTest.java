package com.renan.paymentevents.domain;

import com.renan.paymentevents.TestcontainersConfiguration;
import com.renan.paymentevents.domain.idempotency.ProcessedEvent;
import com.renan.paymentevents.domain.idempotency.ProcessedEventRepository;
import com.renan.paymentevents.domain.outbox.OutboxEvent;
import com.renan.paymentevents.domain.outbox.OutboxEventRepository;
import com.renan.paymentevents.domain.outbox.OutboxStatus;
import com.renan.paymentevents.domain.payment.Payment;
import com.renan.paymentevents.domain.payment.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class PersistenceIntegrationTest {

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    void persistsAndRetrievesPaymentByIdempotencyKey() {
        Payment payment = Payment.createPending(1500L, "USD", "idem-key-001", "fake-fingerprint-for-test");

        Payment saved = paymentRepository.save(payment);

        assertThat(saved.getId()).isNotNull();
        assertThat(paymentRepository.findByIdempotencyKey("idem-key-001"))
                .isPresent()
                .get()
                .extracting(Payment::getId)
                .isEqualTo(saved.getId());
    }

    @Test
    void rejectsDuplicateIdempotencyKey() {
        paymentRepository.save(Payment.createPending(1000L, "USD", "duplicate-key", "fake-fingerprint-for-test"));
        paymentRepository.flush();

        Payment duplicate = Payment.createPending(2000L, "USD", "duplicate-key", "fake-fingerprint-for-test");

        assertThatThrownBy(() -> {
            paymentRepository.save(duplicate);
            paymentRepository.flush();
        }).isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void persistsOutboxEventWithJsonbPayload() {
        UUID aggregateId = UUID.randomUUID();
        OutboxEvent event = OutboxEvent.createPending(
                aggregateId,
                "PaymentCreated",
                "{\"amountCents\":1500,\"currency\":\"USD\"}"
        );

        OutboxEvent saved = outboxEventRepository.save(event);

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEventRepository.findByStatus(OutboxStatus.PENDING))
                .extracting(OutboxEvent::getAggregateId)
                .contains(aggregateId);
    }

    @Test
    void persistsProcessedEventAndRejectsDuplicateOnReinsert() {
        UUID eventId = UUID.randomUUID();
        processedEventRepository.save(ProcessedEvent.of(eventId));
        processedEventRepository.flush();

        assertThatThrownBy(() -> {
            processedEventRepository.save(ProcessedEvent.of(eventId));
            processedEventRepository.flush();
        }).isInstanceOf(DataIntegrityViolationException.class);
    }
}