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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@Transactional
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PersistenceIntegrationTest {

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

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
        paymentRepository.save(Payment.createPending(1000L, "USD", "duplicate-key", "fp-1"));
        paymentRepository.flush();

        Payment duplicate = Payment.createPending(2000L, "USD", "duplicate-key", "fp-2");

        assertThatThrownBy(() -> {
            paymentRepository.save(duplicate);
            paymentRepository.flush();
        }).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
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

        // Spring Data JPA's save() uses merge() for manually-assigned IDs,
        // which does SELECT then UPDATE if found — no constraint violation.
        // Use JdbcTemplate to force a raw INSERT that hits the PK constraint.
        assertThatThrownBy(() ->
                jdbcTemplate.update(
                        "INSERT INTO processed_events (event_id, processed_at) VALUES (?, NOW())",
                        eventId
                )
        ).isInstanceOf(DataIntegrityViolationException.class);
    }
}