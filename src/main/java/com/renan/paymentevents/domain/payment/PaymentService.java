package com.renan.paymentevents.domain.payment;

import com.renan.paymentevents.api.payment.PaymentRequest;
import com.renan.paymentevents.domain.outbox.OutboxEvent;
import com.renan.paymentevents.domain.outbox.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Service
public class PaymentService {

    private static final String PAYMENT_CREATED_EVENT_TYPE = "PaymentCreated";

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;

    public PaymentService(PaymentRepository paymentRepository, OutboxEventRepository outboxEventRepository) {
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    @Transactional
    public Payment createPayment(PaymentRequest request, String idempotencyKey) {
        Long amountCents = toCents(request.amount());
        String fingerprint = RequestFingerprint.of(amountCents, request.currency(), idempotencyKey);

        return paymentRepository.findByIdempotencyKey(idempotencyKey)
                .map(existing -> reconcileWithExisting(existing, fingerprint, idempotencyKey))
                .orElseGet(() -> createNewPayment(amountCents, request.currency(), idempotencyKey, fingerprint));
    }

    private Payment reconcileWithExisting(Payment existing, String fingerprint, String idempotencyKey) {
        if (!existing.matchesFingerprint(fingerprint)) {
            throw new IdempotencyConflictException(idempotencyKey);
        }
        return existing;
    }

    private Payment createNewPayment(Long amountCents, String currency, String idempotencyKey, String fingerprint) {
        Payment payment = Payment.createPending(amountCents, currency, idempotencyKey, fingerprint);
        Payment savedPayment = paymentRepository.save(payment);

        OutboxEvent event = OutboxEvent.createPending(
                savedPayment.getId(),
                PAYMENT_CREATED_EVENT_TYPE,
                buildPayload(savedPayment)
        );
        outboxEventRepository.save(event);

        return savedPayment;
    }

    private String buildPayload(Payment payment) {
        return """
                {"paymentId":"%s","amountCents":%d,"currency":"%s","idempotencyKey":"%s"}"""
                .formatted(payment.getId(), payment.getAmountCents(), payment.getCurrency(), payment.getIdempotencyKey());
    }

    private Long toCents(BigDecimal amount) {
        return amount.setScale(2, RoundingMode.UNNECESSARY)
                .movePointRight(2)
                .longValueExact();
    }
}