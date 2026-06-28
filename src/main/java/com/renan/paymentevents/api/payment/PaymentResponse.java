package com.renan.paymentevents.api.payment;

import com.renan.paymentevents.domain.payment.Payment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;

public record PaymentResponse(
        UUID id,
        BigDecimal amount,
        String currency,
        String status,
        String idempotencyKey,
        Instant createdAt
) {

    public static PaymentResponse from(Payment payment) {
        BigDecimal amount = BigDecimal.valueOf(payment.getAmountCents())
                .movePointLeft(2)
                .setScale(2, RoundingMode.UNNECESSARY);

        return new PaymentResponse(
                payment.getId(),
                amount,
                payment.getCurrency(),
                payment.getStatus().name(),
                payment.getIdempotencyKey(),
                payment.getCreatedAt()
        );
    }
}