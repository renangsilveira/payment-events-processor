package com.renan.paymentevents.api.payment;

import java.util.List;

public record PaymentStatsResponse(
        List<CurrencyStats> stats
) {
    public record CurrencyStats(
            String currency,
            long count,
            long totalAmountCents,
            String windowStart,
            String windowEnd
    ) {}
}