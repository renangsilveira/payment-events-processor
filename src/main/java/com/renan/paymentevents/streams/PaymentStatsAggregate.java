package com.renan.paymentevents.streams;

public class PaymentStatsAggregate {

    private long count;
    private long totalAmountCents;

    public PaymentStatsAggregate() {
        this.count = 0;
        this.totalAmountCents = 0;
    }

    public PaymentStatsAggregate increment(long amountCents) {
        this.count++;
        this.totalAmountCents += amountCents;
        return this;
    }

    public long getCount() {
        return count;
    }

    public long getTotalAmountCents() {
        return totalAmountCents;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setTotalAmountCents(long totalAmountCents) {
        this.totalAmountCents = totalAmountCents;
    }
}