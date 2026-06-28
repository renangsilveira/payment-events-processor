package com.renan.paymentevents.domain.payment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RequestFingerprintTest {

    @Test
    void producesSameHashForSameInputs() {
        String first = RequestFingerprint.of(1500L, "USD", "idem-key-001");
        String second = RequestFingerprint.of(1500L, "USD", "idem-key-001");

        assertThat(first).isEqualTo(second);
    }

    @Test
    void producesDifferentHashWhenAmountDiffers() {
        String first = RequestFingerprint.of(1500L, "USD", "idem-key-001");
        String second = RequestFingerprint.of(2000L, "USD", "idem-key-001");

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void producesDifferentHashWhenCurrencyDiffers() {
        String first = RequestFingerprint.of(1500L, "USD", "idem-key-001");
        String second = RequestFingerprint.of(1500L, "BRL", "idem-key-001");

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void producesDifferentHashWhenIdempotencyKeyDiffers() {
        String first = RequestFingerprint.of(1500L, "USD", "idem-key-001");
        String second = RequestFingerprint.of(1500L, "USD", "idem-key-002");

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void producesSixtyFourCharacterHexString() {
        String fingerprint = RequestFingerprint.of(1500L, "USD", "idem-key-001");

        assertThat(fingerprint).hasSize(64);
        assertThat(fingerprint).matches("^[0-9a-f]{64}$");
    }
}