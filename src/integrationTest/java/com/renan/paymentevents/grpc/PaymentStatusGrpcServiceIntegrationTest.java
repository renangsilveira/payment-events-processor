package com.renan.paymentevents.grpc;

import com.renan.paymentevents.LightweightTestcontainersConfiguration;
import com.renan.paymentevents.domain.payment.Payment;
import com.renan.paymentevents.domain.payment.PaymentRepository;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Import(LightweightTestcontainersConfiguration.class)
@SpringBootTest(properties = {
        "grpc.server.in-process-name=test",
        "grpc.server.port=-1",
        "grpc.client.inProcess.address=in-process:test"
})
@DirtiesContext
class PaymentStatusGrpcServiceIntegrationTest {

    @GrpcClient("inProcess")
    private PaymentStatusServiceGrpc.PaymentStatusServiceBlockingStub stub;

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void returnsPaymentStatusWhenFound() {
        Payment payment = Payment.createPending(2500L, "USD",
                UUID.randomUUID().toString(),
                "test-fingerprint-" + UUID.randomUUID());
        Payment saved = paymentRepository.save(payment);

        GetPaymentStatusResponse response = stub.getPaymentStatus(
                GetPaymentStatusRequest.newBuilder()
                        .setPaymentId(saved.getId().toString())
                        .build()
        );

        assertThat(response.getFound()).isTrue();
        assertThat(response.getPaymentId()).isEqualTo(saved.getId().toString());
        assertThat(response.getStatus()).isEqualTo("PENDING");
        assertThat(response.getAmountCents()).isEqualTo(2500L);
        assertThat(response.getCurrency()).isEqualTo("USD");
    }

    @Test
    void returnsNotFoundWhenPaymentDoesNotExist() {
        GetPaymentStatusResponse response = stub.getPaymentStatus(
                GetPaymentStatusRequest.newBuilder()
                        .setPaymentId(UUID.randomUUID().toString())
                        .build()
        );

        assertThat(response.getFound()).isFalse();
    }

    @Test
    void returnsNotFoundForInvalidUuid() {
        GetPaymentStatusResponse response = stub.getPaymentStatus(
                GetPaymentStatusRequest.newBuilder()
                        .setPaymentId("not-a-valid-uuid")
                        .build()
        );

        assertThat(response.getFound()).isFalse();
    }
}