package com.renan.paymentevents.grpc;

import io.github.resilience4j.retry.Retry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

@Component
public class PaymentStatusGrpcClient {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusGrpcClient.class);

    private final ManagedChannel channel;
    private final PaymentStatusServiceGrpc.PaymentStatusServiceBlockingStub stub;
    private final Retry retry;

    public PaymentStatusGrpcClient(
            @Value("${grpc.client.payment-status.host:localhost}") String host,
            @Value("${grpc.client.payment-status.port:9090}") int port,
            Retry grpcRetry) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = PaymentStatusServiceGrpc.newBlockingStub(channel);
        this.retry = grpcRetry;
    }

    public GetPaymentStatusResponse getPaymentStatus(String paymentId) {
        GetPaymentStatusRequest request = GetPaymentStatusRequest.newBuilder()
                .setPaymentId(paymentId)
                .build();

        // Retry wraps the gRPC call — if the server returns a StatusRuntimeException
        // (e.g., UNAVAILABLE), the client retries up to 3 times with 500ms wait.
        // NOT_FOUND and INVALID_ARGUMENT are not retried (not StatusRuntimeException subtypes
        // that match our retry predicate), preserving fast-fail for known non-transient errors.
        return Retry.decorateCheckedSupplier(retry, () -> stub.getPaymentStatus(request))
                .unchecked()
                .get();
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("PaymentStatusGrpcClient: shutting down channel");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
}