package com.renan.paymentevents.grpc;

import com.renan.paymentevents.domain.payment.Payment;
import com.renan.paymentevents.domain.payment.PaymentRepository;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.Optional;
import java.util.UUID;

@GrpcService
public class PaymentStatusGrpcService extends PaymentStatusServiceGrpc.PaymentStatusServiceImplBase {

    private final PaymentRepository paymentRepository;

    public PaymentStatusGrpcService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @Override
    public void getPaymentStatus(GetPaymentStatusRequest request,
                                 StreamObserver<GetPaymentStatusResponse> responseObserver) {
        String paymentId = request.getPaymentId();

        GetPaymentStatusResponse response;

        try {
            UUID uuid = UUID.fromString(paymentId);
            Optional<Payment> payment = paymentRepository.findById(uuid);

            if (payment.isPresent()) {
                Payment p = payment.get();
                response = GetPaymentStatusResponse.newBuilder()
                        .setPaymentId(p.getId().toString())
                        .setStatus(p.getStatus().name())
                        .setAmountCents(p.getAmountCents())
                        .setCurrency(p.getCurrency())
                        .setCreatedAt(p.getCreatedAt().toString())
                        .setFound(true)
                        .build();
            } else {
                response = GetPaymentStatusResponse.newBuilder()
                        .setPaymentId(paymentId)
                        .setFound(false)
                        .build();
            }
        } catch (IllegalArgumentException e) {
            response = GetPaymentStatusResponse.newBuilder()
                    .setPaymentId(paymentId)
                    .setFound(false)
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}