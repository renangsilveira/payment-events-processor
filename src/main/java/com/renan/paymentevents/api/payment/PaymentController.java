package com.renan.paymentevents.api.payment;

import com.renan.paymentevents.domain.payment.Payment;
import com.renan.paymentevents.domain.payment.PaymentService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> createPayment(
            @Valid @RequestBody PaymentRequest request,
            @RequestHeader(IDEMPOTENCY_KEY_HEADER) String idempotencyKey
    ) {
        Payment payment = paymentService.createPayment(request, idempotencyKey);
        PaymentResponse response = PaymentResponse.from(payment);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}