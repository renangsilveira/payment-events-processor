package com.renan.paymentevents.api.payment;

import com.renan.paymentevents.TestcontainersConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = {"grpc.server.port=0"})
@AutoConfigureMockMvc
@Transactional
class PaymentControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void createsPaymentSuccessfullyAndReturns201() throws Exception {
        PaymentRequest request = new PaymentRequest(new BigDecimal("15.00"), "USD");
        String idempotencyKey = UUID.randomUUID().toString();

        mockMvc.perform(post("/payments")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.amount").value(15.00))
                .andExpect(jsonPath("$.currency").value("USD"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.idempotencyKey").value(idempotencyKey));
    }

    @Test
    void returnsSamePaymentWhenIdempotencyKeyRepeatedWithSamePayload() throws Exception {
        PaymentRequest request = new PaymentRequest(new BigDecimal("20.00"), "USD");
        String idempotencyKey = UUID.randomUUID().toString();

        mockMvc.perform(post("/payments")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/payments")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.idempotencyKey").value(idempotencyKey));
    }

    @Test
    void returns409WhenIdempotencyKeyRepeatedWithDifferentPayload() throws Exception {
        String idempotencyKey = UUID.randomUUID().toString();

        mockMvc.perform(post("/payments")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(new PaymentRequest(new BigDecimal("10.00"), "USD"))))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/payments")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(new PaymentRequest(new BigDecimal("99.00"), "USD"))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("Idempotency Conflict"));
    }

    @Test
    void returns400WhenAmountIsZero() throws Exception {
        PaymentRequest request = new PaymentRequest(new BigDecimal("0.00"), "USD");

        mockMvc.perform(post("/payments")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation Failed"));
    }

    @Test
    void returns400WhenCurrencyIsInvalid() throws Exception {
        PaymentRequest request = new PaymentRequest(new BigDecimal("15.00"), "usd");

        mockMvc.perform(post("/payments")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void returns400WhenIdempotencyKeyHeaderIsMissing() throws Exception {
        PaymentRequest request = new PaymentRequest(new BigDecimal("15.00"), "USD");

        mockMvc.perform(post("/payments")
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }
}