package com.renan.paymentevents.domain.consumer;

import com.renan.paymentevents.avro.PaymentResultEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentResultPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentResultPublisher.class);
    private static final String RESULTS_TOPIC = "payment.results.v1";

    private final KafkaTemplate<String, PaymentResultEvent> kafkaTemplate;

    public PaymentResultPublisher(KafkaTemplate<String, PaymentResultEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String key, PaymentResultEvent event) {
        kafkaTemplate.send(RESULTS_TOPIC, key, event);
        log.info("PaymentResultPublisher: published result for payment {}", key);
    }
}