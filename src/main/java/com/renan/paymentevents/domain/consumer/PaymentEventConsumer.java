package com.renan.paymentevents.domain.consumer;

import com.renan.paymentevents.avro.PaymentEvent;
import com.renan.paymentevents.avro.PaymentResultEvent;
import com.renan.paymentevents.domain.idempotency.ProcessedEvent;
import com.renan.paymentevents.domain.idempotency.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);
    private static final String RESULTS_TOPIC = "payment.results.v1";

    private final ProcessedEventRepository processedEventRepository;
    private final PaymentResultPublisher paymentResultPublisher;

    public PaymentEventConsumer(ProcessedEventRepository processedEventRepository,
                                PaymentResultPublisher paymentResultPublisher) {
        this.processedEventRepository = processedEventRepository;
        this.paymentResultPublisher = paymentResultPublisher;
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = "-dlt"
    )
    @KafkaListener(topics = "payment.events.v1", groupId = "payment-processor")
    public void consume(ConsumerRecord<String, PaymentEvent> record,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        PaymentEvent event = record.value();
        UUID eventId = UUID.nameUUIDFromBytes(
                (record.topic() + "-" + record.partition() + "-" + record.offset()).getBytes()
        );

        log.info("PaymentEventConsumer: received event for payment {} from topic {}",
                event.getPaymentId(), topic);

        if (isDuplicate(eventId)) {
            log.info("PaymentEventConsumer: duplicate event {} — skipping", eventId);
            return;
        }

        try {
            processedEventRepository.save(ProcessedEvent.of(eventId));
        } catch (DataIntegrityViolationException ex) {
            log.warn("PaymentEventConsumer: concurrent duplicate detected for event {} — skipping", eventId);
            return;
        }

        PaymentResultEvent result = PaymentResultEvent.newBuilder()
                .setPaymentId(event.getPaymentId())
                .setStatus("AUTHORIZED")
                .setResult("SUCCESS")
                .setErrorMessage(null)
                .setOccurredAt(Instant.now())
                .build();

        paymentResultPublisher.publish(event.getPaymentId().toString(), result);

        log.info("PaymentEventConsumer: processed payment {} successfully", event.getPaymentId());
    }

    private boolean isDuplicate(UUID eventId) {
        return processedEventRepository.existsById(eventId);
    }
}