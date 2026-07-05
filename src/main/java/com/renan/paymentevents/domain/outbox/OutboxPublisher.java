package com.renan.paymentevents.domain.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.renan.paymentevents.avro.PaymentEvent;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final String TOPIC = "payment.events.v1";
    private static final int MAX_RETRIES = 3;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           KafkaTemplate<String, PaymentEvent> kafkaTemplate,
                           ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${outbox.publisher.fixed-delay-ms:5000}")
    @SchedulerLock(
            name = "OutboxPublisher_publishPendingEvents",
            lockAtLeastFor = "PT4S",
            lockAtMostFor = "PT30S"
    )
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatus(OutboxStatus.PENDING);

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("OutboxPublisher: found {} pending event(s) to publish", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                PaymentEvent avroEvent = toAvroEvent(event);
                kafkaTemplate.send(TOPIC, event.getAggregateId().toString(), avroEvent).get();
                event.markPublished();
                log.info("OutboxPublisher: published event {} for aggregate {}",
                        event.getId(), event.getAggregateId());
            } catch (Exception ex) {
                log.error("OutboxPublisher: failed to publish event {} (retry {}/{}): {}",
                        event.getId(), event.getRetryCount() + 1, MAX_RETRIES, ex.getMessage());
                event.markFailed();

                if (event.getRetryCount() >= MAX_RETRIES) {
                    log.error("OutboxPublisher: event {} exceeded max retries, marking as FAILED permanently",
                            event.getId());
                }
            }
        }
    }

    private PaymentEvent toAvroEvent(OutboxEvent event) throws JsonProcessingException {
        JsonNode payload = objectMapper.readTree(event.getPayload());

        return PaymentEvent.newBuilder()
                .setPaymentId(payload.get("paymentId").asText())
                .setAmountCents(payload.get("amountCents").asLong())
                .setCurrency(payload.get("currency").asText())
                .setIdempotencyKey(payload.get("idempotencyKey").asText())
                .setStatus("PENDING")
                .setOccurredAt(Instant.now())
                .build();
    }
}