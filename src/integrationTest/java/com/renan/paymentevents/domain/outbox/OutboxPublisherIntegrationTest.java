package com.renan.paymentevents.domain.outbox;

import com.renan.paymentevents.TestcontainersConfiguration;
import com.renan.paymentevents.avro.PaymentEvent;
import com.renan.paymentevents.domain.payment.Payment;
import com.renan.paymentevents.domain.payment.PaymentRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class OutboxPublisherIntegrationTest {

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private KafkaContainer kafkaContainer;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Test
    void publishesPendingOutboxEventToKafkaTopic() throws InterruptedException {
        String idempotencyKey = UUID.randomUUID().toString();
        String fingerprint = "test-fingerprint-" + UUID.randomUUID();

        Payment payment = Payment.createPending(2500L, "USD", idempotencyKey, fingerprint);
        Payment savedPayment = paymentRepository.save(payment);

        OutboxEvent event = OutboxEvent.createPending(
                savedPayment.getId(),
                "PaymentCreated",
                """
                {"paymentId":"%s","amountCents":2500,"currency":"USD","idempotencyKey":"%s"}"""
                        .formatted(savedPayment.getId(), idempotencyKey)
        );
        outboxEventRepository.save(event);

        // Wait for OutboxPublisher to poll and publish (configured at 5s delay)
        Thread.sleep(10_000);

        List<PaymentEvent> received = consumeFromKafka(kafkaContainer.getBootstrapServers(), schemaRegistryUrl);

        PaymentEvent published = received.stream()
                .filter(e -> e.getPaymentId().equals(savedPayment.getId().toString()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Expected event for payment " + savedPayment.getId() + " not found in topic"));

        assertThat(published.getAmountCents()).isEqualTo(2500L);
        assertThat(published.getCurrency().toString()).isEqualTo("USD");
        assertThat(published.getStatus().toString()).isEqualTo("PENDING");
        assertThat(published.getIdempotencyKey().toString()).isEqualTo(idempotencyKey);
    }

    private List<PaymentEvent> consumeFromKafka(String bootstrapServers, String schemaRegistryUrl) {
        KafkaConsumer<String, PaymentEvent> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true
        ));

        consumer.subscribe(List.of("payment.events.v1"));

        List<PaymentEvent> results = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, PaymentEvent> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, PaymentEvent> record : records) {
                results.add(record.value());
            }
            if (!results.isEmpty()) break;
        }

        consumer.close();
        return results;
    }
}