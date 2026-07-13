package com.renan.paymentevents.domain.consumer;

import com.renan.paymentevents.TestcontainersConfiguration;
import com.renan.paymentevents.avro.PaymentEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentEventConsumerDlqIntegrationTest {

    // Replaced @Autowired KafkaContainer with @Value to avoid depending on
    // the container as a Spring bean (which would let Spring stop the static container).
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @SpyBean
    private PaymentResultPublisher paymentResultPublisher;

    @Test
    void routesMessageToDltAfterMaxRetries() throws InterruptedException {
        doThrow(new RuntimeException("Simulated downstream failure"))
                .when(paymentResultPublisher)
                .publish(anyString(), any());

        String paymentId = UUID.randomUUID().toString();

        PaymentEvent event = PaymentEvent.newBuilder()
                .setPaymentId(paymentId)
                .setAmountCents(7500L)
                .setCurrency("USD")
                .setIdempotencyKey(UUID.randomUUID().toString())
                .setStatus("PENDING")
                .setOccurredAt(Instant.now())
                .build();

        // Wait 3 seconds before publishing to ensure the @KafkaListener consumer
        // (with auto.offset.reset=latest) has completed rebalancing and is
        // actively subscribed to the partition. Without this, the message might
        // arrive before the consumer is assigned and be missed entirely.
        Thread.sleep(3_000);

        publishToKafka("payment.events.v1", paymentId, event);

        // Wait for all retries to exhaust:
        // 4 attempts × backoffs (1s, 2s, 4s) + rebalancing overhead = ~60s
        Thread.sleep(60_000);

        List<PaymentEvent> dltMessages = consumeFromDlt(bootstrapServers, schemaRegistryUrl, paymentId);

        assertThat(dltMessages)
                .filteredOn(e -> e.getPaymentId().toString().equals(paymentId))
                .isNotEmpty();
    }

    private void publishToKafka(String topic, String key, PaymentEvent event) {
        try (KafkaProducer<String, PaymentEvent> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", schemaRegistryUrl
        ))) {
            producer.send(new ProducerRecord<>(topic, key, event));
            producer.flush();
        }
    }

    private List<PaymentEvent> consumeFromDlt(String bootstrapServers, String schemaRegistryUrl,
                                              String targetPaymentId) {
        KafkaConsumer<String, PaymentEvent> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-dlt-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true
        ));

        consumer.subscribe(List.of("payment.events.v1-dlt"));

        List<PaymentEvent> results = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, PaymentEvent> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(r -> results.add(r.value()));
            boolean found = results.stream()
                    .anyMatch(e -> e.getPaymentId().toString().equals(targetPaymentId));
            if (found) break;
        }

        consumer.close();
        return results;
    }
}
