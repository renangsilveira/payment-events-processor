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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.kafka.KafkaContainer;

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

    @Autowired
    private KafkaContainer kafkaContainer;

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

        publishToKafka("payment.events.v1", paymentId, event);

        // Wait long enough for all retries to exhaust:
        // 4 attempts × max 10s backoff = up to 40s, but typically much less
        Thread.sleep(35_000);

        List<PaymentEvent> dltMessages = consumeFromDlt(kafkaContainer.getBootstrapServers(), schemaRegistryUrl);

        assertThat(dltMessages)
                .filteredOn(e -> e.getPaymentId().toString().equals(paymentId))
                .isNotEmpty();
    }

    private void publishToKafka(String topic, String key, PaymentEvent event) {
        try (KafkaProducer<String, PaymentEvent> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", schemaRegistryUrl
        ))) {
            producer.send(new ProducerRecord<>(topic, key, event));
            producer.flush();
        }
    }

    private List<PaymentEvent> consumeFromDlt(String bootstrapServers, String schemaRegistryUrl) {
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
            if (!results.isEmpty()) break;
        }

        consumer.close();
        return results;
    }
}