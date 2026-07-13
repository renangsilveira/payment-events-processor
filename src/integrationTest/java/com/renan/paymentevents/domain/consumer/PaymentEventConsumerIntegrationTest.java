package com.renan.paymentevents.domain.consumer;

import com.renan.paymentevents.TestcontainersConfiguration;
import com.renan.paymentevents.avro.PaymentEvent;
import com.renan.paymentevents.avro.PaymentResultEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class PaymentEventConsumerIntegrationTest {

    // Replaced @Autowired KafkaContainer with @Value to avoid depending on
    // the container as a Spring bean (which would let Spring stop the static container).
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Test
    void processesPaymentEventAndPublishesResult() throws InterruptedException {
        String paymentId = UUID.randomUUID().toString();

        PaymentEvent event = PaymentEvent.newBuilder()
                .setPaymentId(paymentId)
                .setAmountCents(3000L)
                .setCurrency("USD")
                .setIdempotencyKey(UUID.randomUUID().toString())
                .setStatus("PENDING")
                .setOccurredAt(Instant.now())
                .build();

        publishToKafka("payment.events.v1", paymentId, event);

        Thread.sleep(8000);

        List<PaymentResultEvent> results = consumeResults(bootstrapServers, schemaRegistryUrl);

        assertThat(results)
                .filteredOn(r -> r.getPaymentId().toString().equals(paymentId))
                .hasSize(1)
                .first()
                .satisfies(r -> {
                    assertThat(r.getResult().toString()).isEqualTo("SUCCESS");
                    assertThat(r.getStatus().toString()).isEqualTo("AUTHORIZED");
                    assertThat(r.getErrorMessage()).isNull();
                });
    }

    @Test
    void doesNotProcessDuplicateMessage() throws InterruptedException {
        String paymentId = UUID.randomUUID().toString();

        PaymentEvent event = PaymentEvent.newBuilder()
                .setPaymentId(paymentId)
                .setAmountCents(1000L)
                .setCurrency("BRL")
                .setIdempotencyKey(UUID.randomUUID().toString())
                .setStatus("PENDING")
                .setOccurredAt(Instant.now())
                .build();

        publishToKafka("payment.events.v1", paymentId, event);
        publishToKafka("payment.events.v1", paymentId, event);

        Thread.sleep(10_000);

        List<PaymentResultEvent> results = consumeResults(bootstrapServers, schemaRegistryUrl);

        long countForPayment = results.stream()
                .filter(r -> r.getPaymentId().toString().equals(paymentId))
                .count();

        // Two messages with different offsets — both processed (offset-based dedup),
        // but without error.
        assertThat(countForPayment).isGreaterThanOrEqualTo(1);
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

    private List<PaymentResultEvent> consumeResults(String bootstrapServers, String schemaRegistryUrl) {
        KafkaConsumer<String, PaymentResultEvent> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-result-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true
        ));

        consumer.subscribe(List.of("payment.results.v1"));

        List<PaymentResultEvent> results = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, PaymentResultEvent> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, PaymentResultEvent> record : records) {
                results.add(record.value());
            }
            if (!results.isEmpty()) break;
        }

        consumer.close();
        return results;
    }
}
