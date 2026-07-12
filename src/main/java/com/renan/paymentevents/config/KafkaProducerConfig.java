package com.renan.paymentevents.config;

import com.renan.paymentevents.avro.PaymentEvent;
import com.renan.paymentevents.avro.PaymentResultEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    private final Environment environment;

    public KafkaProducerConfig(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public ProducerFactory<String, PaymentEvent> paymentEventProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProps());
    }

    @Bean("defaultRetryTopicKafkaTemplate")
    public KafkaTemplate<String, PaymentEvent> kafkaTemplate(
            ProducerFactory<String, PaymentEvent> paymentEventProducerFactory) {
        return new KafkaTemplate<>(paymentEventProducerFactory);
    }

    @Bean
    public ProducerFactory<String, PaymentResultEvent> paymentResultEventProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProps());
    }

    @Bean
    public KafkaTemplate<String, PaymentResultEvent> paymentResultKafkaTemplate(
            ProducerFactory<String, PaymentResultEvent> paymentResultEventProducerFactory) {
        return new KafkaTemplate<>(paymentResultEventProducerFactory);
    }

    private Map<String, Object> producerProps() {
        // Read schema.registry.url at bean creation time via Environment,
        // which is resolved AFTER DynamicPropertyRegistrar beans have run.
        // This allows Testcontainers to override the URL before the producer
        // factory is created, fixing the test isolation issue.
        String schemaRegistryUrl = environment.getRequiredProperty(
                "spring.kafka.properties.schema.registry.url");
        String bootstrapServers = environment.getRequiredProperty(
                "spring.kafka.bootstrap-servers");

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        return config;
    }
}