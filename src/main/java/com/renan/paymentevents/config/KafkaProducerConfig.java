package com.renan.paymentevents.config;

import com.renan.paymentevents.avro.PaymentEvent;
import com.renan.paymentevents.avro.PaymentResultEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProducerConfig {

    // NOTE: ProducerFactory beans are intentionally NOT declared here.
    // Spring Kafka auto-configuration creates them from application.properties,
    // which ensures the bootstrap-servers and schema.registry.url are resolved
    // lazily — after Testcontainers DynamicPropertyRegistrar beans have run.
    // Declaring them here would cause them to be created with the wrong URLs
    // during context initialization, before Testcontainers overrides take effect.

    @Bean("defaultRetryTopicKafkaTemplate")
    public KafkaTemplate<String, PaymentEvent> kafkaTemplate(
            ProducerFactory<String, PaymentEvent> paymentEventProducerFactory) {
        return new KafkaTemplate<>(paymentEventProducerFactory);
    }

    @Bean
    public KafkaTemplate<String, PaymentResultEvent> paymentResultKafkaTemplate(
            ProducerFactory<String, PaymentResultEvent> paymentResultEventProducerFactory) {
        return new KafkaTemplate<>(paymentResultEventProducerFactory);
    }
}