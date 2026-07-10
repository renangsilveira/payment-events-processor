package com.renan.paymentevents;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;

@TestConfiguration(proxyBeanMethods = false)
public class LightweightTestcontainersConfiguration {

    @Bean
    @ServiceConnection
    public KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));
    }

    @Bean
    @ServiceConnection
    public PostgreSQLContainer<?> postgresContainer() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest"));
    }

    @Bean
    public DynamicPropertyRegistrar kafkaStreamsStateDir() {
        return registry -> {
            try {
                String tempDir = Files.createTempDirectory("kafka-streams-test-").toString();
                registry.add("spring.kafka.streams.state-dir", () -> tempDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create temp dir for Kafka Streams state", e);
            }
        };
    }
}