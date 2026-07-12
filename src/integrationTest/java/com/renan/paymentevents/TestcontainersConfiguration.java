package com.renan.paymentevents;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.UUID;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	@Bean
	public Network testNetwork() {
		return Network.newNetwork();
	}

	@Bean
	@ServiceConnection
	public KafkaContainer kafkaContainer(Network testNetwork) {
		return new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"))
				.withNetwork(testNetwork)
				.withNetworkAliases("kafka");
	}

	@Bean
	@ServiceConnection
	public PostgreSQLContainer<?> postgresContainer() {
		return new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest"))
				.withStartupTimeout(Duration.ofSeconds(120));
	}

	@Bean
	public GenericContainer<?> schemaRegistryContainer(Network testNetwork) {
		return new GenericContainer<>(
				DockerImageName.parse("apicurio/apicurio-registry-mem:2.6.2.Final"))
				.withNetwork(testNetwork)
				.withExposedPorts(8080)
				.waitingFor(Wait.forHttp("/apis/ccompat/v7/subjects")
						.forStatusCode(200)
						.withStartupTimeout(Duration.ofSeconds(60)))
				.withNetworkAliases("schema-registry");
	}

	@Bean
	public DynamicPropertyRegistrar schemaRegistryProperties(
			GenericContainer<?> schemaRegistryContainer) {
		return registry -> registry.add(
				"spring.kafka.properties.schema.registry.url",
				() -> "http://" + schemaRegistryContainer.getHost()
						+ ":" + schemaRegistryContainer.getMappedPort(8080)
						+ "/apis/ccompat/v7"
		);
	}

	@Bean
	public DynamicPropertyRegistrar kafkaStreamsProperties() {
		return registry -> {
			try {
				String tempDir = Files.createTempDirectory("kafka-streams-test-").toString();
				String uniqueAppId = "payment-events-streams-test-" + UUID.randomUUID();
				registry.add("spring.kafka.streams.state-dir", () -> tempDir);
				registry.add("spring.kafka.streams.application-id", () -> uniqueAppId);
			} catch (IOException e) {
				throw new RuntimeException("Failed to configure Kafka Streams state dir", e);
			}
		};
	}
}