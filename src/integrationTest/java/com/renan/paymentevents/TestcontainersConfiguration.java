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
import java.time.Duration;

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
		return new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest"));
	}

	@Bean
	public GenericContainer<?> schemaRegistryContainer(
			KafkaContainer kafkaContainer,
			Network testNetwork) {
		return new GenericContainer<>(
				DockerImageName.parse("confluentinc/cp-schema-registry:7.7.1"))
				.withNetwork(testNetwork)
				.withExposedPorts(8081)
				.withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
				.withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
				.withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
						"PLAINTEXT://kafka:9092")
				.waitingFor(Wait.forHttp("/subjects")
						.forStatusCode(200)
						.withStartupTimeout(Duration.ofSeconds(120)))
				.dependsOn(kafkaContainer);
	}

	@Bean
	public DynamicPropertyRegistrar schemaRegistryProperties(
			GenericContainer<?> schemaRegistryContainer) {
		return registry -> registry.add(
				"spring.kafka.properties.schema.registry.url",
				() -> "http://" + schemaRegistryContainer.getHost()
						+ ":" + schemaRegistryContainer.getMappedPort(8081)
		);
	}
}