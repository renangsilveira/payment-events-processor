package com.renan.paymentevents;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

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
			Network testNetwork,
			DynamicPropertyRegistry registry) {
		GenericContainer<?> schemaRegistry = new GenericContainer<>(
				DockerImageName.parse("confluentinc/cp-schema-registry:7.7.1"))
				.withNetwork(testNetwork)
				.withExposedPorts(8081)
				.withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
				.withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
				.withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
						"PLAINTEXT://kafka:9092")
				.waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
				.dependsOn(kafkaContainer);

		registry.add("spring.kafka.properties.schema.registry.url",
				() -> "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));

		return schemaRegistry;
	}
}