package com.renan.paymentevents;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.UUID;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	// Static containers - started ONCE per JVM, shared across ALL test contexts.
	//
	// IMPORTANT: NOT exposed as @Bean / @ServiceConnection intentionally.
	// If returned as Spring beans, Spring calls container.stop() when any context
	// holding them is destroyed (e.g. @DirtiesContext), killing the static container
	// and breaking every subsequent test context in the same JVM.
	//
	// All connection properties are registered via DynamicPropertyRegistrar instead,
	// reading the already-running container coordinates without Spring managing lifecycle.

	static final Network SHARED_NETWORK = Network.newNetwork();

	@SuppressWarnings("resource")
	static final KafkaContainer KAFKA_CONTAINER =
			new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"))
					.withNetwork(SHARED_NETWORK)
					.withNetworkAliases("kafka")
					.withStartupTimeout(Duration.ofSeconds(120));

	@SuppressWarnings("resource")
	static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
			new PostgreSQLContainer<>(DockerImageName.parse("postgres:latest"))
					.withStartupTimeout(Duration.ofSeconds(120));

	@SuppressWarnings("resource")
	static final GenericContainer<?> SCHEMA_REGISTRY_CONTAINER =
			new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry-mem:2.6.2.Final"))
					.withNetwork(SHARED_NETWORK)
					.withExposedPorts(8080)
					.waitingFor(Wait.forHttp("/apis/ccompat/v7/subjects")
							.forStatusCode(200)
							.withStartupTimeout(Duration.ofSeconds(90)))
					.withNetworkAliases("schema-registry");

	static {
		Startables.deepStart(KAFKA_CONTAINER, POSTGRES_CONTAINER, SCHEMA_REGISTRY_CONTAINER).join();
	}

	@Bean
	DynamicPropertyRegistrar containerProperties() {
		return registry -> {
			// Kafka bootstrap servers
			registry.add("spring.kafka.bootstrap-servers",
					KAFKA_CONTAINER::getBootstrapServers);

			// Postgres — registered manually so Spring never touches container lifecycle
			registry.add("spring.datasource.url", POSTGRES_CONTAINER::getJdbcUrl);
			registry.add("spring.datasource.username", POSTGRES_CONTAINER::getUsername);
			registry.add("spring.datasource.password", POSTGRES_CONTAINER::getPassword);
			registry.add("spring.datasource.driver-class-name",
					() -> "org.postgresql.Driver");

			// HikariCP — minimum keepalive is 30000ms; HikariCP ignores values below that
			registry.add("spring.datasource.hikari.keepalive-time", () -> "30000");
			registry.add("spring.datasource.hikari.max-lifetime", () -> "120000");

			// Schema Registry (Apicurio Confluent compat endpoint)
			String schemaRegistryUrl = "http://" + SCHEMA_REGISTRY_CONTAINER.getHost()
					+ ":" + SCHEMA_REGISTRY_CONTAINER.getMappedPort(8080)
					+ "/apis/ccompat/v7";
			registry.add("spring.kafka.properties.schema.registry.url", () -> schemaRegistryUrl);
			registry.add("spring.kafka.producer.properties.schema.registry.url", () -> schemaRegistryUrl);

			// Unique consumer group per context — prevents rebalancing conflicts
			// when multiple Spring contexts share the same Kafka broker.
			String uniqueGroupId = "payment-processor-" + UUID.randomUUID();
			registry.add("spring.kafka.consumer.group-id", () -> uniqueGroupId);

			// Use 'latest' offset reset so @KafkaListener only processes messages
			// published AFTER the consumer subscribes. This prevents messages from
			// previous test contexts from interfering with the current test.
			registry.add("spring.kafka.consumer.auto-offset-reset", () -> "latest");

			// Kafka Streams — unique state dir and application-id per context
			try {
				String tempDir = Files.createTempDirectory("kafka-streams-test-").toString();
				String uniqueAppId = "payment-events-streams-test-" + UUID.randomUUID();
				registry.add("spring.kafka.streams.state-dir", () -> tempDir);
				registry.add("spring.kafka.streams.application-id", () -> uniqueAppId);
			} catch (IOException e) {
				throw new RuntimeException("Failed to configure Kafka Streams state dir", e);
			}

			// Random gRPC port prevents bind conflicts across contexts
			registry.add("grpc.server.port", () -> "0");
		};
	}
}
