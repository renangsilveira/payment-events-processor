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
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.UUID;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	// -------------------------------------------------------------------------
	// Static containers — started ONCE per JVM and shared across ALL contexts.
	//
	// Problem solved: when containers are declared as @Bean (non-static), each
	// Spring test context creates its own set of containers. The first context
	// pulls the images from scratch, which combined with startup time easily
	// exceeds the 60-second timeout for Kafka's KRaft recovery log message.
	// Subsequent contexts reuse the Docker layer cache and start faster, which
	// is why tests with a DIFFERENT context key (e.g. PaymentControllerIntegrationTest
	// with @AutoConfigureMockMvc) pass while the first context fails.
	//
	// With static containers, images are pulled and started once, Ryuk cleans
	// them up at JVM exit, and every Spring context simply autowires the already-
	// running container instances via the @Bean methods below.
	// -------------------------------------------------------------------------

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
		// Start all three containers in parallel for faster total startup time.
		Startables.deepStart(KAFKA_CONTAINER, POSTGRES_CONTAINER, SCHEMA_REGISTRY_CONTAINER).join();
	}

	@Bean
	@ServiceConnection
	KafkaContainer kafkaContainer() {
		return KAFKA_CONTAINER;
	}

	@Bean
	@ServiceConnection
	PostgreSQLContainer<?> postgresContainer() {
		return POSTGRES_CONTAINER;
	}

	@Bean
	DynamicPropertyRegistrar schemaRegistryProperties() {
		return registry -> registry.add(
				"spring.kafka.properties.schema.registry.url",
				() -> "http://" + SCHEMA_REGISTRY_CONTAINER.getHost()
						+ ":" + SCHEMA_REGISTRY_CONTAINER.getMappedPort(8080)
						+ "/apis/ccompat/v7"
		);
	}

	@Bean
	DynamicPropertyRegistrar testProperties() {
		return registry -> {
			try {
				String tempDir = Files.createTempDirectory("kafka-streams-test-").toString();
				String uniqueAppId = "payment-events-streams-test-" + UUID.randomUUID();
				registry.add("spring.kafka.streams.state-dir", () -> tempDir);
				registry.add("spring.kafka.streams.application-id", () -> uniqueAppId);
				registry.add("grpc.server.port", () -> "0");

				// Unique consumer group per context prevents rebalancing conflicts
				// when multiple Spring contexts run consumers in the same JVM process.
				String uniqueGroupId = "payment-processor-" + UUID.randomUUID();
				registry.add("spring.kafka.consumer.group-id", () -> uniqueGroupId);

				// Keep DB connections alive during long-running tests (e.g. DLQ test sleeps 60s).
				// Without this, Postgres closes idle connections and HikariCP can't reconnect.
				registry.add("spring.datasource.hikari.keepalive-time", () -> "15000");
				registry.add("spring.datasource.hikari.max-lifetime", () -> "120000");
			} catch (IOException e) {
				throw new RuntimeException("Failed to configure test properties", e);
			}
		};
	}
}