import org.gradle.testing.jacoco.tasks.JacocoCoverageVerification

buildscript {
	repositories {
		gradlePluginPortal()
		mavenCentral()
	}
	dependencies {
		classpath("com.google.protobuf:protobuf-gradle-plugin:0.9.4")
	}
}

plugins {
	java
	id("org.springframework.boot") version "3.4.1"
	id("io.spring.dependency-management") version "1.1.7"
	id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
	jacoco
}

apply(plugin = "com.google.protobuf")

group = "com.renan"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

configurations {
	compileOnly {
		extendsFrom(configurations.annotationProcessor.get())
	}
}

repositories {
	mavenCentral()
	maven { url = uri("https://packages.confluent.io/maven/") }
}

extra["confluentVersion"] = "7.7.1"
extra["testcontainersVersion"] = "1.20.4"

dependencyManagement {
	imports {
		mavenBom("org.testcontainers:testcontainers-bom:${property("testcontainersVersion")}")
	}
}

dependencies {
	// Core
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.springframework.boot:spring-boot-starter-data-jpa")
	implementation("org.springframework.boot:spring-boot-starter-validation")
	implementation("org.springframework.boot:spring-boot-starter-actuator")

	// Distributed scheduling lock
	implementation("net.javacrumbs.shedlock:shedlock-spring:7.7.0")
	implementation("net.javacrumbs.shedlock:shedlock-provider-jdbc-template:7.7.0")

	// API Documentation
	implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:2.8.14")

	// Kafka
	implementation("org.springframework.kafka:spring-kafka")

	// Kafka Streams
	implementation("org.apache.kafka:kafka-streams")
	implementation("io.confluent:kafka-streams-avro-serde:${property("confluentVersion")}")
	testImplementation("org.apache.kafka:kafka-streams-test-utils")

	// Avro + Schema Registry (Confluent)
	implementation("org.apache.avro:avro:1.12.0")
	implementation("io.confluent:kafka-avro-serializer:${property("confluentVersion")}") {
		exclude(group = "org.apache.kafka", module = "kafka-clients")
	}
	implementation("io.confluent:kafka-schema-registry-client:${property("confluentVersion")}") {
		exclude(group = "junit", module = "junit")
		exclude(group = "org.mockito", module = "mockito-core")
	}

	// Database
	implementation("org.flywaydb:flyway-core")
	implementation("org.flywaydb:flyway-database-postgresql")
	implementation("io.hypersistence:hypersistence-utils-hibernate-63:3.15.3")
	runtimeOnly("org.postgresql:postgresql")

	// Observability
	implementation("io.micrometer:micrometer-registry-prometheus")

	// Lombok
	compileOnly("org.projectlombok:lombok")
	annotationProcessor("org.projectlombok:lombok")

	// gRPC
	implementation("net.devh:grpc-server-spring-boot-starter:3.1.0.RELEASE")
	implementation("io.grpc:grpc-stub:1.63.0")
	implementation("io.grpc:grpc-protobuf:1.63.0")
	compileOnly("org.apache.tomcat:annotations-api:6.0.53")

	// Resilience4j
	implementation("io.github.resilience4j:resilience4j-spring-boot3:2.3.0")
	implementation("org.springframework.boot:spring-boot-starter-aop")

	// Tests
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testImplementation("org.testcontainers:junit-jupiter")
	testImplementation("org.testcontainers:postgresql")
	testImplementation("org.testcontainers:kafka")
	testImplementation("org.springframework.boot:spring-boot-testcontainers")
	testImplementation("io.grpc:grpc-testing:1.63.0")
	testImplementation("net.devh:grpc-client-spring-boot-starter:3.1.0.RELEASE")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

avro {
	setGettersReturnOptional(true)
	setOptionalGettersForNullableFieldsOnly(true)
	setFieldVisibility("PRIVATE")
}

extensions.configure<com.google.protobuf.gradle.ProtobufExtension> {
	protoc {
		artifact = "com.google.protobuf:protoc:3.25.5"
	}
	plugins {
		create("grpc") {
			artifact = "io.grpc:protoc-gen-grpc-java:1.68.1"
		}
	}
	generateProtoTasks {
		all().configureEach {
			plugins {
				create("grpc")
			}
		}
	}
}

sourceSets {
	main {
		java {
			srcDir("build/generated-main-avro-java")
			srcDir("build/generated/source/proto/main/grpc")
			srcDir("build/generated/source/proto/main/java")
		}
	}
	create("integrationTest") {
		java {
			srcDir("src/integrationTest/java")
		}
		resources {
			srcDir("src/integrationTest/resources")
		}
		compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
		runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
	}
}

tasks.named<ProcessResources>("processIntegrationTestResources") {
	duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

val integrationTestImplementation by configurations.getting {
	extendsFrom(configurations.testImplementation.get())
}

val integrationTestRuntimeOnly by configurations.getting {
	extendsFrom(configurations.testRuntimeOnly.get())
}

configurations["integrationTestImplementation"].extendsFrom(configurations["testImplementation"])
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])

val integrationTest = tasks.register<Test>("integrationTest") {
	description = "Runs integration tests (requires Docker; intended for CI)."
	group = "verification"
	testClassesDirs = sourceSets["integrationTest"].output.classesDirs
	classpath = sourceSets["integrationTest"].runtimeClasspath
	useJUnitPlatform()
	shouldRunAfter(tasks.test)
	finalizedBy("jacocoIntegrationTestReport")
}

// ─── JaCoCo ────────────────────────────────────────────────────────────────

val jacocoExclusions = listOf(
	"com/renan/paymentevents/grpc/**",        // protobuf-generated stubs
	"com/renan/paymentevents/avro/**",         // avro-generated classes
	"com/renan/paymentevents/PaymentEventsProcessorApplication.class"
)

tasks.withType<Test> {
	useJUnitPlatform()
	finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
	dependsOn(tasks.test)
	reports {
		xml.required.set(true)
		html.required.set(true)
	}
	classDirectories.setFrom(files(classDirectories.files.map {
		fileTree(it) { exclude(jacocoExclusions) }
	}))
}

tasks.register<JacocoReport>("jacocoIntegrationTestReport") {
	executionData.setFrom(fileTree(layout.buildDirectory) {
		include("jacoco/integrationTest.exec")
	})
	sourceSets(sourceSets.main.get())
	reports {
		xml.required.set(true)
		html.required.set(true)
	}
	classDirectories.setFrom(files(classDirectories.files.map {
		fileTree(it) { exclude(jacocoExclusions) }
	}))
}

tasks.register<JacocoReport>("jacocoMergedReport") {
	executionData.setFrom(fileTree(layout.buildDirectory) {
		include("jacoco/test.exec", "jacoco/integrationTest.exec")
	})
	sourceSets(sourceSets.main.get())
	reports {
		xml.required.set(true)
		html.required.set(true)
	}
	classDirectories.setFrom(files(classDirectories.files.map {
		fileTree(it) { exclude(jacocoExclusions) }
	}))
}

tasks.register<JacocoCoverageVerification>("jacocoMergedCoverageVerification") {
	dependsOn("jacocoMergedReport")
	executionData.setFrom(fileTree(layout.buildDirectory) {
		include("jacoco/test.exec", "jacoco/integrationTest.exec")
	})
	sourceSets(sourceSets.main.get())
	classDirectories.setFrom(files(classDirectories.files.map {
		fileTree(it) { exclude(jacocoExclusions) }
	}))
	violationRules {
		rule {
			limit {
				minimum = "0.80".toBigDecimal()
			}
		}
	}
}

tasks.check {
	// integrationTest is intentionally NOT wired into check.
	// jacocoMergedCoverageVerification is wired in CI explicitly (Phase 10).
}