plugins {
	java
	id("org.springframework.boot") version "3.4.1"
	id("io.spring.dependency-management") version "1.1.7"
	id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
	jacoco
}

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

	// Kafka
	implementation("org.springframework.kafka:spring-kafka")

	// Avro + Schema Registry (Confluent) — regular dependencies, NOT a BOM
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
	runtimeOnly("org.postgresql:postgresql")

	// Observability (Micrometer core now; Prometheus registry added in Phase 9)
	implementation("io.micrometer:micrometer-registry-prometheus")

	// Lombok (optional but speeds up entity/DTO boilerplate — remove if you prefer not to use it)
	compileOnly("org.projectlombok:lombok")
	annotationProcessor("org.projectlombok:lombok")

	// Tests
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testImplementation("org.testcontainers:junit-jupiter")
	testImplementation("org.testcontainers:postgresql")
	testImplementation("org.testcontainers:kafka")
	testImplementation("org.springframework.boot:spring-boot-testcontainers")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

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
}

// NOTE: jacocoTestCoverageVerification gate is intentionally NOT wired in yet.
// It will be added in Phase 10, once there's enough code for an 80% threshold to be meaningful.

avro {
	setGettersReturnOptional(true)
	setOptionalGettersForNullableFieldsOnly(true)
	setFieldVisibility("PRIVATE")
}

sourceSets {
	main {
		java {
			srcDir("build/generated-main-avro-java")
		}
	}
}

sourceSets {
	main {
		java {
			srcDir("build/generated-main-avro-java")
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
}

tasks.check {
	// NOTE: integrationTest is intentionally NOT wired into `check` yet.
	// It runs only in CI via an explicit `./gradlew integrationTest` step (added in a later phase),
	// since Testcontainers is unreliable on Apple Silicon + Docker Desktop locally.
}