package com.renan.paymentevents.api.common;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI paymentEventsProcessorOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Payment Events Processor API")
                        .description("Event-driven payment processing system demonstrating Outbox Pattern, "
                                + "consumer-side idempotency, and Kafka-based event streaming.")
                        .version("v0.0.1")
                        .license(new License().name("MIT")));
    }
}