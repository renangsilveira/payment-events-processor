package com.renan.paymentevents;

import org.springframework.boot.SpringApplication;

public class TestPaymentEventsProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.from(PaymentEventsProcessorApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
