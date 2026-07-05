package com.renan.paymentevents.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class PaymentStatsAggregateSerde implements Serde<PaymentStatsAggregate> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Serializer<PaymentStatsAggregate> serializer() {
        return (topic, data) -> {
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize PaymentStatsAggregate", e);
            }
        };
    }

    @Override
    public Deserializer<PaymentStatsAggregate> deserializer() {
        return (topic, data) -> {
            try {
                return MAPPER.readValue(data, PaymentStatsAggregate.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize PaymentStatsAggregate", e);
            }
        };
    }
}