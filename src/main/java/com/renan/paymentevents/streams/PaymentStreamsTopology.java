package com.renan.paymentevents.streams;

import com.renan.paymentevents.avro.PaymentEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class PaymentStreamsTopology {

    public static final String PAYMENT_STATS_STORE = "payment-stats-by-currency";
    private static final String INPUT_TOPIC = "payment.events.v1";

    private static final Duration WINDOW_SIZE = Duration.ofSeconds(60);
    private static final Duration ADVANCE_BY = Duration.ofSeconds(30);
    private static final Duration GRACE_PERIOD = Duration.ofSeconds(5);

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KStream<String, PaymentEvent> paymentStatsStream(StreamsBuilder streamsBuilder) {
        Serde<PaymentEvent> paymentEventSerde = buildPaymentEventSerde();

        KStream<String, PaymentEvent> stream = streamsBuilder.stream(
                INPUT_TOPIC,
                Consumed.with(Serdes.String(), paymentEventSerde)
        );

        stream
                .groupBy(
                        (key, event) -> event.getCurrency().toString(),
                        Grouped.with(Serdes.String(), paymentEventSerde)
                )
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE_PERIOD)
                                .advanceBy(ADVANCE_BY)
                )
                .aggregate(
                        PaymentStatsAggregate::new,
                        (currency, event, aggregate) -> aggregate.increment(event.getAmountCents()),
                        Materialized.<String, PaymentStatsAggregate, WindowStore<Bytes, byte[]>>as(PAYMENT_STATS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new PaymentStatsAggregateSerde())
                )
                .toStream()
                .foreach((windowedKey, aggregate) ->
                        // NOTE: single-instance query only. In a multi-instance deployment,
                        // Interactive Queries would need RPC between instances to aggregate
                        // results from all partitions. See KafkaStreams.metadataForAllStreamsClients().
                        System.out.printf("Window [%s - %s] currency=%s count=%d totalCents=%d%n",
                                windowedKey.window().startTime(),
                                windowedKey.window().endTime(),
                                windowedKey.key(),
                                aggregate.getCount(),
                                aggregate.getTotalAmountCents())
                );

        return stream;
    }

    private Serde<PaymentEvent> buildPaymentEventSerde() {
        SpecificAvroSerde<PaymentEvent> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);
        return serde;
    }
}