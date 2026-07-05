package com.renan.paymentevents.streams;

import com.renan.paymentevents.avro.PaymentEvent;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentStreamsTopologyTest {

    private static final String MOCK_SCHEMA_REGISTRY_SCOPE = "payment-streams-test";
    private static final String MOCK_SCHEMA_REGISTRY_URL =
            "mock://" + MOCK_SCHEMA_REGISTRY_SCOPE;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, PaymentEvent> inputTopic;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payment-streams-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        PaymentStreamsTopology topology = new PaymentStreamsTopology();
        topology.setSchemaRegistryUrl(MOCK_SCHEMA_REGISTRY_URL);

        StreamsBuilder builder = new StreamsBuilder();
        topology.paymentStatsStream(builder);

        testDriver = new TopologyTestDriver(builder.build(), props);

        Serde<PaymentEvent> paymentEventSerde = new SpecificAvroSerde<>();
        paymentEventSerde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL
        ), false);

        inputTopic = testDriver.createInputTopic(
                "payment.events.v1",
                Serdes.String().serializer(),
                paymentEventSerde.serializer()
        );
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_SCOPE);
    }

    @Test
    void aggregatesPaymentEventsByCurrencyInWindow() {
        Instant now = Instant.now();

        inputTopic.pipeInput("key-1", buildEvent("USD", 1500L), now);
        inputTopic.pipeInput("key-2", buildEvent("USD", 2000L), now.plusSeconds(5));
        inputTopic.pipeInput("key-3", buildEvent("BRL", 5000L), now.plusSeconds(10));
        inputTopic.pipeInput("key-sentinel", buildEvent("USD", 1L), now.plusSeconds(70));

        ReadOnlyWindowStore<String, PaymentStatsAggregate> store =
                testDriver.getWindowStore(PaymentStreamsTopology.PAYMENT_STATS_STORE);

        long usdCount = 0;
        long usdTotal = 0;
        long brlCount = 0;
        long brlTotal = 0;

        // Use store.all() instead of fetchAll() to avoid wall-clock alignment issues
        // with TopologyTestDriver — the store has data but window timestamps may differ
        // from Instant.now() calculated at test start.
        try (KeyValueIterator<Windowed<String>, PaymentStatsAggregate> it = store.all()) {
            while (it.hasNext()) {
                var entry = it.next();
                if ("USD".equals(entry.key.key())) {
                    usdCount = Math.max(usdCount, entry.value.getCount());
                    usdTotal = Math.max(usdTotal, entry.value.getTotalAmountCents());
                } else if ("BRL".equals(entry.key.key())) {
                    brlCount = Math.max(brlCount, entry.value.getCount());
                    brlTotal = Math.max(brlTotal, entry.value.getTotalAmountCents());
                }
            }
        }

        // With hopping window (size=60s, advance=30s), events fall into multiple
        // overlapping windows — we take the max across windows to get the "peak" count
        assertThat(usdCount).isGreaterThanOrEqualTo(2);
        assertThat(usdTotal).isGreaterThanOrEqualTo(3500L);
        assertThat(brlCount).isGreaterThanOrEqualTo(1);
        assertThat(brlTotal).isGreaterThanOrEqualTo(5000L);
    }

    @Test
    void producesZeroAggregationWhenNoEventsReceived() {
        ReadOnlyWindowStore<String, PaymentStatsAggregate> store =
                testDriver.getWindowStore(PaymentStreamsTopology.PAYMENT_STATS_STORE);

        try (KeyValueIterator<Windowed<String>, PaymentStatsAggregate> it =
                     store.fetchAll(Instant.now().minusSeconds(60), Instant.now())) {
            assertThat(it.hasNext()).isFalse();
        }
    }

    private PaymentEvent buildEvent(String currency, long amountCents) {
        return PaymentEvent.newBuilder()
                .setPaymentId(UUID.randomUUID().toString())
                .setAmountCents(amountCents)
                .setCurrency(currency)
                .setIdempotencyKey(UUID.randomUUID().toString())
                .setStatus("PENDING")
                .setOccurredAt(Instant.now())
                .build();
    }
}