package com.renan.paymentevents.api.payment;

import com.renan.paymentevents.streams.PaymentStatsAggregate;
import com.renan.paymentevents.streams.PaymentStreamsTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/payments")
public class PaymentStatsController {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public PaymentStatsController(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @GetMapping("/stats")
    public ResponseEntity<PaymentStatsResponse> getStats() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();

        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            return ResponseEntity.status(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        ReadOnlyWindowStore<String, PaymentStatsAggregate> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        PaymentStreamsTopology.PAYMENT_STATS_STORE,
                        QueryableStoreTypes.windowStore()
                )
        );

        // NOTE: querying all windows from the last 2 minutes to cover the hopping window size.
        // In a multi-instance deployment, this would only return results for partitions
        // assigned to this instance — full aggregation would require RPC to other instances
        // via KafkaStreams.streamsMetadataForStore().
        Instant now = Instant.now();
        Instant from = now.minusSeconds(120);

        List<PaymentStatsResponse.CurrencyStats> statsList = new ArrayList<>();

        try (KeyValueIterator<Windowed<String>, PaymentStatsAggregate> iterator =
                     store.fetchAll(from, now)) {
            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, PaymentStatsAggregate> entry = iterator.next();
                statsList.add(new PaymentStatsResponse.CurrencyStats(
                        entry.key.key(),
                        entry.value.getCount(),
                        entry.value.getTotalAmountCents(),
                        entry.key.window().startTime().toString(),
                        entry.key.window().endTime().toString()
                ));
            }
        }

        return ResponseEntity.ok(new PaymentStatsResponse(statsList));
    }
}