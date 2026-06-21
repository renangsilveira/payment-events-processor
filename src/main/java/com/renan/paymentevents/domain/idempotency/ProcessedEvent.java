package com.renan.paymentevents.domain.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @CreationTimestamp
    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
        // required by JPA
    }

    private ProcessedEvent(UUID eventId) {
        this.eventId = eventId;
    }

    public static ProcessedEvent of(UUID eventId) {
        if (eventId == null) {
            throw new IllegalArgumentException("eventId must not be null");
        }
        return new ProcessedEvent(eventId);
    }

    public UUID getEventId() {
        return eventId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProcessedEvent that)) return false;
        return eventId != null && eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventId);
    }
}