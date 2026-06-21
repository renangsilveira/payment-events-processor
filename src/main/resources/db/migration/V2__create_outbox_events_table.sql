CREATE TABLE outbox_events (
                               id            UUID PRIMARY KEY,
                               aggregate_id  UUID NOT NULL,
                               event_type    VARCHAR(100) NOT NULL,
                               payload       JSONB NOT NULL,
                               status        VARCHAR(20) NOT NULL,
                               retry_count   INTEGER NOT NULL DEFAULT 0,
                               created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                               published_at  TIMESTAMPTZ
);

CREATE INDEX idx_outbox_events_status ON outbox_events (status);
CREATE INDEX idx_outbox_events_aggregate_id ON outbox_events (aggregate_id);