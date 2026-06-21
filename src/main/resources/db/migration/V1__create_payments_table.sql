CREATE TABLE payments (
                          id              UUID PRIMARY KEY,
                          amount_cents    BIGINT NOT NULL CHECK (amount_cents > 0),
                          currency        VARCHAR(3) NOT NULL,
                          status          VARCHAR(20) NOT NULL,
                          idempotency_key VARCHAR(255) NOT NULL,
                          version         BIGINT NOT NULL DEFAULT 0,
                          created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                          updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

                          CONSTRAINT uq_payments_idempotency_key UNIQUE (idempotency_key)
);

CREATE INDEX idx_payments_status ON payments (status);