ALTER TABLE payments
    ADD COLUMN request_fingerprint VARCHAR(64) NOT NULL DEFAULT '';

ALTER TABLE payments
    ALTER COLUMN request_fingerprint DROP DEFAULT;