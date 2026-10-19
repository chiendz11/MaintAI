-- Apply explicitly through the Processing or Business owner's migration job.
-- No cross-database access. Never run automatically on application startup.
CREATE TABLE IF NOT EXISTS maintai_kafka_inbox (
    consumer_group TEXT NOT NULL,
    topic TEXT NOT NULL,
    message_id TEXT NOT NULL,
    identity_sha256 TEXT NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    schema_version TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_group, topic, message_id)
);

CREATE TABLE IF NOT EXISTS maintai_kafka_outbox (
    topic TEXT NOT NULL CHECK (topic IN ('inference.results', 'alert.signals')),
    message_id TEXT NOT NULL,
    message_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'LEASED', 'PUBLISHED')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ,
    PRIMARY KEY (topic, message_id)
);

CREATE INDEX IF NOT EXISTS maintai_kafka_outbox_pending
    ON maintai_kafka_outbox (next_attempt_at, created_at)
    WHERE status != 'PUBLISHED';
