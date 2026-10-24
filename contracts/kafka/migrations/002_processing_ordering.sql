-- Additive, explicit Processing-owner migration. 001 remains immutable.
-- Deployment must drain old writers/relays before explicitly activating this mode.
CREATE TABLE IF NOT EXISTS maintai_processing_capabilities (
    capability TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT FALSE
);
INSERT INTO maintai_processing_capabilities VALUES ('proc-order-1', FALSE)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS maintai_processing_assignments (
    consumer_group TEXT NOT NULL,
    topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    owner TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    active BOOLEAN NOT NULL,
    PRIMARY KEY (consumer_group, topic, source_partition)
);
CREATE TABLE IF NOT EXISTS maintai_processing_machine_cursor (
    consumer_group TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    last_sequence BIGINT NOT NULL DEFAULT -1,
    last_event_id TEXT,
    PRIMARY KEY (consumer_group, machine_id)
);
CREATE TABLE IF NOT EXISTS maintai_outbox_keys (
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    last_allocated BIGINT NOT NULL DEFAULT 0,
    last_published BIGINT NOT NULL DEFAULT 0,
    owner TEXT,
    generation BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ,
    PRIMARY KEY (topic, message_key),
    CHECK (last_allocated >= last_published AND last_published >= 0)
);
ALTER TABLE maintai_kafka_outbox ADD COLUMN IF NOT EXISTS dispatch_seq BIGINT;
ALTER TABLE maintai_kafka_outbox ADD COLUMN IF NOT EXISTS source_sequence_number BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS maintai_outbox_dispatch_order
ON maintai_kafka_outbox(topic, message_key, dispatch_seq) WHERE dispatch_seq IS NOT NULL;

-- Fail closed for old binaries: leases/row locks alone do not prevent a legacy
-- publisher bypassing ordered dispatch after activation. Business owns a different DB.
CREATE OR REPLACE FUNCTION maintai_require_processing_writer()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM maintai_processing_capabilities
               WHERE capability='proc-order-1' AND enabled)
       AND current_setting('maintai.processing_writer', TRUE) IS DISTINCT FROM 'proc-order-1'
    THEN
        RAISE EXCEPTION 'Processing ordering requires a compatible writer'
            USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS maintai_ordered_outbox_gate ON maintai_kafka_outbox;
CREATE TRIGGER maintai_ordered_outbox_gate BEFORE INSERT OR UPDATE ON maintai_kafka_outbox
FOR EACH ROW EXECUTE FUNCTION maintai_require_processing_writer();
DROP TRIGGER IF EXISTS maintai_ordered_inbox_gate ON maintai_kafka_inbox;
CREATE TRIGGER maintai_ordered_inbox_gate BEFORE INSERT OR UPDATE ON maintai_kafka_inbox
FOR EACH ROW EXECUTE FUNCTION maintai_require_processing_writer();
