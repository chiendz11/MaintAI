"""Initial owner schema plus the exact frozen SDK inbox/outbox migration.

Revision ID: processing_0001
Revises: None
"""

import hashlib

from alembic import op
from maintai_messaging.contracts import default_contracts

revision = "processing_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    contracts = default_contracts()
    sql = (contracts.directory / "migrations/001_messaging.sql").read_bytes()
    # This historical schema remains byte-identical across bundle1.0/1.1.
    if (
        hashlib.sha256(sql).hexdigest()
        != "12d2de438c1ebffb1169e19a777728d7b1953ee867101c90afcc09aa8ebea0f6"
    ):
        raise RuntimeError("Frozen SDK migration001 checksum differs")
    connection = op.get_bind()
    connection.exec_driver_sql(sql.decode())
    connection.exec_driver_sql("""
        CREATE TABLE sensor_events (
            event_id TEXT PRIMARY KEY,
            machine_id TEXT NOT NULL,
            stream_epoch TEXT NOT NULL,
            sequence_number NUMERIC NOT NULL CHECK (
                sequence_number > 0 AND sequence_number < 'Infinity'::numeric
                AND sequence_number=trunc(sequence_number)),
            event_time TIMESTAMPTZ NOT NULL,
            source_server_accepted_at TIMESTAMPTZ NOT NULL,
            source_schema_version TEXT NOT NULL,
            source TEXT NOT NULL,
            raw_features_json JSONB CHECK (jsonb_typeof(raw_features_json)='object'),
            raw_features_wire BYTEA,
            CHECK ((raw_features_json IS NOT NULL) <> (raw_features_wire IS NOT NULL)),
            payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
            processing_status TEXT NOT NULL
                CHECK (processing_status IN ('PROCESSING','COMPLETED','INVALID','FAILED')),
            validation_status TEXT CHECK (validation_status IN ('VALID','INVALID')),
            validation_errors_json JSONB NOT NULL DEFAULT '[]',
            validator_version TEXT NOT NULL,
            dq_ruleset_sha256 TEXT NOT NULL CHECK (dq_ruleset_sha256 ~ '^[0-9a-f]{64}$'),
            correlation_id TEXT NOT NULL,
            kafka_topic TEXT NOT NULL,
            kafka_partition INTEGER NOT NULL CHECK (kafka_partition >= 0),
            kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CHECK (processing_status='PROCESSING' OR validation_status IS NOT NULL)
        );
        CREATE INDEX sensor_events_machine_source_time
            ON sensor_events(machine_id, source_server_accepted_at DESC, event_id DESC);
        CREATE TABLE predictions (
            prediction_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE REFERENCES sensor_events(event_id),
            label TEXT NOT NULL CHECK (label IN ('NORMAL','FAILURE')),
            failure_probability DOUBLE PRECISION NOT NULL
                CHECK (failure_probability BETWEEN 0 AND 1),
            model_threshold DOUBLE PRECISION NOT NULL CHECK (model_threshold BETWEEN 0 AND 1),
            model_version TEXT NOT NULL,
            preprocessor_version TEXT NOT NULL,
            model_identity JSONB NOT NULL CHECK (jsonb_typeof(model_identity)='object'),
            inference_ms DOUBLE PRECISION CHECK (inference_ms >= 0),
            predicted_at TIMESTAMPTZ NOT NULL,
            correlation_id TEXT NOT NULL,
            CHECK (label = CASE WHEN failure_probability >= model_threshold
                THEN 'FAILURE' ELSE 'NORMAL' END)
        );
        CREATE TABLE risk_decisions (
            risk_decision_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE REFERENCES sensor_events(event_id),
            prediction_id TEXT NOT NULL REFERENCES predictions(prediction_id),
            machine_state TEXT NOT NULL
                CHECK (machine_state IN ('NORMAL','WATCH','HIGH','CRITICAL')),
            severity_nullable TEXT CHECK (severity_nullable IN ('WATCH','HIGH','CRITICAL')),
            should_alert BOOLEAN NOT NULL,
            risk_score DOUBLE PRECISION NOT NULL CHECK (risk_score BETWEEN 0 AND 1),
            policy_version TEXT NOT NULL,
            policy_sha256 TEXT NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
            decided_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE processing_results (
            result_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE REFERENCES sensor_events(event_id),
            kind TEXT NOT NULL
                CHECK (kind IN ('PREDICTION_COMPLETED','DATA_INVALID','INFERENCE_UNAVAILABLE')),
            outcome BYTEA NOT NULL,
            outcome_sha256 TEXT NOT NULL CHECK (outcome_sha256 ~ '^[0-9a-f]{64}$'),
            completed_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE processing_conflicts (
            event_id TEXT NOT NULL,
            source_topic TEXT NOT NULL,
            source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
            source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
            existing_sha256 TEXT CHECK (existing_sha256 ~ '^[0-9a-f]{64}$'),
            incoming_sha256 TEXT NOT NULL CHECK (incoming_sha256 ~ '^[0-9a-f]{64}$'),
            error_code TEXT NOT NULL DEFAULT 'EVENT_CONFLICT',
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (event_id, source_topic, source_partition, source_offset, incoming_sha256)
        );
    """)


def downgrade():
    raise RuntimeError(
        "Processing evidence tables require an explicitly reviewed recovery migration"
    )
