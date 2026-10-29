"""Add bounded latest projections and owner-local messaging schema."""

import hashlib

from alembic import op
from maintai_messaging.contracts import default_contracts

revision = "business_0002_projections"
down_revision = "business_0001_registry"
branch_labels = None
depends_on = None


def upgrade():
    frozen = (default_contracts().directory / "migrations/001_messaging.sql").read_bytes()
    if hashlib.sha256(frozen).hexdigest() != (
        "12d2de438c1ebffb1169e19a777728d7b1953ee867101c90afcc09aa8ebea0f6"
    ):
        raise RuntimeError("Frozen SDK migration001 checksum differs")
    connection = op.get_bind()
    connection.exec_driver_sql(frozen.decode())
    for table in ("latest_telemetry", "latest_processing", "latest_valid_prediction"):
        connection.exec_driver_sql(f"""CREATE TABLE {table} (
            machine_id TEXT PRIMARY KEY REFERENCES machines(machine_id),
            source_event_id TEXT NOT NULL,
            stream_epoch TEXT NOT NULL,
            sequence_number NUMERIC NOT NULL CHECK (
                sequence_number>0 AND sequence_number<'Infinity'::numeric
                AND sequence_number=trunc(sequence_number)),
            identity_sha256 TEXT NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{{64}}$'),
            snapshot BYTEA NOT NULL,
            metadata BYTEA,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )""")
    connection.exec_driver_sql("""CREATE TABLE business_source_inbox (
        machine_id TEXT NOT NULL REFERENCES machines(machine_id),
        sequence_number NUMERIC NOT NULL CHECK (
            sequence_number>0 AND sequence_number<'Infinity'::numeric
            AND sequence_number=trunc(sequence_number)),
        source_event_id TEXT NOT NULL UNIQUE,
        stream_epoch TEXT NOT NULL,
        reading_sha256 TEXT CHECK (reading_sha256 ~ '^[0-9a-f]{64}$'),
        outcome_sha256 TEXT CHECK (outcome_sha256 ~ '^[0-9a-f]{64}$'),
        validation_status TEXT CHECK (validation_status IN ('VALID','INVALID')),
        PRIMARY KEY(machine_id,sequence_number)
    )""")
    connection.exec_driver_sql("""CREATE TABLE business_projection_conflicts (
        consumer_group TEXT NOT NULL,
        topic TEXT NOT NULL,
        source_partition INTEGER NOT NULL CHECK (source_partition>=0),
        source_offset BIGINT NOT NULL CHECK (source_offset>=0),
        message_sha256 TEXT NOT NULL CHECK (message_sha256 ~ '^[0-9a-f]{64}$'),
        error_code TEXT NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY(consumer_group,topic,source_partition,source_offset,message_sha256)
    )""")


def downgrade():
    raise RuntimeError("Projection downgrade requires a reviewed data migration")
