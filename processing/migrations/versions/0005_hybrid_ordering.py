"""Add durable admission cursor and alert resequencer ledger for Hybrid Ordering."""

from alembic import op

revision = "processing_0005_hybrid_ordering"
down_revision = "processing_0004_catalog"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    connection.exec_driver_sql("""
        CREATE TABLE processing_admission_cursor (
            consumer_group TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
            last_admitted_sequence NUMERIC NOT NULL DEFAULT -1,
            last_admitted_event_id TEXT,
            next_admission_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_admission_seq > 0),
            alert_frontier_seq BIGINT NOT NULL DEFAULT 0 CHECK (alert_frontier_seq >= 0),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (consumer_group, machine_id)
        );
        CREATE TABLE processing_alert_ledger (
            consumer_group TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            admission_seq BIGINT NOT NULL CHECK (admission_seq > 0),
            topic TEXT NOT NULL,
            message_id TEXT NOT NULL,
            source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
            source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
            sequence_number NUMERIC NOT NULL CHECK (
                sequence_number > 0 AND sequence_number < 'Infinity'::numeric
                AND sequence_number = trunc(sequence_number)
            ),
            stream_epoch TEXT NOT NULL,
            identity_sha256 TEXT NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
            disposition TEXT NOT NULL CHECK (
                disposition IN (
                    'PENDING',
                    'SIGNAL_READY',
                    'NO_SIGNAL',
                    'BLOCKED',
                    'EMITTED',
                    'RESOLVED_NO_SIGNAL'
                )
            ),
            disposition_reason TEXT,
            signal_id TEXT,
            signal_payload BYTEA,
            signal_sha256 TEXT CHECK (signal_sha256 IS NULL OR signal_sha256 ~ '^[0-9a-f]{64}$'),
            outcome_kind TEXT,
            admitted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            decided_at TIMESTAMPTZ,
            resolved_at TIMESTAMPTZ,
            operator_actor TEXT,
            operator_note TEXT,
            PRIMARY KEY (consumer_group, machine_id, admission_seq),
            UNIQUE (consumer_group, topic, message_id)
        );
        CREATE INDEX processing_alert_ledger_unresolved
            ON processing_alert_ledger(consumer_group, machine_id, admission_seq)
            WHERE disposition IN ('PENDING', 'SIGNAL_READY', 'NO_SIGNAL', 'BLOCKED');
        CREATE INDEX processing_alert_ledger_blocked
            ON processing_alert_ledger(consumer_group, machine_id, decided_at)
            WHERE disposition = 'BLOCKED';
    """)


def downgrade():
    raise RuntimeError("Hybrid ordering schema requires reviewed drain/reconciliation")
