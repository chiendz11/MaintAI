"""Add additive source-sequence guard metadata on alerts and signal links."""

from alembic import op

revision = "business_0008_alert_seq_guard"
down_revision = "business_0007_rebuild"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    connection.exec_driver_sql("""
        ALTER TABLE alerts
            ADD COLUMN IF NOT EXISTS latest_source_sequence NUMERIC CHECK (
                latest_source_sequence IS NULL OR (
                    latest_source_sequence > 0
                    AND latest_source_sequence < 'Infinity'::numeric
                    AND latest_source_sequence = trunc(latest_source_sequence)
                )
            ),
            ADD COLUMN IF NOT EXISTS latest_source_stream_epoch TEXT;
        CREATE INDEX IF NOT EXISTS alerts_machine_latest_seq
            ON alerts(machine_id, severity, latest_source_sequence DESC)
            WHERE latest_source_sequence IS NOT NULL;
        DO $$
        DECLARE
            constraint_name TEXT;
        BEGIN
            FOR constraint_name IN
                SELECT c.conname
                FROM pg_constraint c
                JOIN pg_class r ON r.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = r.relnamespace
                WHERE n.nspname = current_schema()
                  AND r.relname = 'alert_signal_links'
                  AND c.contype = 'c'
                  AND strpos(pg_get_constraintdef(c.oid), 'disposition') > 0
            LOOP
                EXECUTE 'ALTER TABLE alert_signal_links DROP CONSTRAINT ' || quote_ident(constraint_name);
            END LOOP;
        END $$;
        ALTER TABLE alert_signal_links
            ADD CONSTRAINT alert_signal_links_disposition_check CHECK (
                disposition IN ('CREATED', 'DEDUPLICATED', 'STALE_IGNORED', 'STALE_AFTER_TERMINAL')
            );
    """)


def downgrade():
    raise RuntimeError("Alert sequence guard metadata requires a reviewed retention migration")
