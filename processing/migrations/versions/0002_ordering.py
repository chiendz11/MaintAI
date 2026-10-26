"""Add ordering schema without activating or guessing legacy source cursors."""

from alembic import op
from maintai_messaging.contracts import default_contracts

revision = "processing_0002_ordering"
down_revision = "processing_0001"
branch_labels = None
depends_on = None


def upgrade():
    contracts = default_contracts()
    if contracts.bundle_version != "1.1.0":
        raise RuntimeError("Ordering revision requires reviewed bundle1.1.0")
    connection = op.get_bind()
    connection.exec_driver_sql(
        (contracts.directory / "migrations/002_processing_ordering.sql").read_text()
    )
    # Source JSON integers and owner history were never limited to signed64.
    # Additive owner adjustment, without changing either frozen SDK SQL file.
    connection.exec_driver_sql("""ALTER TABLE maintai_processing_machine_cursor
        ALTER COLUMN last_sequence TYPE NUMERIC;
        ALTER TABLE maintai_kafka_outbox ALTER COLUMN source_sequence_number TYPE NUMERIC;""")
    connection.exec_driver_sql("""ALTER TABLE maintai_processing_capabilities
        ADD CONSTRAINT processing_capability_identity CHECK (capability='proc-order-1');
        CREATE TABLE processing_source_cursor (
            machine_id TEXT PRIMARY KEY,
            topic TEXT NOT NULL,
            source_partition INTEGER NOT NULL,
            last_sequence NUMERIC NOT NULL DEFAULT -1,
            last_event_id TEXT
        );""")


def downgrade():
    raise RuntimeError("Ordering downgrade requires a reviewed drain/reconciliation migration")
