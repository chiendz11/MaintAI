"""Stable Business-owned composite chart/marker snapshots."""

from alembic import op

revision = "business_0004_history_markers"
down_revision = "business_0003_alerts"
branch_labels = None
depends_on = None


def upgrade():
    op.get_bind().exec_driver_sql("""
        CREATE TABLE history_marker_snapshots (
            snapshot_id TEXT PRIMARY KEY, actor TEXT NOT NULL,
            scope_sha256 TEXT NOT NULL, machine_id TEXT NOT NULL,
            query BYTEA NOT NULL, payload BYTEA NOT NULL, payload_sha256 TEXT NOT NULL,
            as_of TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX history_marker_expiry ON history_marker_snapshots(expires_at);
        CREATE INDEX alerts_history_machine_time ON alerts(machine_id,created_at,alert_id);
        CREATE INDEX alerts_history_machine_prediction
            ON alerts(machine_id,prediction_id,created_at);
    """)


def downgrade():
    raise RuntimeError("History metadata downgrade requires explicit draining")
