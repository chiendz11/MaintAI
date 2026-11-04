"""Verified Drift metadata and immutable list membership snapshots."""

from alembic import op

revision = "business_0006_drift_catalog"
down_revision = "business_0005_copilot"
branch_labels = None
depends_on = None


def upgrade():
    op.get_bind().exec_driver_sql("""
    CREATE TABLE drift_catalog (
        report_id TEXT PRIMARY KEY, computed_at TIMESTAMPTZ NOT NULL,
        model_version TEXT NOT NULL, overall_status TEXT NOT NULL,
        manifest BYTEA NOT NULL, manifest_sha256 TEXT NOT NULL,
        window_end_at TIMESTAMPTZ NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    CREATE INDEX drift_catalog_order ON drift_catalog(computed_at DESC,report_id DESC);
    CREATE TABLE drift_catalog_state (
        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
        last_scan_at TIMESTAMPTZ, complete_scan_at TIMESTAMPTZ, error_code TEXT
    );
    INSERT INTO drift_catalog_state(singleton) VALUES(TRUE);
    CREATE TABLE drift_scan_progress (
        prefix TEXT PRIMARY KEY, scan_id TEXT NOT NULL, continuation TEXT,
        done BOOLEAN NOT NULL DEFAULT FALSE, has_errors BOOLEAN NOT NULL DEFAULT FALSE
    );
    CREATE TABLE drift_catalog_errors (
        object_key TEXT PRIMARY KEY, error_code TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    CREATE TABLE drift_list_snapshots (
        snapshot_id TEXT PRIMARY KEY, binding TEXT NOT NULL,
        catalog_as_of TIMESTAMPTZ NOT NULL, stale BOOLEAN NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()+interval '10 minutes'
    );
    CREATE INDEX drift_snapshot_expiry ON drift_list_snapshots(expires_at);
    CREATE TABLE drift_list_members (
        snapshot_id TEXT NOT NULL REFERENCES drift_list_snapshots(snapshot_id),
        position BIGINT NOT NULL, manifest BYTEA NOT NULL, PRIMARY KEY(snapshot_id,position)
    );
    """)


def downgrade():
    raise RuntimeError("Catalog snapshots require an explicit retention migration")
