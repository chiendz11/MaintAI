"""Add M6 metadata without granting source writes to the history runtime."""

from alembic import op
from psycopg import sql

from processing.shared.history_meta.persistence import metadata_schema

revision = "processing_0003_history"
down_revision = "processing_0002_ordering"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    source = bind.exec_driver_sql("SELECT current_schema()").scalar_one()
    namespace = metadata_schema(source)
    quoted = sql.Identifier(namespace).as_string()
    bind.exec_driver_sql(f"CREATE SCHEMA {quoted}")
    bind.exec_driver_sql(f"""
        CREATE TABLE {quoted}.snapshots (
            snapshot_id TEXT PRIMARY KEY, actor TEXT NOT NULL, scope_sha256 TEXT NOT NULL,
            query_sha256 TEXT NOT NULL, query BYTEA NOT NULL,
            collection TEXT NOT NULL CHECK(collection IN ('PROCESSING','RAW')),
            state TEXT NOT NULL CHECK(state IN ('BUILDING','READY','FAILED')),
            catalog_generation TEXT, coverage BYTEA NOT NULL,
            as_of TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0 CHECK(row_count>=0),
            CHECK(expires_at>as_of)
        );
        CREATE TABLE {quoted}.snapshot_members (
            snapshot_id TEXT NOT NULL REFERENCES {quoted}.snapshots(snapshot_id),
            ordinal INTEGER NOT NULL CHECK(ordinal>=0), entry_id TEXT NOT NULL,
            payload BYTEA NOT NULL, payload_sha256 TEXT NOT NULL,
            markers BYTEA NOT NULL DEFAULT convert_to('{{"items":[]}}','UTF8'),
            PRIMARY KEY(snapshot_id,ordinal), UNIQUE(snapshot_id,entry_id)
        );
        CREATE INDEX snapshot_expiry ON {quoted}.snapshots(expires_at,snapshot_id)
            WHERE row_count>0;
        CREATE TABLE {quoted}.export_jobs (
            job_id TEXT PRIMARY KEY, actor TEXT NOT NULL, scope_sha256 TEXT NOT NULL,
            idempotency_key TEXT NOT NULL, request_sha256 TEXT NOT NULL, request BYTEA NOT NULL,
            snapshot_id TEXT NOT NULL REFERENCES {quoted}.snapshots(snapshot_id),
            status TEXT NOT NULL
                CHECK(status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','EXPIRED')),
            mode TEXT NOT NULL CHECK(mode IN ('STREAM','JOB')),
            attempts INTEGER NOT NULL DEFAULT 0, fencing_token BIGINT NOT NULL DEFAULT 0,
            lease_owner TEXT, lease_until TIMESTAMPTZ, next_attempt_at TIMESTAMPTZ NOT NULL,
            deadline TIMESTAMPTZ NOT NULL, artifact_ref BYTEA, error_code TEXT,
            holds_input BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ, UNIQUE(actor,idempotency_key)
        );
        CREATE INDEX export_jobs_claim ON {quoted}.export_jobs(status,next_attempt_at,lease_until);
        CREATE INDEX export_snapshot_holds ON {quoted}.export_jobs(snapshot_id)
            WHERE holds_input;
        CREATE TABLE {quoted}.raw_catalog_sync_state (
            generation_id TEXT PRIMARY KEY, manifest BYTEA NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('BUILDING','ACTIVE','RETIRED','FAILED')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        );
        CREATE UNIQUE INDEX one_active_catalog ON {quoted}.raw_catalog_sync_state(state)
            WHERE state='ACTIVE';
        CREATE TABLE {quoted}.raw_catalog_entries (
            generation_id TEXT NOT NULL REFERENCES {quoted}.raw_catalog_sync_state(generation_id),
            receipt_id TEXT NOT NULL, machine_id TEXT, received_at TIMESTAMPTZ,
            topic TEXT NOT NULL, partition INTEGER NOT NULL, source_offset BIGINT NOT NULL,
            payload BYTEA NOT NULL, PRIMARY KEY(generation_id,receipt_id)
        );
        CREATE INDEX raw_catalog_timeline ON {quoted}.raw_catalog_entries
            (generation_id,machine_id,received_at DESC,topic DESC,
             partition DESC,source_offset DESC);
        CREATE INDEX processing_results_completed_history
            ON processing_results(completed_at DESC,result_id DESC);
    """)


def downgrade():
    raise RuntimeError("History snapshots and durable jobs require reviewed retention/recovery")
