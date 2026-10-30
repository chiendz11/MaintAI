"""Resumable native Storage delta index; existing snapshot memberships stay intact."""

from alembic import op
from psycopg import sql

from processing.shared.history_meta.persistence import metadata_schema

revision = "processing_0004_catalog"
down_revision = "processing_0003_history"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    namespace = metadata_schema(bind.exec_driver_sql("SELECT current_schema()").scalar_one())
    schema = sql.Identifier(namespace).as_string()
    bind.exec_driver_sql(f"""
        CREATE TABLE {schema}.native_catalog_heads (
            source_key TEXT PRIMARY KEY, active_id TEXT, next_sequence BIGINT NOT NULL DEFAULT 0,
            blocked BOOLEAN NOT NULL DEFAULT FALSE, lease_owner TEXT,
            lease_until TIMESTAMPTZ, fence BIGINT NOT NULL DEFAULT 0
        );
        CREATE TABLE {schema}.native_catalog_imports (
            source_key TEXT NOT NULL REFERENCES {schema}.native_catalog_heads(source_key),
            generation_id TEXT NOT NULL, import_sequence BIGINT NOT NULL,
            manifest BYTEA NOT NULL, reference BYTEA NOT NULL, anchor_id TEXT,
            next_ref BYTEA, segment_index INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL CHECK(state IN ('BUILDING','READY')),
            PRIMARY KEY(source_key,generation_id), UNIQUE(source_key,import_sequence)
        );
        CREATE UNIQUE INDEX one_native_build ON {schema}.native_catalog_imports(source_key)
            WHERE state='BUILDING';
        CREATE TABLE {schema}.native_catalog_visited (
            source_key TEXT NOT NULL, target_id TEXT NOT NULL, generation_id TEXT NOT NULL,
            reference BYTEA NOT NULL, finished BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY(source_key,target_id,generation_id),
            FOREIGN KEY(source_key,target_id)
                REFERENCES {schema}.native_catalog_imports(source_key,generation_id)
        );
        CREATE TABLE {schema}.native_catalog_receipts (
            source_key TEXT NOT NULL REFERENCES {schema}.native_catalog_heads(source_key),
            receipt_id TEXT NOT NULL, first_sequence BIGINT NOT NULL,
            machine_id TEXT, received_at TIMESTAMPTZ, topic TEXT NOT NULL,
            partition INTEGER NOT NULL, source_offset BIGINT NOT NULL, payload BYTEA NOT NULL,
            PRIMARY KEY(source_key,receipt_id)
        );
        CREATE INDEX native_catalog_timeline ON {schema}.native_catalog_receipts
            (source_key,machine_id,received_at DESC,topic DESC,partition DESC,source_offset DESC);
    """)


def downgrade():
    raise RuntimeError("Native catalog checkpoints require reviewed recovery")
