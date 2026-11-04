"""Durable on-demand Copilot jobs and immutable response evidence."""

from alembic import op

revision = "business_0005_copilot"
down_revision = "business_0004_history_markers"
branch_labels = None
depends_on = None


def upgrade():
    op.get_bind().exec_driver_sql("""
    CREATE TABLE brief_jobs (
        job_id TEXT PRIMARY KEY, alert_id TEXT NOT NULL REFERENCES alerts(alert_id),
        machine_id TEXT NOT NULL REFERENCES machines(machine_id), actor_id TEXT NOT NULL,
        intent TEXT NOT NULL CHECK(intent IN
            ('MAINTENANCE_BRIEF','TECHNICIAN_CHECKLIST','ALERT_EXPLANATION')),
        evidence_sha256 TEXT NOT NULL, input BYTEA NOT NULL,
        input_sha256 TEXT NOT NULL CHECK(input_sha256 ~ '^[0-9a-f]{64}$'),
        status TEXT NOT NULL CHECK(status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','EXPIRED')),
        attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        deadline TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()+interval '5 minutes',
        next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        lease_owner TEXT, lease_until TIMESTAMPTZ, fencing_token BIGINT NOT NULL DEFAULT 0,
        error_code TEXT, output BYTEA,
        CHECK((status='SUCCEEDED')=(output IS NOT NULL))
    );
    CREATE UNIQUE INDEX brief_active ON brief_jobs(alert_id,intent,evidence_sha256)
        WHERE status IN ('QUEUED','RUNNING');
    CREATE INDEX brief_claim ON brief_jobs(next_attempt_at,created_at)
        WHERE status IN ('QUEUED','RUNNING');
    CREATE TABLE brief_idempotency (
        actor_id TEXT NOT NULL, key_hash TEXT NOT NULL, request_hash TEXT NOT NULL,
        job_id TEXT NOT NULL REFERENCES brief_jobs(job_id), PRIMARY KEY(actor_id,key_hash)
    );
    CREATE TABLE brief_facades (
        alert_id TEXT PRIMARY KEY REFERENCES alerts(alert_id),
        job_id TEXT NOT NULL REFERENCES brief_jobs(job_id)
    );
    CREATE TABLE brief_job_audit (
        audit_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES brief_jobs(job_id),
        event_type TEXT NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    CREATE TABLE business_m7_notifications (
        notification_id TEXT PRIMARY KEY, machine_id TEXT REFERENCES machines(machine_id),
        event BYTEA NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    """)


def downgrade():
    raise RuntimeError("Copilot evidence removal requires a reviewed retention migration")
