"""Business-owned lifecycle, immutable evidence/audit and transactional notifications."""

from alembic import op

revision = "business_0003_alerts"
down_revision = "business_0002_projections"
branch_labels = None
depends_on = None


def upgrade():
    op.get_bind().exec_driver_sql("""
        CREATE TABLE alerts (
            alert_id TEXT PRIMARY KEY,
            machine_id TEXT NOT NULL REFERENCES machines(machine_id),
            severity TEXT NOT NULL CHECK(severity IN ('WATCH','HIGH','CRITICAL')),
            status TEXT NOT NULL CHECK(status IN
                ('OPEN','ACKNOWLEDGED','IN_PROGRESS','ESCALATED','RESOLVED','FALSE_ALERT')),
            risk_score DOUBLE PRECISION NOT NULL CHECK(risk_score BETWEEN 0 AND 1),
            model_version TEXT NOT NULL, policy_version TEXT NOT NULL,
            source_event_id TEXT NOT NULL, prediction_id TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
            latest_signal_id TEXT NOT NULL, latest_metadata BYTEA NOT NULL
        );
        CREATE INDEX alerts_page ON alerts(created_at DESC,alert_id DESC);
        CREATE INDEX alerts_active_dedupe
            ON alerts(machine_id,severity,created_at DESC,alert_id DESC)
            WHERE status IN ('OPEN','ACKNOWLEDGED','IN_PROGRESS','ESCALATED');
        CREATE TABLE alert_evidence_snapshots (
            alert_id TEXT PRIMARY KEY REFERENCES alerts(alert_id), evidence BYTEA NOT NULL,
            evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256 ~ '^[0-9a-f]{64}$')
        );
        CREATE TABLE alert_signal_links (
            signal_id TEXT PRIMARY KEY, alert_id TEXT NOT NULL REFERENCES alerts(alert_id),
            identity_sha256 TEXT NOT NULL CHECK(identity_sha256 ~ '^[0-9a-f]{64}$'),
            result_id TEXT NOT NULL, source_event_id TEXT NOT NULL,
            disposition TEXT NOT NULL CHECK(disposition IN ('CREATED','DEDUPLICATED')),
            recorded_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE alert_events (
            alert_event_id TEXT PRIMARY KEY, alert_id TEXT NOT NULL REFERENCES alerts(alert_id),
            event_type TEXT NOT NULL CHECK(event_type IN
                ('CREATED','ACKNOWLEDGED','IN_PROGRESS','ESCALATED','RESOLVED','FALSE_ALERT')),
            from_status TEXT, to_status TEXT NOT NULL, actor_id BYTEA, note BYTEA,
            details BYTEA NOT NULL, timestamp TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX alert_timeline ON alert_events(alert_id,timestamp,alert_event_id);
        CREATE TABLE feedback (
            feedback_id TEXT PRIMARY KEY, alert_id TEXT NOT NULL REFERENCES alerts(alert_id),
            actor_id BYTEA NOT NULL, payload BYTEA NOT NULL, recorded_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX feedback_alert ON feedback(alert_id,recorded_at,feedback_id);
        CREATE TABLE alert_notifications (
            notification_id TEXT PRIMARY KEY,
            machine_id TEXT NOT NULL REFERENCES machines(machine_id),
            alert_id TEXT NOT NULL REFERENCES alerts(alert_id), event BYTEA NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX alert_notifications_machine ON alert_notifications(machine_id,created_at);
    """)


def downgrade():
    raise RuntimeError("Alert evidence/audit downgrade requires a reviewed retention migration")
