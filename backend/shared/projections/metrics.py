"""Bounded recovery gauges; status/error enums never include source identifiers."""

from prometheus_client import Gauge

STATUSES = ("QUEUED", "RUNNING", "VERIFIED", "SUCCEEDED", "FAILED", "ABORTED")


class RebuildMetrics:
    def __init__(self, registry):
        self.jobs = Gauge(
            "maintai_projection_rebuild_jobs",
            "Durable rebuild jobs by status",
            ["status"],
            registry=registry,
        )
        self.age = Gauge(
            "maintai_projection_rebuild_oldest_age_seconds",
            "Oldest active rebuild age",
            registry=registry,
        )
        self.pending = Gauge(
            "maintai_projection_rebuild_pending_members",
            "Unapplied original source members",
            registry=registry,
        )
        self.bytes = Gauge(
            "maintai_projection_rebuild_journal_bytes",
            "Captured wire bytes for active rebuild",
            registry=registry,
        )
        for status in STATUSES:
            self.jobs.labels(status).set(0)

    def observe(self, factory):
        with factory() as connection:
            statuses = connection.execute(
                "SELECT status,count(*) AS n FROM projection_rebuild_jobs GROUP BY status"
            ).fetchall()
            active = connection.execute("""SELECT COALESCE(EXTRACT(EPOCH FROM
                clock_timestamp()-min(created_at)),0) AS age,
                COALESCE(sum(journal_bytes),0) AS bytes FROM projection_rebuild_jobs
                WHERE status IN ('QUEUED','RUNNING','VERIFIED')""").fetchone()
            pending = connection.execute("""SELECT count(*) AS n FROM projection_members m
                JOIN projection_rebuild_jobs j USING(job_id) WHERE NOT m.applied
                AND j.status IN ('QUEUED','RUNNING','VERIFIED')""").fetchone()["n"]
        counts = {row["status"]: row["n"] for row in statuses}
        for status in STATUSES:
            self.jobs.labels(status).set(counts.get(status, 0))
        self.age.set(max(0, float(active["age"])))
        self.bytes.set(active["bytes"])
        self.pending.set(pending)
