"""Bounded Business job/catalog metrics, without actor or event identifiers."""

from prometheus_client import Counter, Gauge


class JobMetrics:
    def __init__(self, registry):
        self.attempts = Counter(
            "maintai_brief_job_attempts_total",
            "Finished provider attempts",
            ["status"],
            registry=registry,
        )
        self.oldest = Gauge(
            "maintai_brief_queue_oldest_age_seconds", "Oldest active job age", registry=registry
        )
        self.catalog_age = Gauge(
            "maintai_drift_catalog_scan_age_seconds",
            "Age of last successful catalog scan; -1 before observation",
            registry=registry,
        )

    def observe(self, factory):
        with factory() as connection:
            row = connection.execute(
                "SELECT COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-"
                "min(created_at)),0) AS age FROM brief_jobs WHERE status IN ('QUEUED','RUNNING')"
            ).fetchone()
            state = connection.execute(
                "SELECT EXTRACT(EPOCH FROM clock_timestamp()-last_scan_at) "
                "AS age FROM drift_catalog_state"
            ).fetchone()
        self.oldest.set(max(0, float(row["age"])))
        self.catalog_age.set(-1 if state["age"] is None else max(0, float(state["age"])))
