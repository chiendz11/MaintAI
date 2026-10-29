"""OBS-01 catalog: bounded labels, one registry shared by all archive groups."""

import logging

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

LOGGER = logging.getLogger("maintai.storage")
IO_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120)


class Metrics:
    def __init__(self, state):
        self.state = state
        self.registry = CollectorRegistry()
        self.batches = Counter(
            "maintai_storage_batches_total",
            "Archive attempts by durable result",
            ["status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "maintai_storage_write_duration_seconds",
            "Write/verify attempt duration",
            ["outcome"],
            buckets=IO_BUCKETS,
            registry=self.registry,
        )
        self.errors = Counter(
            "maintai_dependency_errors_total",
            "Failed dependency calls",
            ["dependency", "error_type"],
            registry=self.registry,
        )
        self.age = Gauge(
            "maintai_storage_oldest_uncommitted_age_seconds",
            "Age since local admission of the oldest owned batch awaiting offset ACK",
            registry=self.registry,
        )
        self.age.set_function(state.oldest_age)
        self.catalog_runs = Counter(
            "maintai_storage_catalog_sync_total",
            "Catalog sync cycles by result",
            ["status"],
            registry=self.registry,
        )
        self.catalog_entries = Counter(
            "maintai_storage_catalog_entries_total",
            "Verified receipts offered to catalog publication",
            registry=self.registry,
        )
        self.catalog_partitions = Gauge(
            "maintai_storage_catalog_partitions",
            "Partition counts by bounded topic and coverage state",
            ["topic", "status"],
            registry=self.registry,
        )

    def catalog_result(self, status, count):
        self.observe(lambda: self.catalog_runs.labels(status=status).inc())
        self.observe(lambda: self.catalog_entries.inc(count))

    def catalog_coverage(self, partitions):
        for topic in ("sensor.readings", "sensor.rejected"):
            for status in ("COMPLETE", "LAGGING", "GAP", "UNKNOWN"):
                count = sum(p["topic"] == topic and p["status"] == status for p in partitions)
                self.observe(
                    lambda t=topic, s=status, n=count: self.catalog_partitions.labels(
                        topic=t, status=s
                    ).set(n)
                )

    def observe(self, method, *args):
        # A collector failure must never alter the archive receipt or offset decision.
        try:
            method(*args)
        except Exception:
            LOGGER.error('{"event":"instrumentation_failed","error_type":"OTHER"}')

    def batch(self, status):
        self.observe(lambda: self.batches.labels(status=status).inc())

    def write(self, seconds, outcome):
        self.observe(lambda: self.duration.labels(outcome=outcome).observe(seconds))

    def dependency_error(self, dependency, error_type):
        self.observe(lambda: self.errors.labels(dependency=dependency, error_type=error_type).inc())

    def render(self):
        return generate_latest(self.registry)
