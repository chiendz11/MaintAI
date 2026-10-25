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
