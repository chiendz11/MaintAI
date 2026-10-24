"""OBS-01 metric names, labels and buckets; never event/machine IDs as labels."""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10)


class SensorMetrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.generated = Counter(
            "maintai_simulator_generated_total",
            "Readings committed to spool",
            registry=self.registry,
        )
        self.attempts = Counter(
            "maintai_simulator_send_attempts_total",
            "HTTP delivery attempts",
            ["outcome"],
            registry=self.registry,
        )
        self.machines = Gauge(
            "maintai_simulator_machines",
            "Persisted machine control states",
            ["state"],
            registry=self.registry,
        )
        self.pending = Gauge(
            "maintai_simulator_spool_pending_count",
            "Durable pending readings",
            registry=self.registry,
        )
        self.http = Counter(
            "maintai_http_requests_total",
            "Completed control requests",
            ["route", "method", "status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "maintai_http_request_duration_seconds",
            "Control request duration",
            ["route", "method"],
            buckets=HTTP_BUCKETS,
            registry=self.registry,
        )

    def refresh(self, store):
        machines = store.list()
        summary = store.summary()
        for state in ("RUNNING", "PAUSED"):
            self.machines.labels(state).set(sum(m["stream_state"] == state for m in machines))
        self.pending.set(summary["counts"].get("pending", 0))
