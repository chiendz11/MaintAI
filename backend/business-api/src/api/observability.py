"""Bounded OBS-01 HTTP/dependency series for the Business API."""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        from backend.shared.alerts.metrics import AlertMetrics

        self.alerts = AlertMetrics(self.registry)
        self.requests = Counter(
            "maintai_http_requests_total",
            "Completed REST requests",
            ["route", "method", "status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "maintai_http_request_duration_seconds",
            "REST handler duration",
            ["route", "method"],
            registry=self.registry,
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
        )
        self.errors = Counter(
            "maintai_dependency_errors_total",
            "Failed dependency attempts",
            ["dependency", "error_type"],
            registry=self.registry,
        )
        self.connections = Gauge(
            "maintai_sse_connections",
            "Authenticated active SSE connections",
            registry=self.registry,
        )
        self.resync = Counter(
            "maintai_sse_resync_total",
            "Server-known resync triggers",
            ["reason"],
            registry=self.registry,
        )

    def sse_connection(self, delta):
        self.connections.inc(delta)

    def sse_resync(self, reason):
        self.resync.labels(
            reason if reason in {"RECONNECT", "GAP", "NOTIFY_LOST"} else "OTHER"
        ).inc()

    def http(self, route, method, status, elapsed):
        method = (
            method
            if method in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
            else "OTHER"
        )
        self.requests.labels(route, method, str(status)).inc()
        self.duration.labels(route, method).observe(elapsed)

    def dependency(self, dependency):
        self.errors.labels(dependency, "UNAVAILABLE").inc()
