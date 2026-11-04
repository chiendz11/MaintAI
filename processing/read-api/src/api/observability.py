"""Bounded OBS-01 HTTP/dependency series for the internal Read API."""

from prometheus_client import CollectorRegistry, Counter, Histogram


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
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
        self.history_queries = Counter(
            "maintai_history_queries_total",
            "Completed history query responses",
            ["source", "outcome"],
            registry=self.registry,
        )
        self.history_duration = Histogram(
            "maintai_history_query_duration_seconds",
            "History read duration",
            ["source"],
            registry=self.registry,
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
        )

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

    def history(self, source, status, elapsed):
        if source not in {"HOT", "ARCHIVE", "MIXED"}:
            return
        outcome = "SUCCESS" if status == 200 else "PARTIAL" if status == 206 else "ERROR"
        self.history_queries.labels(source, outcome).inc()
        self.history_duration.labels(source).observe(elapsed)
