"""Finite-label metrics and structured, payload-free request logs."""

import json
import logging
import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class Observability:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "maintai_ingestion_http_requests_total",
            "HTTP requests",
            ["method", "route", "status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "maintai_ingestion_http_request_duration_seconds",
            "HTTP latency",
            ["method", "route"],
            registry=self.registry,
        )
        self.publishes = Counter(
            "maintai_ingestion_publish_total",
            "Kafka publish attempts",
            ["topic", "outcome"],
            registry=self.registry,
        )
        self.publish_duration = Histogram(
            "maintai_ingestion_publish_duration_seconds",
            "Kafka ACK latency",
            ["topic", "outcome"],
            registry=self.registry,
        )
        self.inflight = Gauge(
            "maintai_ingestion_inflight_requests", "Admitted requests", registry=self.registry
        )

    def record_request(self, request, response, trace, started):
        route = request.scope.get("route")
        path = getattr(route, "path", "__unmatched__")
        method = (
            request.method
            if request.method in {"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"}
            else "OTHER"
        )
        elapsed = time.monotonic() - started
        self.requests.labels(method, path, str(response.status_code)).inc()
        self.duration.labels(method, path).observe(elapsed)
        logging.getLogger("maintai.ingestion").info(
            json.dumps(
                {
                    "service": "ingestion-api",
                    "method": method,
                    "route": path,
                    "status": response.status_code,
                    "request_id": trace.request_id,
                    "correlation_id": trace.correlation_id,
                    "duration_seconds": round(elapsed, 6),
                }
            )
        )

    def render(self):
        return generate_latest(self.registry)


class ObservedPublisher:
    def __init__(self, publisher, observability):
        self.publisher, self.obs, self.settings = publisher, observability, publisher.settings

    def publish(self, topic, payload):
        started = time.monotonic()
        outcome = "failed"
        try:
            delivery = self.publisher.publish(topic, payload)
            outcome = "acknowledged"
            return delivery
        finally:
            self.obs.publishes.labels(topic, outcome).inc()
            self.obs.publish_duration.labels(topic, outcome).observe(time.monotonic() - started)

    def close(self):
        self.publisher.close()


def configure_logging():
    logger = logging.getLogger("maintai.ingestion")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
