"""OBS-01 metrics: bounded labels, request-local admission and safe observations."""

import json
import logging
import math
import time
from contextvars import ContextVar
from dataclasses import dataclass

import httpx
from maintai_messaging.errors import PublishUnavailable
from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest

HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10)
EXCLUDED_ROUTES = frozenset({"/health/live", "/health/ready", "/metrics"})


@dataclass
class RequestObservation:
    admitted_at: float | None = None
    error_code: str | None = None
    rejection_recorded: bool = False


# asyncio.to_thread copies this context. Each request owns a distinct observation.
request_observation = ContextVar("ingestion_observation", default=None)


def error_type(error):
    if isinstance(error, (TimeoutError, httpx.TimeoutException)):
        return "TIMEOUT"
    if isinstance(error, httpx.HTTPStatusError):
        if error.response.status_code in {401, 403}:
            return "AUTH"
        return "UNAVAILABLE" if error.response.status_code >= 500 else "INVALID_RESPONSE"
    # PublishUncertain also covers negative ACKs: the type does not prove timeout.
    if isinstance(error, (PublishUnavailable, httpx.TransportError, ConnectionError, OSError)):
        return "UNAVAILABLE"
    if isinstance(error, (ValueError, TypeError)):
        return "INVALID_RESPONSE"
    return "OTHER"


class Observability:
    def __init__(self, clock=time.perf_counter):
        self.clock = clock
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "maintai_http_requests_total",
            "REST requests",
            ["route", "method", "status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "maintai_http_request_duration_seconds",
            "REST latency",
            ["route", "method"],
            buckets=HTTP_BUCKETS,
            registry=self.registry,
        )
        self.accepted = Counter(
            "maintai_ingestion_accepted_total",
            "Broker ACKs for sensor.readings",
            registry=self.registry,
        )
        self.rejected = Counter(
            "maintai_ingestion_rejected_total",
            "Terminal rejected requests",
            ["reason"],
            registry=self.registry,
        )
        self.publish_failures = Counter(
            "maintai_ingestion_publish_failures_total",
            "Failed publish attempts",
            ["error_type"],
            registry=self.registry,
        )
        self.ack_duration = Histogram(
            "maintai_ingestion_ack_duration_seconds",
            "Admission to broker result",
            ["outcome"],
            buckets=HTTP_BUCKETS,
            registry=self.registry,
        )
        self.dependency_errors = Counter(
            "maintai_dependency_errors_total",
            "Failed dependency calls",
            ["dependency", "error_type"],
            registry=self.registry,
        )
        self.dropped = Counter(
            "maintai_metric_samples_dropped_total",
            "Invalid observations discarded",
            ["reason"],
            registry=self.registry,
        )

    @staticmethod
    def _emit(action):
        try:
            action()
        except Exception:
            # Never include exception text: transport/collector errors may contain secrets.
            try:
                logging.getLogger("maintai.ingestion").error(
                    '{"service":"ingestion-api","event":"instrumentation_failure"}'
                )
            except Exception:
                # A broken logging sink must not replace a business/transport result either.
                pass

    def observe_duration(self, histogram, started, *labels):
        def observe():
            if started is None:
                self.dropped.labels("SOURCE_TIME_MISSING").inc()
                return
            elapsed = self.clock() - started
            if not math.isfinite(elapsed):
                self.dropped.labels("INVALID_VALUE").inc()
            elif elapsed < 0:
                self.dropped.labels("CLOCK_SKEW").inc()
            else:
                histogram.labels(*labels).observe(elapsed)

        self._emit(observe)

    def record_dependency_error(self, dependency, error):
        self._emit(lambda: self.dependency_errors.labels(dependency, error_type(error)).inc())

    def record_rejection(self, reason, context):
        if context is not None:
            if context.rejection_recorded:
                return
            context.rejection_recorded = True
        if reason not in {"MALFORMED_JSON", "VALIDATION_FAILED", "AUTH", "SIZE"}:
            reason = "OTHER"
        self._emit(lambda: self.rejected.labels(reason).inc())

    def record_publish(self, topic, error=None, *, reason=None):
        context = request_observation.get()
        outcome = (
            "ERROR"
            if error is not None
            else ("ACCEPTED" if topic == "sensor.readings" else "REJECTED")
        )
        if error is not None:
            self._emit(lambda: self.publish_failures.labels(error_type(error)).inc())
            self.record_dependency_error("kafka", error)
        elif topic == "sensor.readings":
            self._emit(self.accepted.inc)
        elif topic == "sensor.rejected":
            # The quarantine rejection is durable even if its HTTP task was cancelled.
            self.record_rejection(reason, context)
        self.observe_duration(self.ack_duration, context.admitted_at if context else None, outcome)

    def record_request(self, request, response, trace, started, context):
        route = request.scope.get("route")
        path = getattr(route, "path", "__unmatched__")
        if request.url.path in EXCLUDED_ROUTES:
            return
        method = (
            request.method
            if request.method in {"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"}
            else "OTHER"
        )
        status = str(response.status_code) if response is not None else "UNHANDLED"
        self._emit(lambda: self.requests.labels(path, method, status).inc())
        self.observe_duration(self.duration, started, path, method)
        if (
            request.method == "POST"
            and request.url.path == "/api/v1/sensor-readings"
            and response is not None
            and 400 <= response.status_code < 499
        ):
            reason = "OTHER"
            if response.status_code in {401, 403}:
                reason = "AUTH"
            elif response.status_code == 413:
                reason = "SIZE"
            elif context.error_code in {"MALFORMED_JSON", "VALIDATION_FAILED"}:
                reason = context.error_code
            self.record_rejection(reason, context)
        self._emit(
            lambda: logging.getLogger("maintai.ingestion").info(
                json.dumps(
                    {
                        "service": "ingestion-api",
                        "method": method,
                        "route": path,
                        "status": status,
                        "request_id": trace.request_id,
                        "correlation_id": trace.correlation_id,
                        "duration_seconds": round(self.clock() - started, 6),
                    }
                )
            )
        )

    def render(self):
        return generate_latest(self.registry)


class ObservedPublisher:
    def __init__(self, publisher, observability):
        self.publisher, self.obs, self.settings = publisher, observability, publisher.settings

    def publish(self, topic, payload):
        try:
            delivery = self.publisher.publish(topic, payload)
        except Exception as error:
            self.obs.record_publish(topic, error)
            raise
        self.obs.record_publish(topic, reason=payload.get("reason"))
        return delivery

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
