"""Commit-based OBS-01 samples and cached Consumer dependency probes."""

import math
import threading
import time
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest


class Metrics:
    def __init__(self, *, wall_time=time.time, stop=None):
        self.wall_time = wall_time
        self.stop = stop if stop is not None else threading.Event()
        self._dependencies = (False, False, float("-inf"))
        self.registry = CollectorRegistry()
        self.duplicates = Counter(
            "maintai_business_inbox_duplicates_total",
            "Committed duplicate deliveries",
            ["topic"],
            registry=self.registry,
        )
        self.visibility = Histogram(
            "maintai_event_to_telemetry_visible_duration_seconds",
            "Accepted source to latest telemetry commit; not browser rendering latency",
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
            registry=self.registry,
        )
        self.dropped = Counter(
            "maintai_metric_samples_dropped_total",
            "Invalid observation samples",
            ["reason"],
            registry=self.registry,
        )
        self.errors = Counter(
            "maintai_dependency_errors_total",
            "Dependency attempt failures",
            ["dependency", "error_type"],
            registry=self.registry,
        )
        self.app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

        @self.app.get("/health/live")
        def live():
            return {"status": "UP", "service": "business-consumer", "version": "0.1.0"}

        @self.app.get("/health/ready")
        def ready():
            database, kafka, checked_at = self._dependencies
            fresh = time.monotonic() - checked_at <= 10
            checks = {
                "database": database and fresh,
                "kafka": kafka and fresh,
                "not_draining": not self.stop.is_set(),
            }
            healthy = all(checks.values())
            return JSONResponse(
                {"status": "UP" if healthy else "DEGRADED", "checks": checks},
                status_code=200 if healthy else 503,
            )

        @self.app.get("/metrics")
        def metrics():
            return Response(generate_latest(self.registry), media_type="text/plain; version=0.0.4")

    def dependencies(self, *, database, kafka):
        self._dependencies = (bool(database), bool(kafka), time.monotonic())

    def committed(self, event, *, duplicate, telemetry_visible):
        if duplicate:
            self.duplicates.labels(event.topic).inc()
        elif telemetry_visible:
            try:
                accepted = datetime.fromisoformat(event.payload["server_accepted_at"])
                if accepted.tzinfo is None:
                    raise ValueError("Source time lacks timezone")
                elapsed = self.wall_time() - accepted.timestamp()
                if not math.isfinite(elapsed):
                    self.dropped.labels("INVALID_VALUE").inc()
                elif elapsed < 0:
                    self.dropped.labels("CLOCK_SKEW").inc()
                else:
                    self.visibility.observe(elapsed)
            except (KeyError, TypeError, ValueError, OverflowError):
                self.dropped.labels("SOURCE_TIME_MISSING").inc()
