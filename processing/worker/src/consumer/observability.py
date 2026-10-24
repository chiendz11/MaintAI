"""Nonblocking health routes, bounded-cardinality metrics and dependency probes."""

import asyncio
import ipaddress
import threading
import time
from contextlib import contextmanager

import psycopg
import uvicorn
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class RuntimeObservability:
    def __init__(self, role, *, profile, stop=None):
        self.role, self.profile = role, profile
        self.stop = stop
        self.database = self.kafka = self.draining = False
        self.checked_at = float("-inf")
        self.registry = CollectorRegistry()
        self.records = Counter(
            "maintai_processing_durable_receipts_total",
            "Durable Kafka receipts, including replay",
            ["role"],
            registry=self.registry,
        )
        self.errors = Counter(
            "maintai_processing_failures_total",
            "Runtime failures",
            ["role"],
            registry=self.registry,
        )
        self.database_failures = Counter(
            "maintai_processing_database_failures_total",
            "Database failures by operation role and bounded reason",
            ["role", "reason"],
            registry=self.registry,
        )
        self.publish_attempts = Counter(
            "maintai_processing_outbox_publish_attempts_total",
            "Outbox publish attempts, including SDK retries",
            ["result"],
            registry=self.registry,
        )
        self.validation_attempts = Counter(
            "maintai_processing_validation_attempts_total",
            "Validation attempts, including attempts retried after rollback",
            ["status"],
            registry=self.registry,
        )
        self.invalid_reasons = Counter(
            "maintai_processing_invalid_reasons_total",
            "Validation attempt reasons with bounded labels",
            ["reason"],
            registry=self.registry,
        )
        self.ml_latency = Histogram(
            "maintai_processing_ml_seconds",
            "ML HTTP attempt sequence latency",
            registry=self.registry,
        )
        self.ml_results = Counter(
            "maintai_processing_ml_results_total",
            "ML request sequences",
            ["status"],
            registry=self.registry,
        )
        self.published = Counter(
            "maintai_processing_outbox_published_total",
            "Outbox rows acknowledged",
            registry=self.registry,
        )
        self.backlog = Gauge(
            "maintai_processing_outbox_pending", "Unpublished outbox rows", registry=self.registry
        )
        self.age = Gauge(
            "maintai_processing_outbox_oldest_seconds",
            "Oldest unpublished outcome age",
            registry=self.registry,
        )
        self.app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

        @self.app.get("/health/live")
        async def live():
            return {"status": "UP"}

        @self.app.get("/health/ready")
        async def ready():
            fresh = time.monotonic() - self.checked_at <= 10
            checks = {
                "database": self.database and fresh,
                "kafka": self.kafka and fresh,
                "not_draining": not self.draining and not (self.stop and self.stop.is_set()),
            }
            return JSONResponse(
                {
                    "status": "UP" if all(checks.values()) else "DEGRADED",
                    "profile": self.profile,
                    "provider_integration_validated": False,
                    "checks": checks,
                },
                status_code=200 if all(checks.values()) else 503,
            )

        @self.app.get("/metrics")
        async def metrics():
            return Response(generate_latest(self.registry), media_type="text/plain; version=0.0.4")

    def record_database_failure(self, error):
        if isinstance(error, psycopg.errors.QueryCanceled):
            reason = "TIMEOUT"
        elif isinstance(error, psycopg.IntegrityError):
            reason = "INTEGRITY"
        elif isinstance(error, psycopg.OperationalError):
            reason = "CONNECTION"
        else:
            reason = "OTHER"
        self.database_failures.labels(self.role, reason).inc()

    @contextmanager
    def database_connection(self, factory):
        try:
            with factory() as connection:
                yield connection
        except psycopg.Error as exc:
            self.record_database_failure(exc)
            raise

    def record_validation(self, status, reasons):
        status = status if status in {"VALID", "INVALID", "UNKNOWN"} else "UNKNOWN"
        self.validation_attempts.labels(status).inc()
        allowed = {
            "NON_FINITE",
            "MISSING_FIELD",
            "INVALID_TYPE",
            "UNSUPPORTED_VALUE",
            "OUT_OF_RANGE",
            "UNEXPECTED_FIELD",
            "SCHEMA_VIOLATION",
            "MACHINE_NOT_FOUND",
            "REGISTRY_UNAVAILABLE",
        }
        for reason in {reason if reason in allowed else "OTHER" for reason in reasons}:
            self.invalid_reasons.labels(reason).inc()

    def update_dependencies(self, *, database, kafka):
        self.database, self.kafka = database, kafka
        self.checked_at = time.monotonic()


def probe_host(env):
    host = env.get("PROCESSING_HEALTH_HOST", "127.0.0.1")
    ipaddress.ip_address(host)
    return host


def start_probe_server(observer, port, *, host="127.0.0.1"):
    server = uvicorn.Server(
        uvicorn.Config(
            observer.app,
            host=host,
            port=port,
            access_log=False,
            log_level="warning",
            log_config=None,
        )
    )
    failed = threading.Event()

    def run():
        try:
            server.run()
        except (Exception, SystemExit):
            failed.set()

    thread = threading.Thread(target=run, name="processing-probes", daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started:
        if failed.is_set() or not thread.is_alive() or time.monotonic() >= deadline:
            server.should_exit = True
            thread.join(timeout=1)
            raise RuntimeError("Processing probe server did not start")
        time.sleep(0.01)
    if not thread.is_alive():
        raise RuntimeError("Processing probe server stopped during startup")
    return server, thread


async def monitor_dependencies(observer, settings, database_url, stop, *, relay=False):
    admin = AdminClient(settings.client_config())
    topics = ("inference.results", "alert.signals") if relay else ("sensor.readings",)

    def check():
        database = kafka = False
        try:
            with psycopg.connect(
                database_url, connect_timeout=2, options="-c statement_timeout=2000"
            ) as conn:
                if relay:
                    count, age = conn.execute("""SELECT count(*),COALESCE(EXTRACT(EPOCH FROM
                        clock_timestamp()-min(created_at)),0)
                        FROM maintai_kafka_outbox WHERE status!='PUBLISHED'""").fetchone()
                    observer.backlog.set(count)
                    observer.age.set(max(0, age))
                else:
                    conn.execute("SELECT event_id FROM sensor_events LIMIT 0")
                database = True
        except psycopg.Error as exc:
            observer.record_database_failure(exc)
        except Exception:
            pass
        try:
            metadata = admin.list_topics(timeout=2)
            kafka = all(
                name in metadata.topics and metadata.topics[name].error is None for name in topics
            )
        except Exception:
            pass
        observer.update_dependencies(database=database, kafka=kafka)

    while not stop.is_set():
        await asyncio.to_thread(check)
        await asyncio.sleep(2)
