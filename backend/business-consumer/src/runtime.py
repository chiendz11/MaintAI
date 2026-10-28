"""Supervise serial SDK loops and a separate management HTTP server."""

import signal
import threading
import time

import uvicorn
from confluent_kafka.admin import AdminClient
from maintai_messaging.postgres import connect
from maintai_messaging.service import serve_consumers

from .main import make_handlers
from .observability import Metrics


def serve(settings, *, stop=None, connection_factory=None):
    stop = stop if stop is not None else threading.Event()
    factory = connection_factory or (lambda: connect(settings.database_url))
    metrics = Metrics(stop=stop)
    failed = threading.Event()
    previous = {}
    server = uvicorn.Server(
        uvicorn.Config(
            metrics.app,
            host=settings.host,
            port=settings.port,
            access_log=False,
            log_config=None,
            log_level="warning",
            timeout_graceful_shutdown=10,
        )
    )

    def probes():
        try:
            server.run()
        except (Exception, SystemExit):
            failed.set()
        finally:
            if not stop.is_set():
                failed.set()
                stop.set()

    def monitor():
        try:
            admin = AdminClient(settings.kafka.client_config())
            while not stop.is_set():
                database = kafka = False
                try:
                    with factory() as connection:
                        for table in (
                            "machines",
                            "latest_telemetry",
                            "latest_processing",
                            "latest_valid_prediction",
                            "business_source_inbox",
                            "maintai_kafka_inbox",
                        ):
                            # All table names are internal constants.
                            connection.execute(f"SELECT 1 FROM {table} LIMIT 0")
                    database = True
                except Exception:
                    try:
                        metrics.errors.labels("business_db", "UNAVAILABLE").inc()
                    except Exception:
                        pass
                try:
                    metadata = admin.list_topics(timeout=2)
                    kafka = all(
                        name in metadata.topics and metadata.topics[name].error is None
                        for name in ("sensor.readings", "inference.results")
                    )
                except Exception:
                    try:
                        metrics.errors.labels("kafka", "UNAVAILABLE").inc()
                    except Exception:
                        pass
                metrics.dependencies(database=database, kafka=kafka)
                stop.wait(2)
        except Exception:
            failed.set()
            stop.set()

    probe_thread = threading.Thread(target=probes, name="business-probes", daemon=True)
    monitor_thread = threading.Thread(target=monitor, name="business-dependencies", daemon=True)
    try:
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, lambda *_: stop.set())
        probe_thread.start()
        deadline = time.monotonic() + 5
        while not server.started:
            if failed.is_set() or stop.is_set() or time.monotonic() >= deadline:
                raise RuntimeError("Business Consumer probes did not start")
            time.sleep(0.01)
        monitor_thread.start()
        # No keyed limits: Business retains the SDK serial runner for each role.
        serve_consumers(
            settings.kafka, make_handlers(factory, settings.kafka, metrics=metrics), stop
        )
        if failed.is_set():
            raise RuntimeError("Business Consumer management loop failed")
    finally:
        stop.set()
        server.should_exit = True
        if monitor_thread.ident is not None:
            monitor_thread.join(timeout=6)
        if probe_thread.ident is not None:
            probe_thread.join(timeout=11)
        for signum, handler in previous.items():
            signal.signal(signum, handler)
