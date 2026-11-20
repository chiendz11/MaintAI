"""Supervise serial SDK loops and a separate management HTTP server."""

import signal
import threading
import time
from contextlib import contextmanager

import psycopg
import uvicorn
from confluent_kafka.admin import AdminClient
from maintai_messaging.service import serve_consumers
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

from .main import make_handlers
from .observability import Metrics


def _close_quietly(connection) -> None:
    try:
        connection.close()
    except Exception:
        pass


class ConnectionPool:
    """Bounded persistent PostgreSQL connection factory for business-consumer."""

    def __init__(
        self,
        dsn: str,
        *,
        size: int = 8,
        timeout: float = 30.0,
        max_idle_check_seconds: float = 10.0,
    ):
        if not 1 <= size <= 32 or timeout <= 0 or max_idle_check_seconds < 0:
            raise ValueError("Connection pool budgets must be positive and bounded")
        self._dsn = dsn
        self._size = size
        self._timeout = timeout
        self._max_idle_check_seconds = max_idle_check_seconds
        self._condition = threading.Condition()
        self._idle = []
        self._allocated = 0
        self._closed = False

    def _connect(self):
        return psycopg.connect(
            self._dsn,
            row_factory=dict_row,
            connect_timeout=10,
            options=(
                "-c statement_timeout=15000 -c lock_timeout=5000 "
                "-c idle_in_transaction_session_timeout=60000"
            ),
        )

    def _is_usable(self, connection) -> bool:
        return (
            not connection.closed
            and not connection.broken
            and connection.info.transaction_status == TransactionStatus.IDLE
        )

    def _borrow(self):
        deadline = time.monotonic() + self._timeout
        while True:
            candidate = None
            idle_since = 0.0
            with self._condition:
                while True:
                    if self._closed:
                        raise RuntimeError("Connection pool is closed")
                    if self._idle:
                        candidate, idle_since = self._idle.pop()
                        if self._is_usable(candidate):
                            break
                        _close_quietly(candidate)
                        self._allocated -= 1
                        self._condition.notify()
                        candidate = None
                        continue
                    if self._allocated < self._size:
                        self._allocated += 1
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Timed out waiting for a PostgreSQL connection")
                    self._condition.wait(remaining)

            if candidate is None:
                try:
                    connection = self._connect()
                except BaseException:
                    with self._condition:
                        self._allocated -= 1
                        self._condition.notify()
                    raise
                with self._condition:
                    if self._closed:
                        _close_quietly(connection)
                        self._allocated -= 1
                        self._condition.notify_all()
                        raise RuntimeError("Connection pool is closed")
                return connection

            if (time.monotonic() - idle_since) >= self._max_idle_check_seconds:
                try:
                    candidate.execute("SELECT 1")
                    candidate.rollback()
                except Exception:
                    _close_quietly(candidate)
                    with self._condition:
                        self._allocated -= 1
                        self._condition.notify()
                    continue

            with self._condition:
                if self._closed:
                    _close_quietly(candidate)
                    self._allocated -= 1
                    self._condition.notify_all()
                    raise RuntimeError("Connection pool is closed")
            return candidate

    def _finish(self, connection, *, commit: bool) -> None:
        if connection.closed or connection.broken:
            return
        status = connection.info.transaction_status
        if commit:
            if status == TransactionStatus.INTRANS:
                connection.commit()
            elif status != TransactionStatus.IDLE:
                try:
                    connection.rollback()
                finally:
                    _close_quietly(connection)
            return
        try:
            if status != TransactionStatus.IDLE:
                connection.rollback()
        except Exception:
            _close_quietly(connection)

    def _release(self, connection, *, healthy: bool) -> None:
        with self._condition:
            if self._closed or not healthy or not self._is_usable(connection):
                _close_quietly(connection)
                self._allocated -= 1
            else:
                self._idle.append((connection, time.monotonic()))
            self._condition.notify()

    @contextmanager
    def __call__(self):
        connection = self._borrow()
        healthy = False
        try:
            yield connection
        except Exception:
            self._finish(connection, commit=False)
            healthy = not (connection.closed or connection.broken)
            raise
        except BaseException:
            self._finish(connection, commit=False)
            raise
        else:
            self._finish(connection, commit=True)
            healthy = not (connection.closed or connection.broken)
        finally:
            self._release(connection, healthy=healthy)

    def close(self) -> None:
        with self._condition:
            self._closed = True
            for connection, _ in self._idle:
                _close_quietly(connection)
                self._allocated -= 1
            self._idle.clear()
            self._condition.notify_all()


def serve(settings, *, stop=None, connection_factory=None):
    stop = stop if stop is not None else threading.Event()
    pool = ConnectionPool(settings.database_url) if connection_factory is None else None
    factory = connection_factory or pool
    metrics = Metrics(stop=stop)
    from backend.shared.alerts.evidence import EvidenceClient
    from backend.shared.service_auth import ServiceOAuthTokenProvider

    token_provider = (
        ServiceOAuthTokenProvider(settings.processing_oauth)
        if getattr(settings, "processing_oauth", None)
        else None
    )
    evidence = (
        EvidenceClient(
            settings.processing_read_url,
            settings.processing_token_file,
            token_provider=token_provider,
        )
        if settings.processing_read_url
        else None
    )
    runners = []
    job_metrics = None
    if settings.m7:
        from backend.shared.copilot.metrics import JobMetrics
        from backend.shared.copilot.repository import BriefRepository
        from backend.shared.copilot.runner import BriefRunner
        from backend.shared.monitoring.runtime import CatalogRunner, catalog
        from rag_client import RAGClient

        job_metrics = JobMetrics(metrics.registry)
        runners = [
            BriefRunner(
                BriefRepository(factory),
                RAGClient(settings.m7.rag_url, settings.m7.rag_token_file, local=settings.m7.local),
                stop,
                metrics=job_metrics,
            ),
            CatalogRunner(catalog(settings.m7, factory), stop),
        ]
    if settings.projection_generations:
        from backend.shared.projections.generations import current

        with factory() as connection:
            current(connection)
    if settings.rebuild is not None:
        from backend.shared.projections.runtime import build_runner

        runners.append(build_runner(settings, stop, token_provider=token_provider))
    rebuild_metrics = None
    if settings.projection_generations:
        from backend.shared.projections.metrics import RebuildMetrics

        rebuild_metrics = RebuildMetrics(metrics.registry)
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
                            "alerts",
                            "alert_notifications",
                            "alert_evidence_snapshots",
                        ):
                            # All table names are internal constants.
                            connection.execute(f"SELECT 1 FROM {table} LIMIT 0")
                    if job_metrics is not None:
                        job_metrics.observe(factory)
                    if rebuild_metrics is not None:
                        rebuild_metrics.observe(factory)
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
                        for name in ("sensor.readings", "inference.results", "alert.signals")
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
        for runner in runners:
            runner.start()
        # No keyed limits: Business retains the SDK serial runner for each role.
        serve_consumers(
            settings.kafka,
            make_handlers(
                factory,
                settings.kafka,
                metrics=metrics,
                notification_channel=settings.notification_channel,
                alert_channel=settings.alert_channel,
                generations=settings.projection_generations,
                evidence=evidence,
            ),
            stop,
        )
        if failed.is_set():
            raise RuntimeError("Business Consumer management loop failed")
    finally:
        if evidence is not None:
            evidence.close()
        stop.set()
        for runner in runners:
            runner.close()
        server.should_exit = True
        if monitor_thread.ident is not None:
            monitor_thread.join(timeout=6)
        if probe_thread.ident is not None:
            probe_thread.join(timeout=11)
        if pool is not None:
            pool.close()
        for signum, handler in previous.items():
            signal.signal(signum, handler)
