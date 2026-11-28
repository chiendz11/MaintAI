"""Standalone background worker for Business M7 (Brief + Drift catalog) and projection rebuild."""

import argparse
import signal
import sys
import threading
import time

import uvicorn
from backend.shared.persistence.pool import ConnectionPool

from .observability import Metrics


def serve_worker(settings, *, stop=None, connection_factory=None):
    stop = stop if stop is not None else threading.Event()
    pool = ConnectionPool(settings.database_url) if connection_factory is None else None
    factory = connection_factory or pool
    metrics = Metrics(stop=stop)

    from backend.shared.service_auth import ServiceOAuthTokenProvider

    token_provider = (
        ServiceOAuthTokenProvider(settings.processing_oauth)
        if getattr(settings, "processing_oauth", None)
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
        rag_token_provider = (
            ServiceOAuthTokenProvider(settings.m7.rag_oauth)
            if settings.m7.rag_oauth is not None
            else None
        )
        runners = [
            BriefRunner(
                BriefRepository(factory),
                RAGClient(
                    settings.m7.rag_url,
                    settings.m7.rag_token_file,
                    token_provider=rag_token_provider,
                    local=settings.m7.local,
                ),
                stop,
                metrics=job_metrics,
            ),
            CatalogRunner(catalog(settings.m7, factory), stop),
        ]

    rebuild_metrics = None
    if settings.projection_generations:
        from backend.shared.projections.generations import current
        from backend.shared.projections.metrics import RebuildMetrics

        with factory() as connection:
            current(connection)
        rebuild_metrics = RebuildMetrics(metrics.registry)

    if settings.rebuild is not None:
        from backend.shared.projections.runtime import build_runner

        runners.append(build_runner(settings, stop, token_provider=token_provider))

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
            while not stop.is_set():
                database = False
                try:
                    with factory() as connection:
                        for table in (
                            "machines",
                            "alerts",
                            "brief_jobs",
                            "drift_catalog",
                        ):
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
                # Standalone worker has no Kafka dependency; keep readiness aligned with DB check.
                metrics.dependencies(database=database, kafka=database)
                stop.wait(2)
        except Exception:
            failed.set()
            stop.set()

    probe_thread = threading.Thread(target=probes, name="business-worker-probes", daemon=True)
    monitor_thread = threading.Thread(
        target=monitor, name="business-worker-dependencies", daemon=True
    )
    try:
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, lambda *_: stop.set())
        probe_thread.start()
        deadline = time.monotonic() + 5
        while not server.started:
            if failed.is_set() or stop.is_set() or time.monotonic() >= deadline:
                raise RuntimeError("Business Worker probes did not start")
            time.sleep(0.01)
        monitor_thread.start()
        for runner in runners:
            runner.start()
        while not stop.wait(1):
            pass
        if failed.is_set():
            raise RuntimeError("Business Worker management loop failed")
    finally:
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


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Business Background Worker (M7 & Projection Rebuild)"
    )
    parser.parse_args(argv)
    try:
        from .config import WorkerSettings

        serve_worker(WorkerSettings.from_env())
        return 0
    except Exception as exc:
        print(f"Business Worker failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
