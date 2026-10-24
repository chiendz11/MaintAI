"""Readiness reflects fresh dependency evidence and drain, not fixture success."""

import asyncio
import threading
import time

import httpx
import pytest
import uvicorn
from consumer.observability import RuntimeObservability, start_probe_server


def test_stop_immediately_makes_readiness_degraded_without_cleanup():
    async def run():
        stop = threading.Event()
        observer = RuntimeObservability("worker", profile="local-fixture", stop=stop)
        observer.update_dependencies(database=True, kafka=True)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=observer.app), base_url="http://test"
        ) as client:
            assert (await client.get("/health/ready")).status_code == 200
            stop.set()
            assert (await client.get("/health/ready")).status_code == 503
            assert (await client.get("/health/live")).status_code == 200

    asyncio.run(run())


def test_database_failure_is_counted_and_propagated_without_secret_labels():
    from contextlib import contextmanager

    import psycopg
    from prometheus_client import generate_latest

    observer = RuntimeObservability("worker", profile="local-fixture")

    @contextmanager
    def unavailable():
        raise psycopg.OperationalError("secret-hostname-and-password")
        yield

    with pytest.raises(psycopg.OperationalError):
        with observer.database_connection(unavailable):
            pass
    metrics = generate_latest(observer.registry).decode()
    assert (
        'maintai_processing_database_failures_total{reason="CONNECTION",role="worker"} 1.0'
        in metrics
    )
    assert "secret-hostname-and-password" not in metrics


def test_probe_host_can_be_explicit_and_invalid_bind_is_rejected():
    from consumer.observability import probe_host

    assert probe_host({}) == "127.0.0.1"
    assert probe_host({"PROCESSING_HEALTH_HOST": "0.0.0.0"}) == "0.0.0.0"
    with pytest.raises(ValueError):
        probe_host({"PROCESSING_HEALTH_HOST": "not-an-ip"})


def test_readiness_requires_fresh_dependencies_and_not_draining():
    async def run():
        observer = RuntimeObservability("worker", profile="local-fixture")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=observer.app), base_url="http://test"
        ) as client:
            assert (await client.get("/health/live")).status_code == 200
            assert (await client.get("/health/ready")).status_code == 503
            observer.update_dependencies(database=True, kafka=True)
            ready = await client.get("/health/ready")
            assert ready.status_code == 200
            assert ready.json()["profile"] == "local-fixture"
            assert ready.json()["provider_integration_validated"] is False
            observer.checked_at = time.monotonic() - 20
            assert (await client.get("/health/ready")).status_code == 503
            observer.update_dependencies(database=True, kafka=True)
            observer.draining = True
            assert (await client.get("/health/ready")).status_code == 503
            assert (await client.get("/metrics")).status_code == 200

    asyncio.run(run())


def test_probe_bind_failure_prevents_runtime_startup(monkeypatch):
    def failed_server(self):
        raise SystemExit(3)

    monkeypatch.setattr(uvicorn.Server, "run", failed_server)
    observer = RuntimeObservability("worker", profile="local-fixture")
    with pytest.raises(RuntimeError):
        start_probe_server(observer, 12345)


def test_validation_metrics_count_attempts_without_unbounded_input_labels():
    from prometheus_client import generate_latest

    observer = RuntimeObservability("worker", profile="local-fixture")
    observer.record_validation("INVALID", ["INVALID_TYPE", "secret-untrusted-reason"])
    text = generate_latest(observer.registry).decode()
    assert 'maintai_processing_validation_attempts_total{status="INVALID"} 1.0' in text
    assert 'maintai_processing_invalid_reasons_total{reason="INVALID_TYPE"} 1.0' in text
    assert 'reason="OTHER"' in text
    assert "secret-untrusted-reason" not in text
