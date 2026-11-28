"""Unit tests for the business-consumer / business-worker split."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from business_consumer.config import Settings, WorkerSettings
from business_consumer.worker import serve_worker


def test_consumer_run_workers_flag():
    base_env = {
        "BUSINESS_DATABASE_URL": "postgresql://u:p@127.0.0.1:5432/business_db",
        "BUSINESS_CONSUMER_PROFILE": "local-test",
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:9092",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_LOCAL_MODE": "true",
    }
    assert Settings.from_env(base_env).run_workers is True
    assert (
        Settings.from_env({**base_env, "BUSINESS_CONSUMER_RUN_WORKERS": "false"}).run_workers
        is False
    )
    with pytest.raises(ValueError, match="BUSINESS_CONSUMER_RUN_WORKERS"):
        Settings.from_env({**base_env, "BUSINESS_CONSUMER_RUN_WORKERS": "maybe"})


def test_worker_settings_requires_no_kafka_and_validates_profile(tmp_path):
    token = tmp_path / "rag-token"
    token.write_text("tok", encoding="ascii")
    key = tmp_path / "cursor-key"
    key.write_bytes(b"x" * 32)
    env = {
        "BUSINESS_DATABASE_URL": "postgresql://u:p@postgres:5432/business_db",
        "BUSINESS_WORKER_PROFILE": "shared",
        "BUSINESS_WORKER_PORT": "8084",
        "BUSINESS_M7_ENABLED": "true",
        "BUSINESS_RAG_URL": "https://rag.internal:8002",
        "BUSINESS_RAG_TOKEN_FILE": str(token),
        "BUSINESS_DRIFT_CURSOR_KEY_FILE": str(key),
        "BUSINESS_DRIFT_BUCKET": "maintai-drift",
        "BUSINESS_DRIFT_S3_REGION": "ap-southeast-1",
    }
    parsed = WorkerSettings.from_env(env)
    assert parsed.port == 8084
    assert parsed.m7 is not None
    assert parsed.m7.bucket == "maintai-drift"

    with pytest.raises(ValueError, match="PostgreSQL URL"):
        WorkerSettings.from_env({})
    with pytest.raises(ValueError, match="profile"):
        WorkerSettings.from_env({**env, "BUSINESS_WORKER_PROFILE": "invalid"})
    with pytest.raises(ValueError, match="loopback"):
        WorkerSettings.from_env(
            {
                **env,
                "BUSINESS_DATABASE_URL": "postgresql://u:p@db.prod.example.com:5432/business_db",
                "BUSINESS_WORKER_PROFILE": "local-test",
            }
        )


def test_serve_worker_starts_and_closes_runners_without_kafka(tmp_path):
    token = tmp_path / "rag-token"
    token.write_text("tok", encoding="ascii")
    key = tmp_path / "cursor-key"
    key.write_bytes(b"x" * 32)
    settings = WorkerSettings.from_env(
        {
            "BUSINESS_DATABASE_URL": "postgresql://u:p@127.0.0.1:5432/business_db",
            "BUSINESS_WORKER_PROFILE": "local-test",
            "BUSINESS_WORKER_HOST": "127.0.0.1",
            "BUSINESS_WORKER_PORT": "8094",
            "BUSINESS_M7_ENABLED": "true",
            "BUSINESS_RAG_URL": "http://127.0.0.1:8002",
            "BUSINESS_RAG_TOKEN_FILE": str(token),
            "BUSINESS_DRIFT_CURSOR_KEY_FILE": str(key),
            "BUSINESS_DRIFT_BUCKET": "maintai-drift",
            "BUSINESS_DRIFT_S3_REGION": "us-east-1",
            "BUSINESS_DRIFT_S3_ENDPOINT": "http://127.0.0.1:9000",
        }
    )

    stop = threading.Event()
    checked = threading.Event()
    brief_runner = MagicMock()
    catalog_runner = MagicMock()

    class _FakeServer:
        started = True
        should_exit = False

        def run(self):
            checked.wait(timeout=2)
            stop.set()

    fake_factory = MagicMock()
    fake_conn = MagicMock()

    def _exec(sql, *args, **kwargs):
        if "drift_catalog" in str(sql):
            checked.set()
        return MagicMock()

    fake_conn.execute.side_effect = _exec
    fake_factory.return_value.__enter__.return_value = fake_conn

    with (
        patch("uvicorn.Server", return_value=_FakeServer()),
        patch("backend.shared.copilot.runner.BriefRunner", return_value=brief_runner),
        patch("backend.shared.monitoring.runtime.CatalogRunner", return_value=catalog_runner),
        patch("backend.shared.monitoring.runtime.catalog", return_value=SimpleNamespace()),
    ):
        serve_worker(settings, stop=stop, connection_factory=fake_factory)

    assert checked.is_set()
    executed_sql = " ".join(str(call.args[0]) for call in fake_conn.execute.call_args_list)
    assert "brief_jobs" in executed_sql
    assert "drift_catalog" in executed_sql
    brief_runner.start.assert_called_once()
    brief_runner.close.assert_called_once()
    catalog_runner.start.assert_called_once()
    catalog_runner.close.assert_called_once()
