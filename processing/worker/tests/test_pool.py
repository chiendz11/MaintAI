"""Unit tests for processing shared ConnectionPool and worker registry cache validation."""

import asyncio
import threading
from contextlib import contextmanager

import psycopg
import pytest
from consumer.main import run_worker
from processing.shared.persistence.pool import ConnectionPool
from psycopg.pq import TransactionStatus


class _FakeConnection:
    def __init__(self, *, fail_commit=False, fail_idle_check=False):
        self.closed = False
        self.broken = False
        self.fail_commit = fail_commit
        self.fail_idle_check = fail_idle_check
        self.commits = 0
        self.rollbacks = 0
        self.queries = []
        self.info = type("Info", (), {"transaction_status": TransactionStatus.IDLE})()

    def execute(self, query):
        self.queries.append(query)
        if self.fail_idle_check and query == "SELECT 1":
            self.broken = True
            raise RuntimeError("stale connection")
        self.info.transaction_status = TransactionStatus.INTRANS
        return self

    def commit(self):
        if self.fail_commit:
            self.broken = True
            raise RuntimeError("commit failed")
        self.commits += 1
        self.info.transaction_status = TransactionStatus.IDLE

    def rollback(self):
        self.rollbacks += 1
        self.info.transaction_status = TransactionStatus.IDLE

    def transaction(self):
        @contextmanager
        def _tx():
            try:
                yield self
            except BaseException:
                self.rollback()
                raise
            else:
                self.commit()

        return _tx()

    def close(self):
        self.closed = True


def test_processing_pool_reuses_and_discards_broken_or_stale_connections(monkeypatch):
    behaviors = [{"fail_commit": True}, {"fail_idle_check": True}, {}]
    created = []

    def fake_connect(*_args, **_kwargs):
        kwargs = behaviors.pop(0) if behaviors else {}
        conn = _FakeConnection(**kwargs)
        created.append(conn)
        return conn

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    pool = ConnectionPool("postgresql://test@127.0.0.1/test", size=2, max_idle_check_seconds=0.0)

    with pytest.raises(RuntimeError, match="commit failed"), pool() as conn:
        conn.execute("INSERT INTO sensor_events VALUES (1)")
    assert len(created) == 1
    assert created[0].closed

    with pool() as conn, conn.transaction():
        conn.execute("SELECT 1 FROM sensor_events")
    assert len(created) == 2
    assert created[1].commits == 1

    with pool() as conn:
        assert conn is created[2]
        assert created[1].closed
        assert created[1].queries[-1] == "SELECT 1"

    pool.close()
    assert created[2].closed
    with pytest.raises(RuntimeError, match="closed"), pool():
        pass


@pytest.mark.parametrize("raw", ["-1", "nan", "inf", "abc"])
def test_run_worker_rejects_invalid_registry_cache_seconds(monkeypatch, raw):
    monkeypatch.setenv("PROCESSING_REGISTRY_CACHE_SECONDS", raw)
    with pytest.raises(ValueError):
        asyncio.run(run_worker(object(), threading.Event()))
