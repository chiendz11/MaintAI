"""Unit tests for backend shared ConnectionPool."""

from contextlib import contextmanager

import psycopg
import pytest
from backend.shared.persistence.pool import ConnectionPool
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


def test_backend_pool_respects_custom_timeouts_and_reuses(monkeypatch):
    seen_kwargs = []
    created = []

    def fake_connect(_dsn, **kwargs):
        seen_kwargs.append(kwargs)
        conn = _FakeConnection()
        created.append(conn)
        return conn

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    pool = ConnectionPool(
        "postgresql://test@127.0.0.1/test",
        size=4,
        connect_timeout=2,
        options="-c statement_timeout=3000",
    )

    with pool() as conn:
        conn.execute("SELECT 1 FROM machines LIMIT 0")
    with pytest.raises(RuntimeError, match="boom"), pool() as conn:
        conn.execute("SELECT 1 FROM machines")
        raise RuntimeError("boom")
    with pool() as conn, conn.transaction():
        conn.execute("SELECT 1 FROM alerts")

    assert len(created) == 1
    assert created[0].commits == 2
    assert created[0].rollbacks == 1
    assert seen_kwargs[0]["connect_timeout"] == 2
    assert seen_kwargs[0]["options"] == "-c statement_timeout=3000"

    pool.close()
    assert created[0].closed
    with pytest.raises(RuntimeError, match="closed"), pool():
        pass
