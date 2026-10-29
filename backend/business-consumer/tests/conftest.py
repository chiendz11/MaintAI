"""Isolated M3 owner schema; opt in independently of M2 tests."""

import os
from contextlib import contextmanager
from types import SimpleNamespace
from urllib.parse import urlparse
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row


@pytest.fixture
def business_database():
    if os.environ.get("MAINTAI_M3_INTEGRATION") != "1":
        pytest.skip("Enable MAINTAI_M3_INTEGRATION for real PostgreSQL tests")
    dsn = os.environ.get("MAINTAI_M3_DATABASE_URL", "")
    if urlparse(dsn).hostname not in {"localhost", "127.0.0.1"}:
        pytest.fail("M3 tests require the isolated loopback database")
    schema = "m3_" + uuid4().hex
    with psycopg.connect(dsn) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    @contextmanager
    def factory():
        with psycopg.connect(
            dsn,
            row_factory=dict_row,
            connect_timeout=2,
            options="-c statement_timeout=5000 -c lock_timeout=3000",
        ) as conn:
            conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            conn.commit()
            yield conn

    try:
        yield SimpleNamespace(dsn=dsn, schema=schema, factory=factory)
    finally:
        with psycopg.connect(dsn) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
