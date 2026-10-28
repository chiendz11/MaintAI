"""Business tests use an independent temporary owner schema in the test DB."""

import os
from contextlib import contextmanager
from types import SimpleNamespace
from urllib.parse import urlparse
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row


@pytest.fixture(scope="session")
def signing_key():
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def registry_database():
    if os.environ.get("MAINTAI_M2_INTEGRATION") != "1":
        pytest.skip("Business registry PostgreSQL tests were not enabled")
    dsn = os.environ.get("MAINTAI_M2_DATABASE_URL", "")
    if urlparse(dsn).hostname not in {"localhost", "127.0.0.1"}:
        pytest.fail("Registry tests require the isolated loopback database")
    schema = "m2r_" + uuid4().hex
    with psycopg.connect(dsn) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    @contextmanager
    def factory():
        with psycopg.connect(
            dsn, row_factory=dict_row, connect_timeout=2, options="-c statement_timeout=3000"
        ) as conn:
            conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            conn.commit()
            yield conn

    try:
        yield SimpleNamespace(dsn=dsn, schema=schema, factory=factory)
    finally:
        with psycopg.connect(dsn) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
