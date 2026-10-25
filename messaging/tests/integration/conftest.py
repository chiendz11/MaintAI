"""Real owner DB tests can run without Docker/Kafka, with an explicit local DSN."""

import os
import uuid
from contextlib import contextmanager
from urllib.parse import urlparse

import pytest
from maintai_messaging.postgres import apply_messaging_schema, connect


@pytest.fixture
def owner_db(contracts):
    if not any(
        os.environ.get(name) == "1"
        for name in ("MAINTAI_KAFKA_INTEGRATION", "MAINTAI_POSTGRES_INTEGRATION")
    ):
        pytest.skip("Local PostgreSQL integration was not explicitly enabled")
    dsn = os.environ.get("KAFKA_TEST_DATABASE_URL", "")
    if urlparse(dsn).hostname not in {"127.0.0.1", "localhost"}:
        pytest.fail("Tests require an isolated loopback PostgreSQL database")
    from psycopg import sql

    name = "ordering_it_" + uuid.uuid4().hex
    with connect(dsn) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))

    @contextmanager
    def factory():
        with connect(dsn) as connection:
            connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(name)))
            connection.commit()
            yield connection

    with factory() as connection:
        apply_messaging_schema(connection, contracts)
        connection.execute("CREATE TABLE effects (id TEXT PRIMARY KEY,total INTEGER NOT NULL)")
    try:
        yield factory
    finally:
        with connect(dsn) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))
