import os
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from drift_monitor.repository import PostgresRepository

from .support import create_fixture_tables


@pytest.fixture
def postgres_repo():
    base_url = os.environ.get("DRIFT_TEST_DATABASE_URL")
    if not base_url:
        pytest.skip("DRIFT_TEST_DATABASE_URL not set")
    name = f"drift_test_{uuid4().hex}"
    with psycopg.connect(base_url, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
    url = make_conninfo(base_url, options=f"-c search_path={name}")
    repo = PostgresRepository(url)
    try:
        create_fixture_tables(url)
        repo.init_db()
        yield repo
    finally:
        with psycopg.connect(base_url, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))
