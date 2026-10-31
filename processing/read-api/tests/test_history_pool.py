"""Separate bounded pools reuse credentials and keep source transactions read-only."""

import psycopg
import pytest
from processing.shared.history_meta.pool import Connections
from processing.shared.history_meta.security import HistoryError

pytestmark = pytest.mark.integration


def test_source_pool_reuses_connections_and_cannot_write(migrated_database):
    db = migrated_database
    pool = Connections(db.dsn, readonly=True, source_schema=db.schema, size=1, timeout=0.05)
    try:
        with pool() as c:
            pid = c.execute("SELECT pg_backend_pid() AS pid").fetchone()["pid"]
            assert (
                c.execute("SHOW default_transaction_read_only").fetchone()[
                    "default_transaction_read_only"
                ]
                == "on"
            )
            with pytest.raises(HistoryError):
                with pool():
                    pass
        with pool() as c:
            assert c.execute("SELECT pg_backend_pid() AS pid").fetchone()["pid"] == pid
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction), c.transaction():
                c.execute("DELETE FROM sensor_events")
    finally:
        pool.close()
    with pytest.raises(HistoryError):
        with pool():
            pass
