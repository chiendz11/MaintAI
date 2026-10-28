"""Verify, never delete, the isolated browser fixture's database/role cleanup."""

import os
import time
from urllib.parse import urlparse

import psycopg

if os.environ.get("MAINTAI_M4_INTEGRATION") == "1":
    dsn = os.environ.get("MAINTAI_M3_DATABASE_URL", "")
    if urlparse(dsn).hostname not in {"localhost", "127.0.0.1"}:
        raise RuntimeError("Cleanup verification requires the explicit local test database")
    deadline = time.monotonic() + 15
    while True:
        with psycopg.connect(dsn, connect_timeout=2) as connection:
            databases = connection.execute(
                "SELECT count(*) FROM pg_database WHERE datname ~ "
                "'^m4_browser_(business|processing)_[0-9a-f]{32}$'"
            ).fetchone()[0]
            roles = connection.execute(
                "SELECT count(*) FROM pg_roles WHERE rolname ~ "
                "'^m4_(reader|consumer)_[0-9a-f]{32}$'"
            ).fetchone()[0]
        if databases == roles == 0:
            print("Browser fixture databases/roles: cleanup PASS")
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("Browser fixture left temporary databases or roles")
        time.sleep(0.1)
