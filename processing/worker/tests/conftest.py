"""Opt-in PostgreSQL fixtures isolated in an expendable owner schema."""

import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import pytest
from alembic import command
from alembic.config import Config
from maintai_messaging.postgres import connect
from psycopg import sql
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[3]


@dataclass
class OwnerDatabase:
    dsn: str
    schema: str

    @contextmanager
    def factory(self):
        with connect(self.dsn) as connection:
            connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema)))
            connection.commit()
            yield connection

    def migrate(self):
        config = Config()
        config.set_main_option("script_location", str(ROOT / "processing/migrations"))
        engine = create_engine(
            make_url(self.dsn).set(drivername="postgresql+psycopg"),
            connect_args={"options": f"-csearch_path={self.schema}"},
        )
        try:
            with engine.connect() as connection:
                config.attributes["connection"] = connection
                command.upgrade(config, "head")
        finally:
            engine.dispose()


@pytest.fixture
def owner_database():
    if os.environ.get("MAINTAI_M2_INTEGRATION") != "1":
        pytest.skip("M2 PostgreSQL/Kafka integration was not explicitly enabled")
    dsn = os.environ.get("MAINTAI_M2_DATABASE_URL", "")
    if urlparse(dsn).hostname not in {"127.0.0.1", "localhost"}:
        pytest.fail("M2 tests require an isolated loopback database")
    database = OwnerDatabase(dsn, "m2_" + uuid.uuid4().hex)
    with connect(dsn) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(database.schema)))
    try:
        yield database
    finally:
        with connect(dsn) as connection:
            # M6 metadata is a separate namespace of this isolated test owner.
            from processing.shared.history_meta.persistence import metadata_schema

            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(metadata_schema(database.schema))
                )
            )
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(database.schema))
            )


@pytest.fixture
def migrated_database(owner_database):
    owner_database.migrate()
    return owner_database


@pytest.fixture
def pipeline_database(owner_database):
    """The real runtime uses its default schema in an entirely separate test DB."""
    from sqlalchemy.engine import make_url

    name = "m2_pipeline_" + uuid.uuid4().hex
    with connect(owner_database.dsn) as connection:
        connection.autocommit = True
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = make_url(owner_database.dsn).set(database=name).render_as_string(hide_password=False)
    database = OwnerDatabase(dsn, "public")
    try:
        database.migrate()
        yield database
    finally:
        with connect(owner_database.dsn) as connection:
            connection.autocommit = True
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


@pytest.fixture
def business_registry_service(owner_database):
    """Actual Business API/DB over HTTP; signed-token issuer remains a test double."""
    import json
    import threading
    import time
    from types import SimpleNamespace

    import jwt
    import uvicorn
    from api.app import create_app
    from api.auth import IdentityVerifier
    from backend.shared.persistence.migration import upgrade
    from backend.shared.persistence.registry import RegistryRepository
    from cryptography.hazmat.primitives.asymmetric import rsa

    name = "m2_business_" + uuid.uuid4().hex
    with connect(owner_database.dsn) as connection:
        connection.autocommit = True
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = make_url(owner_database.dsn).set(database=name).render_as_string(hide_password=False)
    server = thread = None
    try:
        upgrade(dsn)
        repo = RegistryRepository(lambda: connect(dsn))
        repo.provision([{"machine_id": "M-001", "display_name": "Pipeline fixture machine"}])
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk = {
            **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())),
            "kid": "pipeline-test",
            "alg": "RS256",
            "use": "sig",
        }
        verifier = IdentityVerifier(
            issuer="https://pipeline-issuer.test",
            audience="business-test",
            roles_claim="roles",
            machines_claim="machines",
            fetch_jwks=lambda: {"keys": [jwk]},
        )
        token = jwt.encode(
            {
                "iss": "https://pipeline-issuer.test",
                "aud": "business-test",
                "sub": "test-processing",
                "exp": int(time.time()) + 300,
                "roles": ["VIEWER"],
                "machines": ["M-001"],
            },
            key,
            algorithm="RS256",
            headers={"kid": "pipeline-test"},
        )
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(repo, verifier),
                host="127.0.0.1",
                port=0,
                access_log=False,
                log_level="warning",
                log_config=None,
            )
        )
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 5
        while not server.started:
            if not thread.is_alive() or time.monotonic() >= deadline:
                pytest.fail("Business registry HTTP test server did not start")
            time.sleep(0.01)
        port = server.servers[0].sockets[0].getsockname()[1]
        yield SimpleNamespace(origin=f"http://127.0.0.1:{port}", token=token)
    finally:
        if server:
            server.should_exit = True
        if thread:
            thread.join(timeout=5)
        with connect(owner_database.dsn) as connection:
            connection.autocommit = True
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )
