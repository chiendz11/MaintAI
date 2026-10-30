"""Local browser fixture: signed PKCE issuer and real Kafka/PG/Business runtimes.

Issuer, registry lookup, ML model/provenance and policy are explicit test fixtures.
No production auth service or deployment is created. The gateway is the SDK gateway,
not the outer Ingestion HTTP image. Two owner databases/roles are disposable.
"""

import asyncio
import base64
import hashlib
import importlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse
from uuid import uuid4

import httpx
import jwt
import psycopg
import uvicorn
from api.app import create_app
from api.auth import IdentityVerifier, jwks_fetcher
from business_sse.hub import NotificationHub, SSESettings
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from maintai_messaging.config import KafkaSettings
from maintai_messaging.contracts import canonical_json
from maintai_messaging.postgres import connect
from psycopg import sql
from sqlalchemy.engine import make_url

from backend.shared.persistence.migration import upgrade as business_upgrade
from backend.shared.persistence.permissions import grant_business_permissions
from backend.shared.persistence.projections import ProjectionRepository
from backend.shared.persistence.registry import RegistryRepository
from processing.shared.persistence.connection import upgrade as processing_upgrade

ROOT = Path(__file__).resolve().parents[3]
ISSUER = "http://127.0.0.1:18033"
UI = "http://127.0.0.1:4173"


def browser_history(factory, token_file, signing_key, issuer):
    from api.history import HistoryFacade
    from api.history_charts import CompositeCharts
    from business_processing_client.delegation import DelegationSigner
    from business_processing_client.history import ProcessingHistoryClient
    from business_processing_client.history_schemas import HistorySchemas

    from backend.shared.persistence.history_markers import MarkerSnapshots

    client = ProcessingHistoryClient(
        httpx.AsyncClient(trust_env=False, follow_redirects=False),
        base_url="http://127.0.0.1:18036",
        token_file=token_file,
        signer=DelegationSigner(
            signing_key, kid="m6-fixture", issuer=issuer + "/business", service_subject="m4-browser"
        ),
    )
    schemas = HistorySchemas(ROOT / "contracts/v1.3/schemas")
    # Both replicas receive the same fixture key; production keys are explicit config.
    snapshots = MarkerSnapshots(
        factory, {"fixture": hashlib.sha256(str(token_file).encode()).digest()}, active="fixture"
    )
    return HistoryFacade(client, schemas, markers=CompositeCharts(client, schemas, snapshots))


def serve_fixture():
    dsn = os.environ.get("MAINTAI_M3_DATABASE_URL", "")
    if os.environ.get("MAINTAI_M4_INTEGRATION") != "1" or urlparse(dsn).hostname not in {
        "localhost",
        "127.0.0.1",
    }:
        raise RuntimeError("Explicit M4 local database test opt-in required")
    suffix = uuid4().hex
    m6 = os.environ.get("MAINTAI_M6_INTEGRATION") == "1"
    databases = {
        owner: "m4_browser_" + owner + "_" + suffix for owner in ("business", "processing")
    }
    dsns = {
        owner: make_url(dsn).set(database=name).render_as_string(hide_password=False)
        for owner, name in databases.items()
    }
    roles = {
        name: "m4_" + name + "_" + suffix
        for name in ("reader", "consumer", "api", "processing_reader")
    }
    if m6:
        roles["processing_metadata"] = "m6_metadata_" + suffix
    password = uuid4().hex
    channel = "m4_" + suffix
    alert_channel = "m5_alert_" + suffix
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    delegation_key = rsa.generate_private_key(public_exponent=65537, key_size=2048) if m6 else None
    jwks = {
        "keys": [
            {
                **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())),
                "kid": "browser-fixture",
                "alg": "RS256",
                "use": "sig",
            }
        ]
    }
    codes = {}
    m5 = os.environ.get("MAINTAI_M5_INTEGRATION") == "1"
    processing_server = None
    processing_thread = None
    consumer = None
    edge_server = None
    edge_thread = None
    receipts = {}
    scenarios = {}
    worker = None
    archive_fixture = None
    servers, threads, hubs = [], [], []
    lock = threading.Lock()
    sequence = 2**80
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/history-jwks")
    def history_jwks():
        if delegation_key is None:
            raise HTTPException(503, "M6 fixture not enabled")
        return {
            "keys": [
                {
                    **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(delegation_key.public_key())),
                    "kid": "m6-fixture",
                    "alg": "RS256",
                    "use": "sig",
                }
            ]
        }

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[UI],
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )

    def signed(audience, nonce=None, role=None):
        role = role or ("TECHNICIAN" if m5 else "VIEWER")
        now = int(time.time())
        payload = {
            "iss": ISSUER,
            "aud": audience,
            "sub": "m4-browser",
            "name": "Fixture Viewer",
            "roles": [role],
            "machines": ["M-001"],
            "iat": now,
            "exp": now + 300,
        }
        if nonce is not None:
            payload["nonce"] = nonce
        return jwt.encode(payload, key, algorithm="RS256", headers={"kid": "browser-fixture"})

    @app.get("/.well-known/openid-configuration")
    def discovery():
        return {
            "issuer": ISSUER,
            "authorization_endpoint": ISSUER + "/authorize",
            "token_endpoint": ISSUER + "/token",
            "jwks_uri": ISSUER + "/jwks",
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "code_challenge_methods_supported": ["S256"],
        }

    @app.get("/jwks")
    def keys():
        return jwks

    @app.get("/authorize")
    def authorize(request: Request):
        query = dict(request.query_params)
        if (
            query.get("client_id") not in {"business-ui-fixture", "business-ui-fixture-viewer"}
            or query.get("redirect_uri") != UI + "/auth/callback"
            or query.get("response_type") != "code"
            or query.get("code_challenge_method") != "S256"
            or not query.get("code_challenge")
            or not query.get("state")
        ):
            raise HTTPException(400, "Fixture requires exact public client, redirect and PKCE")
        code = uuid4().hex
        codes[code] = query
        return RedirectResponse(
            query["redirect_uri"] + "?" + urlencode({"code": code, "state": query["state"]})
        )

    @app.post("/token")
    async def token(request: Request):
        if int(request.headers.get("Content-Length", "0")) > 16384:
            raise HTTPException(400, "Oversized fixture token request")
        values = {key: value[0] for key, value in parse_qs((await request.body()).decode()).items()}
        code = codes.pop(values.get("code"), None)
        challenge = (
            base64.urlsafe_b64encode(
                hashlib.sha256(values.get("code_verifier", "").encode()).digest()
            )
            .rstrip(b"=")
            .decode()
        )
        if (
            not code
            or values.get("grant_type") != "authorization_code"
            or values.get("client_id") != code["client_id"]
            or values.get("redirect_uri") != code["redirect_uri"]
            or challenge != code["code_challenge"]
        ):
            raise HTTPException(400, "Invalid fixture PKCE exchange")
        return {
            "access_token": signed(
                "business-test", role="VIEWER" if code["client_id"].endswith("-viewer") else None
            ),
            "token_type": "Bearer",
            "expires_in": 300,
            "scope": "openid profile",
            "id_token": signed(
                code["client_id"],
                code.get("nonce"),
                role="VIEWER" if code["client_id"].endswith("-viewer") else None,
            ),
        }

    @app.get("/health/ready")
    def ready():
        if not all(server.started and hub.ready for server, hub in zip(servers, hubs, strict=True)):
            raise HTTPException(503, "API listeners starting")
        return {"ready": True}

    try:
        with psycopg.connect(dsn, autocommit=True) as admin:
            for name in databases.values():
                admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            for role in roles.values():
                admin.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(role), sql.Literal(password)
                    )
                )
        business_upgrade(dsns["business"])
        processing_upgrade(dsns["processing"])
        business_factory = partial(connect, dsns["business"])
        processing_factory = partial(connect, dsns["processing"])
        RegistryRepository(business_factory).provision(
            [{"machine_id": "M-001", "display_name": "Browser fixture machine"}]
        )
        with business_factory() as connection:
            grant_business_permissions(
                connection,
                reader_role=roles["reader"],
                consumer_role=roles["consumer"],
                api_role=roles["api"],
            )
        reader_dsn = (
            make_url(dsns["business"])
            .set(username=roles["api"] if m5 else roles["reader"], password=password)
            .render_as_string(hide_password=False)
        )
        consumer_dsn = (
            make_url(dsns["business"])
            .set(username=roles["consumer"], password=password)
            .render_as_string(hide_password=False)
        )
        kafka = KafkaSettings(
            local_mode=True,
            security_protocol="PLAINTEXT",
            bootstrap_servers=os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:39092"),
            group_prefix="m3-flow-" + suffix + "-",
        )
        # Existing helper only initializes new UUID local groups; never resets live groups.
        importlib.import_module(
            "backend.business-consumer.tests.test_kafka_runtime"
        ).prepare_new_test_groups(kafka, include_processing=True)
        if m6:
            from history_archive import ArchiveFixture

            archive_fixture = ArchiveFixture(kafka.bootstrap_servers)
            archive_fixture.publish()
            os.environ["AWS_ACCESS_KEY_ID"] = "maintai_m6_test"
            os.environ["AWS_SECRET_ACCESS_KEY"] = "maintai_m6_test_only"
        with tempfile.TemporaryDirectory(prefix="maintai-m4-browser-") as temporary:
            from processing_read_api.app import create_app as read_app
            from processing_read_api.auth import IdentityVerifier as ReadVerifier

            from processing.shared.persistence.status import StatusRepository

            with processing_factory() as connection:
                connection.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                        sql.Identifier(roles["processing_reader"])
                    )
                )
                connection.execute(
                    sql.SQL(
                        "GRANT SELECT ON sensor_events,predictions,"
                        "risk_decisions,processing_results "
                        "TO {}"
                    ).format(sql.Identifier(roles["processing_reader"]))
                )
            read_dsn = (
                make_url(dsns["processing"])
                .set(username=roles["processing_reader"], password=password)
                .render_as_string(hide_password=False)
            )
            read_factory = partial(
                psycopg.connect,
                read_dsn,
                row_factory=psycopg.rows.dict_row,
                options="-c default_transaction_read_only=on -c statement_timeout=3000",
            )
            history_runtime = None
            if m6:
                from types import SimpleNamespace

                from processing_read_api.history_config import HistorySettings
                from processing_read_api.history_runtime import build

                from processing.shared.history_meta.persistence import grant_metadata
                from processing.shared.history_meta.pool import Connections

                cursor_file = Path(temporary) / "processing-history-cursors.json"
                cursor_file.write_text(
                    json.dumps(
                        {
                            "active": "fixture",
                            "keys": {"fixture": base64.b64encode(os.urandom(32)).decode()},
                        }
                    )
                )
                metadata_dsn = (
                    make_url(dsns["processing"])
                    .set(username=roles["processing_metadata"], password=password)
                    .render_as_string(hide_password=False)
                )
                with processing_factory() as connection:
                    grant_metadata(connection, role=roles["processing_metadata"])
                os.environ["PROCESSING_SCHEMA_DIRECTORY"] = str(ROOT / "contracts/v1.3/schemas")
                history_runtime = build(
                    SimpleNamespace(
                        history=HistorySettings(
                            metadata_dsn,
                            ISSUER + "/business",
                            ISSUER + "/history-jwks",
                            str(cursor_file),
                            raw_bucket=archive_fixture.raw_bucket,
                            export_bucket=archive_fixture.export_bucket,
                            s3_endpoint="http://127.0.0.1:39000",
                            s3_region="us-east-1",
                        )
                    ),
                    Connections(read_dsn, readonly=True),
                )
            processing_server = uvicorn.Server(
                uvicorn.Config(
                    read_app(
                        StatusRepository(read_factory),
                        ReadVerifier(
                            issuer=ISSUER,
                            audience="processing-internal",
                            roles_claim="roles",
                            machines_claim="machines",
                            fetch_jwks=lambda: jwks,
                        ),
                        history_runtime=history_runtime,
                    ),
                    host="127.0.0.1",
                    port=18036,
                    access_log=False,
                    log_level="error",
                )
            )
            processing_thread = threading.Thread(target=processing_server.run, daemon=True)
            processing_thread.start()
            service_token = Path(temporary) / "processing-token"
            service_token.write_text(signed("processing-internal", role="BUSINESS_SERVICE"))
            log_path = Path(temporary) / "consumer.log"
            with log_path.open("wb") as log:
                consumer = subprocess.Popen(
                    [sys.executable, "-B", "-m", "business_consumer.main"],
                    stdout=log,
                    stderr=log,
                    env={
                        **os.environ,
                        "BUSINESS_DATABASE_URL": consumer_dsn,
                        "BUSINESS_CONSUMER_PROFILE": "local-test",
                        "BUSINESS_CONSUMER_HOST": "127.0.0.1",
                        "BUSINESS_CONSUMER_PORT": "18034",
                        "BUSINESS_NOTIFY_CHANNEL": channel,
                        "BUSINESS_ALERT_NOTIFY_CHANNEL": alert_channel,
                        "BUSINESS_PROCESSING_READ_URL": "http://127.0.0.1:18036",
                        "BUSINESS_PROCESSING_TOKEN_FILE": str(service_token),
                        "KAFKA_LOCAL_MODE": "true",
                        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
                        "KAFKA_BOOTSTRAP_SERVERS": kafka.bootstrap_servers,
                        "KAFKA_GROUP_PREFIX": kafka.group_prefix,
                    },
                )
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    if (
                        httpx.get(
                            "http://127.0.0.1:18034/health/ready",
                            timeout=1,
                            trust_env=False,
                        ).status_code
                        == 200
                    ):
                        break
                except httpx.HTTPError:
                    pass
                if consumer.poll() is not None:
                    raise RuntimeError("Fixture Consumer exited before readiness")
                time.sleep(0.1)
            else:
                raise RuntimeError("Fixture Consumer did not become ready")
            for selected in (18031, 18032):
                registry = RegistryRepository(partial(connect, reader_dsn))
                from backend.shared.alerts.repository import AlertRepository

                alerts = (
                    AlertRepository(registry.connection_factory, channel=alert_channel)
                    if m5
                    else None
                )
                projections = ProjectionRepository(
                    registry.connection_factory,
                    alert_reader=None if alerts is None else alerts.active,
                )
                hub = NotificationHub(
                    projections,
                    dsn=reader_dsn,
                    settings=SSESettings(channel=channel, alert_channel=alert_channel),
                    alert_repository=alerts,
                )
                verifier = IdentityVerifier(
                    issuer=ISSUER,
                    audience="business-test",
                    roles_claim="roles",
                    machines_claim="machines",
                    fetch_jwks=jwks_fetcher(ISSUER + "/jwks"),
                )
                business = create_app(
                    registry,
                    verifier,
                    projections=projections,
                    notifications=hub,
                    alerts=alerts,
                    history=browser_history(
                        registry.connection_factory, service_token, delegation_key, ISSUER
                    )
                    if m6
                    else None,
                )
                server = uvicorn.Server(
                    uvicorn.Config(
                        business,
                        host="127.0.0.1",
                        port=selected,
                        access_log=False,
                        log_level="error",
                        timeout_graceful_shutdown=2,
                    )
                )
                thread = threading.Thread(target=server.run, daemon=True)
                servers.append(server)
                threads.append(thread)
                hubs.append(hub)
                thread.start()

            from http_ingestion import application

            edge_server = uvicorn.Server(
                uvicorn.Config(
                    application(kafka, ISSUER, jwks, receipts),
                    host="127.0.0.1",
                    port=18035,
                    access_log=False,
                    log_level="error",
                    timeout_graceful_shutdown=2,
                )
            )
            edge_thread = threading.Thread(target=edge_server.run, daemon=True)
            edge_thread.start()
            deadline = time.monotonic() + 10
            while not edge_server.started:
                if time.monotonic() >= deadline:
                    raise RuntimeError("HTTP Ingestion fixture did not start")
                time.sleep(0.05)

            from worker import WorkerHarness

            worker = WorkerHarness(kafka, processing_factory, scenarios)
            worker.start()

            @app.post("/fixture/publish")
            def publish(value: dict):
                nonlocal sequence
                with lock:
                    sequence += 1
                    number = sequence
                    kind = value.get("kind", "completed")
                    payload = json.loads(
                        (ROOT / "contracts/kafka/examples/sensor-request.json").read_text()
                    )
                    payload.update(
                        event_id="browser-" + uuid4().hex,
                        sequence_number=number,
                        received_at=datetime.now(UTC).isoformat(),
                        stream_epoch="m4-browser-" + suffix,
                    )
                    if kind == "invalid":
                        payload["features"]["rotational_speed_rpm"] = "invalid-fixture"

                    scenarios[payload["event_id"]] = kind
                    ack = httpx.post(
                        "http://127.0.0.1:18035/api/v1/sensor-readings",
                        content=canonical_json(payload),
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": "Bearer " + signed("ingestion", role="SENSOR_SERVICE"),
                        },
                        timeout=5,
                        trust_env=False,
                    )
                    if ack.status_code != 202:
                        raise RuntimeError("HTTP Ingestion did not ACK")
                    receipt = receipts.pop(payload["event_id"])
                    deadline = time.monotonic() + 15
                    while StatusRepository(processing_factory).get(payload["event_id"]) is None:
                        if worker.failed or time.monotonic() >= deadline:
                            raise RuntimeError("Kafka Worker did not commit: " + str(worker.failed))
                        time.sleep(0.05)
                    if archive_fixture is not None:
                        archive_fixture.archive(*receipt)
                        history_runtime.history.catalog.sync()
                    return {
                        "event_id": payload["event_id"],
                        "sequence_number": str(number),
                        "ml_calls": 0 if kind == "invalid" else 1,
                        "published": True,
                    }

            @app.post("/fixture/restart-listeners")
            def restart():
                for hub in hubs:
                    if hub.connection is not None:
                        asyncio.run_coroutine_threadsafe(
                            hub.connection.close(), hub.tasks[0].get_loop()
                        ).result(timeout=3)
                return {"restarting": True}

            uvicorn.run(app, host="127.0.0.1", port=18033, access_log=False, log_level="error")
    finally:
        if worker is not None:
            worker.close()
        if processing_server is not None:
            processing_server.should_exit = True
        if processing_thread is not None:
            processing_thread.join(timeout=6)
        if edge_server is not None:
            edge_server.should_exit = True
        if edge_thread is not None:
            edge_thread.join(timeout=6)
        for server in servers:
            server.should_exit = True
        for thread in threads:
            thread.join(timeout=6)
        if archive_fixture is not None:
            archive_fixture.close()
        if consumer is not None:
            consumer.terminate()
            try:
                consumer.wait(timeout=15)
            except subprocess.TimeoutExpired:
                consumer.kill()
                consumer.wait(timeout=5)
        with psycopg.connect(dsn, autocommit=True) as admin:
            for name in databases.values():
                admin.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
                )
            for role in roles.values():
                admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


if __name__ == "__main__":
    # Uvicorn replays captured signals after shutdown. Use a Python exception so
    # the outer finally cleans databases/roles instead of the OS exiting early.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        serve_fixture()
    except KeyboardInterrupt:
        pass
