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
from types import SimpleNamespace
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
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.ingestion import IngestionGateway, SourceIdentity
from maintai_messaging.postgres import OutboxRelay, connect
from maintai_messaging.producer import KafkaPublisher
from psycopg import sql
from sqlalchemy.engine import make_url

from backend.shared.persistence.migration import upgrade as business_upgrade
from backend.shared.persistence.permissions import grant_business_permissions
from backend.shared.persistence.projections import ProjectionRepository
from backend.shared.persistence.registry import RegistryRepository
from processing.shared.inference_client.client import PredictionAttempt
from processing.shared.persistence.connection import upgrade as processing_upgrade
from processing.worker.tests.test_worker_transaction import Providers, handler

ROOT = Path(__file__).resolve().parents[3]
ISSUER = "http://127.0.0.1:18033"
UI = "http://127.0.0.1:4173"


def serve_fixture():
    dsn = os.environ.get("MAINTAI_M3_DATABASE_URL", "")
    if os.environ.get("MAINTAI_M4_INTEGRATION") != "1" or urlparse(dsn).hostname not in {
        "localhost",
        "127.0.0.1",
    }:
        raise RuntimeError("Explicit M4 local database test opt-in required")
    suffix = uuid4().hex
    databases = {
        owner: "m4_browser_" + owner + "_" + suffix for owner in ("business", "processing")
    }
    dsns = {
        owner: make_url(dsn).set(database=name).render_as_string(hide_password=False)
        for owner, name in databases.items()
    }
    roles = {name: "m4_" + name + "_" + suffix for name in ("reader", "consumer")}
    password = uuid4().hex
    channel = "m4_" + suffix
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
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
    consumer = None
    servers, threads, hubs = [], [], []
    lock = threading.Lock()
    sequence = 2**80
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[UI],
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )

    def signed(audience, nonce=None):
        now = int(time.time())
        payload = {
            "iss": ISSUER,
            "aud": audience,
            "sub": "m4-browser",
            "name": "Fixture Viewer",
            "roles": ["VIEWER"],
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
            query.get("client_id") != "business-ui-fixture"
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
            "access_token": signed("business-test"),
            "token_type": "Bearer",
            "expires_in": 300,
            "scope": "openid profile",
            "id_token": signed("business-ui-fixture", code.get("nonce")),
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
                connection, reader_role=roles["reader"], consumer_role=roles["consumer"]
            )
        reader_dsn = (
            make_url(dsns["business"])
            .set(username=roles["reader"], password=password)
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
        ).prepare_new_test_groups(kafka)
        with tempfile.TemporaryDirectory(prefix="maintai-m4-browser-") as temporary:
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
                projections = ProjectionRepository(registry.connection_factory)
                hub = NotificationHub(
                    projections, dsn=reader_dsn, settings=SSESettings(channel=channel)
                )
                verifier = IdentityVerifier(
                    issuer=ISSUER,
                    audience="business-test",
                    roles_claim="roles",
                    machines_claim="machines",
                    fetch_jwks=jwks_fetcher(ISSUER + "/jwks"),
                )
                business = create_app(
                    registry, verifier, projections=projections, notifications=hub
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

                    class Capture:
                        def __init__(self, publisher):
                            self.publisher = publisher
                            self.settings = publisher.settings

                        def publish(self, topic, payload):
                            delivery = self.publisher.publish(topic, payload)
                            event = default_contracts().decode(topic, canonical_json(payload))
                            self.event = event
                            self.record = Record(
                                event.topic,
                                delivery.partition,
                                delivery.offset,
                                event.key.encode(),
                                event.wire_bytes,
                            )

                    class CurrentProviders(Providers):
                        def predict(self, **kwargs):
                            time.sleep(1.5)
                            attempt = super().predict(**kwargs)
                            if attempt.response is None:
                                return attempt
                            return PredictionAttempt(
                                attempt.response.model_copy(
                                    update={"predicted_at": datetime.now(UTC)}
                                ),
                                None,
                                1,
                            )

                    with KafkaPublisher(kafka, "ingestion") as publisher:
                        capture = Capture(publisher)
                        ack = IngestionGateway(capture, "http://127.0.0.1:18031").handle(
                            canonical_json(payload),
                            identity=SourceIdentity("fixture", frozenset({"M-001"})),
                            request_id="req-m4",
                            correlation_id="cor-m4",
                        )
                    if ack.status != 202:
                        raise RuntimeError("Fixture gateway did not ACK")
                    providers = CurrentProviders(
                        probability=0.12,
                        failure="ML_UNAVAILABLE" if kind == "unavailable" else None,
                    )
                    handler(SimpleNamespace(factory=processing_factory), providers)(
                        capture.event, capture.record
                    )
                    with KafkaPublisher(kafka, "outbox") as publisher:
                        published = OutboxRelay(processing_factory, publisher).run_once()
                    return {
                        "event_id": payload["event_id"],
                        "sequence_number": str(number),
                        "ml_calls": providers.calls,
                        "published": published,
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
        for server in servers:
            server.should_exit = True
        for thread in threads:
            thread.join(timeout=6)
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
