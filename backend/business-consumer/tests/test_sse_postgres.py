"""Two real API replicas subscribe to the same committed Business projections."""

import json
import os
import socket
import threading
import time
from uuid import uuid4

import httpx
import jwt
import pytest
import uvicorn
from api.app import create_app
from api.auth import IdentityVerifier
from backend.shared.notifications import Notifier
from backend.shared.persistence.projections import ProjectionRepository
from backend.shared.persistence.registry import RegistryRepository
from backend.shared.projections.projector import Projector
from business_sse.hub import NotificationHub, SSESettings
from cryptography.hazmat.primitives.asymmetric import rsa
from psycopg.conninfo import make_conninfo

from .test_projections import deliver, event, setup

pytestmark = pytest.mark.integration


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait(predicate):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    pytest.fail("SSE replica did not reach expected state")


def data(lines, kind):
    for line in lines:
        if line.startswith("data: "):
            value = json.loads(line[6:])
            if value["type"] == kind:
                return value
    pytest.fail("Stream closed before expected event")


def test_two_http_replicas_auth_scopes_and_committed_sse(business_database):
    if os.environ.get("MAINTAI_M4_INTEGRATION") != "1":
        pytest.skip("Enable MAINTAI_M4_INTEGRATION")
    db = business_database
    setup(db)
    channel = "m4_" + uuid4().hex
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwks = {
        "keys": [
            {
                **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())),
                "kid": "test",
                "alg": "RS256",
                "use": "sig",
            }
        ]
    }
    verifier = IdentityVerifier(
        issuer="https://fixture.test",
        audience="business-test",
        roles_claim="roles",
        machines_claim="machines",
        fetch_jwks=lambda: jwks,
    )
    token = jwt.encode(
        {
            "iss": "https://fixture.test",
            "aud": "business-test",
            "sub": "fixture",
            "roles": ["VIEWER"],
            "machines": ["M-001"],
            "exp": int(time.time()) + 30,
        },
        key,
        algorithm="RS256",
        headers={"kid": "test"},
    )
    hubs, servers, threads, origins = [], [], [], []
    try:
        for _ in range(2):
            repository = ProjectionRepository(db.factory)
            hub = NotificationHub(
                repository,
                dsn=make_conninfo(db.dsn, options=f"-c search_path={db.schema}"),
                settings=SSESettings(channel=channel, coalesce_seconds=0.02, heartbeat_seconds=0.1),
            )
            app = create_app(
                RegistryRepository(db.factory), verifier, projections=repository, notifications=hub
            )
            selected = port()
            server = uvicorn.Server(
                uvicorn.Config(
                    app,
                    host="127.0.0.1",
                    port=selected,
                    access_log=False,
                    log_level="error",
                    timeout_graceful_shutdown=2,
                )
            )
            thread = threading.Thread(target=server.run, daemon=True)
            hubs.append(hub)
            servers.append(server)
            threads.append(thread)
            origins.append(f"http://127.0.0.1:{selected}")
            thread.start()
        wait(
            lambda: all(
                server.started and hub.ready for server, hub in zip(servers, hubs, strict=True)
            )
        )
        with httpx.Client(timeout=5, trust_env=False) as http:
            headers = {"Authorization": "Bearer " + token}
            assert (
                http.get(
                    origins[0] + "/api/v1/events/stream?machine_id=M-002", headers=headers
                ).status_code
                == 403
            )
            assert (
                http.get(origins[0] + "/api/v1/events/stream?machine_id=M-001").status_code == 401
            )
            with (
                http.stream(
                    "GET", origins[0] + "/api/v1/events/stream?machine_id=M-001", headers=headers
                ) as first,
                http.stream(
                    "GET", origins[1] + "/api/v1/events/stream?machine_id=M-001", headers=headers
                ) as second,
            ):
                assert first.status_code == second.status_code == 200
                wait(lambda: all(len(hub.subscriptions) == 1 for hub in hubs))
                projector = Projector(notify=Notifier(channel))
                deliver(db, projector, event("sensor-reading", 2**80 + 1))
                first_lines, second_lines = first.iter_lines(), second.iter_lines()
                for lines in (first_lines, second_lines):
                    value = data(lines, "telemetry.received")
                    assert value["data"]["sequence_number"] == 2**80 + 1
                    assert value["data"]["validation_status"] == "UNVERIFIED"
                deliver(db, projector, event("completed", 2**80 + 1))
                for lines in (first_lines, second_lines):
                    value = data(lines, "prediction.completed")
                    assert value["data"]["source_event_id"] == f"e-{2**80 + 1}"
                deliver(db, projector, event("invalid", 2**80 + 2))
                value = data(first_lines, "processing.status_changed")
                while value["data"]["processing_status"] != "INVALID":
                    value = data(first_lines, "processing.status_changed")
                live = http.get(origins[0] + "/api/v1/machines/M-001/live", headers=headers).json()
                assert live["latest_valid_prediction"]["sequence_number"] == 2**80 + 1
                assert http.get(origins[0] + "/api/v1/machines", headers=headers).status_code == 503
            wait(lambda: not any(hub.subscriptions for hub in hubs))
            for hub in hubs:
                assert hub.pending_bytes == 0
    finally:
        for server in servers:
            server.should_exit = True
        for thread in threads:
            thread.join(timeout=8)
        assert not any(thread.is_alive() for thread in threads)
