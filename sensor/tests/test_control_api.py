"""Provider contract, scoped auth, restart receipts and mutation boundaries."""

import json
import time
from pathlib import Path

import jwt
import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker

from simulator.control.api import create_app
from simulator.control.auth import AuthError, Principal, TokenVerifier
from simulator.scheduling.engine import SensorEngine
from simulator.settings import RuntimeSettings

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[2] / "contracts/v1.3/schemas/runtime.schema.json").read_text()
)["$defs"]
BASE = "/api/v1/simulator/machines"


def validate(name, value):
    root = {"$defs": SCHEMA, "$ref": f"#/$defs/{name}"}
    Draft202012Validator(root, format_checker=FormatChecker()).validate(value)


@pytest.fixture
def api(runtime_settings):
    engine = SensorEngine(runtime_settings)
    with TestClient(
        base_url="http://127.0.0.1",
        app=create_app(runtime_settings, engine=engine, background=False),
        client=("127.0.0.1", 50000),
    ) as client:
        yield client, engine


def test_all_five_operations_use_frozen_dtos_and_persist_before_ack(api):
    client, engine = api
    assert client.get(BASE, headers={"Host": "untrusted.example"}).status_code == 403
    response = client.get(BASE)
    assert response.status_code == 200
    validate("ControlList", response.json())
    assert len(response.json()["items"]) == 2
    validate("ControlState", client.get(BASE + "/M-001/control").json())
    version = 0
    for action, body in [("pause", {}), ("scenario", {"scenario": "recovery"}), ("resume", {})]:
        method = client.put if action == "scenario" else client.post
        response = method(
            BASE + "/M-001/" + action,
            headers={"Idempotency-Key": action},
            json={"expected_control_version": version, **body},
        )
        assert response.status_code == 200
        validate("ControlState", response.json())
        assert response.json() == engine.store.get("M-001")
        assert response.json()["last_controlled_by"] == "local-operator"
        if action == "scenario":
            assert response.json()["stream_state"] == "PAUSED"
        version = response.json()["control_version"]
    assert client.get(BASE + "/M-002/control").json()["control_version"] == 0


def test_idempotency_conflicts_and_durable_receipt(api):
    client, _ = api
    path = BASE + "/M-001/pause"
    headers = {"Idempotency-Key": "key"}
    one = client.post(path, headers=headers, json={"expected_control_version": 0})
    assert (
        client.post(path, headers=headers, json={"expected_control_version": 0}).json()
        == one.json()
    )
    bad = client.post(path, headers=headers, json={"expected_control_version": 1})
    assert bad.status_code == 409
    assert bad.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    bad = client.post(
        path, headers={"Idempotency-Key": "new"}, json={"expected_control_version": 0}
    )
    assert bad.status_code == 409
    assert bad.json()["error"]["code"] == "CONTROL_VERSION_CONFLICT"
    validate("Error", bad.json())


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"expected_control_version": True},
        {"expected_control_version": -1},
        {"expected_control_version": 0, "extra": "no"},
    ],
)
def test_invalid_command_never_mutates(api, body):
    client, engine = api
    result = client.post(BASE + "/M-001/pause", headers={"Idempotency-Key": "x"}, json=body)
    assert result.status_code == 422
    validate("Error", result.json())
    assert engine.store.get("M-001")["control_version"] == 0


def test_malformed_duplicate_keys_oversize_and_unsupported_scenario(api):
    client, _ = api
    headers = {"Idempotency-Key": "x", "Content-Type": "application/json"}
    assert client.post(BASE + "/M-001/pause", headers=headers, content="{").status_code == 400
    assert (
        client.post(
            BASE + "/M-001/pause",
            headers=headers,
            content='{"expected_control_version":0,"expected_control_version":0}',
        ).status_code
        == 400
    )
    assert (
        client.post(BASE + "/M-001/pause", headers=headers, content=" " * 5000).status_code == 422
    )
    assert (
        client.put(
            BASE + "/M-001/scenario",
            headers=headers,
            json={"expected_control_version": 0, "scenario": "made-up"},
        ).status_code
        == 422
    )
    assert (
        client.post(BASE + "/M-001/pause", json={"expected_control_version": 0}).status_code == 422
    )


def test_metrics_probes_and_cors(api):
    client, engine = api
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").status_code == 200
    client.get(BASE)
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert 'maintai_simulator_machines{state="RUNNING"} 2' in response.text
    assert 'route="/api/v1/simulator/machines"' in response.text
    assert 'route="/health' not in response.text
    assert "machine_id=" not in response.text
    engine.fatal = "DiskError"
    assert client.get("/health/ready").status_code == 503
    assert client.get("/health/live").status_code == 503


def test_local_auth_rejects_remote_peer_even_with_forwarded_headers(runtime_settings):
    engine = SensorEngine(runtime_settings)
    with TestClient(
        base_url="http://127.0.0.1",
        app=create_app(runtime_settings, engine=engine, background=False),
        client=("192.0.2.1", 5000),
    ) as client:
        assert client.get(BASE, headers={"X-Forwarded-For": "127.0.0.1"}).status_code == 403


class ScopeVerifier:
    def verify(self, authorization, peer):
        if authorization != "Bearer operator":
            raise AuthError(401, "UNAUTHENTICATED")
        return Principal("alice", frozenset({"M-001"}))


def test_list_and_commands_enforce_machine_scope(runtime_settings):
    engine = SensorEngine(runtime_settings)
    with TestClient(
        base_url="http://127.0.0.1",
        app=create_app(runtime_settings, engine=engine, verifier=ScopeVerifier(), background=False),
    ) as client:
        assert client.get(BASE).status_code == 401
        headers = {"Authorization": "Bearer operator", "Idempotency-Key": "x"}
        assert [x["machine_id"] for x in client.get(BASE, headers=headers).json()["items"]] == [
            "M-001"
        ]
        assert client.get(BASE + "/M-002/control", headers=headers).status_code == 403
        assert (
            client.post(
                BASE + "/M-002/pause", headers=headers, json={"expected_control_version": 0}
            ).status_code
            == 403
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"iss": "https://wrong.example"},
        {"aud": "business"},
        {"exp": 1},
        {"roles": ["VIEWER"]},
        {"scope": "business:read"},
        {"machine_ids": "*"},
        {"sub": ""},
    ],
)
def test_signed_jwt_still_requires_claims_and_permissions(runtime_settings, changes):
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    s = runtime_settings.model_copy(
        update={
            "auth_mode": "jwt",
            "jwt_issuer": "https://idp.example",
            "jwks_url": "https://idp.example/jwks",
        }
    )
    verifier = TokenVerifier(s)
    verifier.jwks.get_signing_key_from_jwt = lambda token: jwt.PyJWK.from_json(
        jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())
    )
    claims = {
        "iss": s.jwt_issuer,
        "aud": s.jwt_audience,
        "sub": "alice",
        "iat": int(time.time()),
        "exp": int(time.time()) + 60,
        "scope": "simulator:control",
        "roles": ["SIMULATOR_OPERATOR"],
        "machine_ids": ["M-001"],
        **changes,
    }
    with pytest.raises(AuthError) as error:
        verifier.verify("Bearer " + jwt.encode(claims, key, algorithm="RS256"), "127.0.0.1")
    assert error.value.status in (401, 403)


@pytest.mark.parametrize(
    "changes",
    [
        {"auth_mode": "local", "host": "0.0.0.0"},
        {"auth_mode": "jwt", "jwks_url": "http://idp.example/jwks"},
        {"ingestion_auth": "none", "backend_base_url": "https://ingestion.example"},
        {"cors_origins": ["*"]},
        {"interval_ms": 0},
    ],
)
def test_unsafe_runtime_configuration_fails_closed(runtime_settings, changes):
    with pytest.raises(ValueError):
        RuntimeSettings.model_validate({**runtime_settings.model_dump(), **changes})
