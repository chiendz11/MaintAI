"""Signed delegation and snapshot cursors never trust browser actor headers."""

import json
import time
from types import SimpleNamespace

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from processing.shared.history_meta.security import CursorKeys, DelegationVerifier, HistoryError


@pytest.fixture
def delegation():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwks = {
        "keys": [
            {
                **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())),
                "kid": "history-test",
                "alg": "RS256",
                "use": "sig",
            }
        ]
    }
    verifier = DelegationVerifier(issuer="https://business.test", fetch_jwks=lambda: jwks)
    now = int(time.time())
    claims = {
        "iss": "https://business.test",
        "aud": "processing-history",
        "sub": "technician",
        "azp": "business-service",
        "iat": now,
        "nbf": now,
        "exp": now + 60,
        "jti": "fixture",
        "roles": ["TECHNICIAN"],
        "machines": ["M-001"],
    }
    service = SimpleNamespace(subject="business-service", machines=frozenset({"M-001", "M-002"}))
    return verifier, key, claims, service


def sign(key, claims):
    return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "history-test"})


def test_verified_delegation_intersects_service_scope(delegation):
    verifier, key, claims, service = delegation
    identity = verifier.verify(sign(key, claims), service)
    assert identity.subject == "technician"
    assert identity.machines == frozenset({"M-001"})


@pytest.mark.parametrize(
    "change",
    [
        {"azp": "another-service"},
        {"aud": "other"},
        {"exp": 1},
        {"machines": ["M-003"]},
        {"roles": ["SENSOR_SERVICE"]},
        {"machines": ["*"]},
    ],
)
def test_delegation_rejects_cross_service_or_scope_and_bad_claims(delegation, change):
    verifier, key, claims, service = delegation
    with pytest.raises(HistoryError):
        verifier.verify(sign(key, {**claims, **change}), service)


def test_delegation_rejects_long_lifetime_and_bad_signature(delegation):
    verifier, key, claims, service = delegation
    with pytest.raises(HistoryError):
        verifier.verify(sign(key, {**claims, "exp": claims["iat"] + 61}), service)
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with pytest.raises(HistoryError):
        verifier.verify(sign(other, claims), service)


def test_cursor_binds_actor_scope_and_expiry_and_rejects_tampering():
    keys = CursorKeys({"v1": b"x" * 32}, active="v1", clock=lambda: 10)
    payload = {"actor": "tech", "scope": "digest", "snapshot": "s", "exp": 20, "last": 3}
    token = keys.encode(payload)
    assert keys.decode(token, actor="tech", scope="digest")["last"] == 3
    for actor, scope in [("other", "digest"), ("tech", "another")]:
        with pytest.raises(HistoryError):
            keys.decode(token, actor=actor, scope=scope)
    with pytest.raises(HistoryError):
        keys.decode(token + "x", actor="tech", scope="digest")
    expired = CursorKeys({"v1": b"x" * 32}, active="v1", clock=lambda: 20)
    with pytest.raises(HistoryError) as error:
        expired.decode(token, actor="tech", scope="digest")
    assert error.value.status == 410


def test_cursor_rotation_reads_previous_key_without_signing_with_it():
    old = CursorKeys({"old": b"o" * 32}, active="old", clock=lambda: 1)
    rotated = CursorKeys({"old": b"o" * 32, "new": b"n" * 32}, active="new", clock=lambda: 1)
    payload = {"actor": "a", "scope": "s", "exp": 2}
    assert rotated.decode(old.encode(payload), actor="a", scope="s")
    assert rotated.encode(payload).startswith("new.")
