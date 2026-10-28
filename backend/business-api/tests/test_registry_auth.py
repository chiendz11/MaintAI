"""Real JWT signatures against an in-memory test issuer, not production OIDC."""

import json
import time

import jwt
import pytest
from api.auth import IdentityVerifier, RegistryFailure


def document(key):
    return {
        "keys": [
            {
                **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())),
                "kid": "test-key",
                "alg": "RS256",
                "use": "sig",
            }
        ]
    }


def token(key, **changes):
    claims = {
        "iss": "https://issuer.test",
        "aud": "business-test",
        "sub": "reader",
        "exp": int(time.time()) + 120,
        "roles": ["VIEWER"],
        "machines": ["M-001"],
    }
    claims.update(changes)
    return "Bearer " + jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test-key"})


def verifier(key, **kwargs):
    return IdentityVerifier(
        issuer="https://issuer.test",
        audience="business-test",
        roles_claim="roles",
        machines_claim="machines",
        fetch_jwks=lambda: document(key),
        **kwargs,
    )


def test_signed_identity_preserves_explicit_machine_scope(signing_key):
    identity = verifier(signing_key).verify(token(signing_key))
    assert identity.subject == "reader"
    identity.authorize("M-001")
    with pytest.raises(RegistryFailure) as exc:
        identity.authorize("M-002")
    assert (exc.value.status, exc.value.code) == (403, "FORBIDDEN")


@pytest.mark.parametrize(
    "changes",
    [
        {"exp": 1},
        {"iss": "https://wrong.test"},
        {"aud": "wrong"},
        {"sub": None},
    ],
)
def test_invalid_verified_claims_are_unauthorized(signing_key, changes):
    with pytest.raises(RegistryFailure) as exc:
        verifier(signing_key).verify(token(signing_key, **changes))
    assert exc.value.status == 401


@pytest.mark.parametrize(
    "changes",
    [
        {"roles": ["SENSOR_SERVICE"]},
        {"roles": "VIEWER"},
        {"machines": []},
        {"machines": ["*"]},
        {"machines": "M-001"},
    ],
)
def test_role_and_machine_claims_do_not_grant_implicit_access(signing_key, changes):
    with pytest.raises(RegistryFailure) as exc:
        verifier(signing_key).verify(token(signing_key, **changes))
    assert exc.value.status == 403


@pytest.mark.parametrize("authorization", [None, "", "Basic abc", "Bearer broken"])
def test_missing_or_malformed_token_is_unauthorized(signing_key, authorization):
    with pytest.raises(RegistryFailure) as exc:
        verifier(signing_key).verify(authorization)
    assert exc.value.status == 401


def test_expired_jwks_cache_is_not_used_during_issuer_outage(signing_key):
    now, down = [0], [False]

    def fetch():
        if down[0]:
            raise OSError("fixture issuer unavailable")
        return document(signing_key)

    auth = IdentityVerifier(
        issuer="https://issuer.test",
        audience="business-test",
        roles_claim="roles",
        machines_claim="machines",
        fetch_jwks=fetch,
        clock=lambda: now[0],
        cache_seconds=5,
    )
    auth.verify(token(signing_key))
    now[0], down[0] = 10, True
    with pytest.raises(RegistryFailure) as exc:
        auth.verify(token(signing_key))
    assert (exc.value.status, exc.value.code) == (503, "DEPENDENCY_UNAVAILABLE")
