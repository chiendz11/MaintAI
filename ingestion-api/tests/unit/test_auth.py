import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from maintai_ingestion.auth import OidcIdentityVerifier
from maintai_ingestion.errors import EdgeFailure


@pytest.fixture
def keys():
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private.public_key()))
    jwk.update(kid="fixture", alg="RS256", use="sig")
    return private, {"keys": [jwk]}


def verifier(keys, **kw):
    return OidcIdentityVerifier(
        issuer="https://issuer.example.invalid",
        audience="ingestion",
        algorithms=("RS256",),
        roles_claim="roles",
        machines_claim="machine_ids",
        fetch_jwks=lambda: keys[1],
        **kw,
    )


def token(keys, **claims):
    payload = {
        "iss": "https://issuer.example.invalid",
        "aud": "ingestion",
        "sub": "sensor",
        "exp": int(time.time()) + 60,
        "roles": ["SENSOR_SERVICE"],
        "machine_ids": ["M-001"],
        **claims,
    }
    return "Bearer " + jwt.encode(payload, keys[0], algorithm="RS256", headers={"kid": "fixture"})


def test_signed_token_produces_verified_source(keys):
    identity = verifier(keys).verify(token(keys))
    assert identity.subject == "sensor"
    assert identity.allowed_machine_ids == frozenset({"M-001"})


@pytest.mark.parametrize(
    "claims",
    [
        {"iss": "other"},
        {"aud": "business"},
        {"exp": 0},
        {"exp": None},
        {"sub": ""},
        {"nbf": int(time.time()) + 1000},
    ],
)
def test_invalid_claims_fail_closed(keys, claims):
    with pytest.raises(EdgeFailure) as exc:
        verifier(keys).verify(token(keys, **claims))
    assert exc.value.status == 401


@pytest.mark.parametrize(
    "claims",
    [
        {"roles": ["VIEWER"]},
        {"roles": "SENSOR_SERVICE"},
        {"machine_ids": []},
        {"machine_ids": "*"},
        {"machine_ids": ["*"]},
    ],
)
def test_permission_claims_are_not_coerced(keys, claims):
    with pytest.raises(EdgeFailure) as exc:
        verifier(keys).verify(token(keys, **claims))
    assert exc.value.status == 403


@pytest.mark.parametrize("header", [None, "", "Basic abc", "Bearer bad", "Bearer x y"])
def test_missing_or_malformed_credentials(keys, header):
    with pytest.raises(EdgeFailure) as exc:
        verifier(keys).verify(header)
    assert exc.value.status == 401


def test_bad_signature_and_disallowed_algorithm(keys):
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    raw = token(keys).removeprefix("Bearer ")
    claims = jwt.decode(raw, options={"verify_signature": False})
    for forged in [
        jwt.encode(claims, other, algorithm="RS256", headers={"kid": "fixture"}),
        jwt.encode(claims, "fixture-secret-long-enough-for-tests-only", algorithm="HS256"),
    ]:
        with pytest.raises(EdgeFailure) as exc:
            verifier(keys).verify("Bearer " + forged)
        assert exc.value.status == 401


def test_missing_required_claim(keys):
    claims = {"iss": "https://issuer.example.invalid", "aud": "ingestion", "sub": "sensor"}
    raw = jwt.encode(claims, keys[0], algorithm="RS256", headers={"kid": "fixture"})
    with pytest.raises(EdgeFailure) as exc:
        verifier(keys).verify("Bearer " + raw)
    assert exc.value.status == 401


def test_jwks_failure_no_cache_is_unavailable(keys):
    def unavailable():
        raise OSError("sensitive provider detail")

    v = verifier(keys)
    v.fetch_jwks = unavailable
    with pytest.raises(EdgeFailure) as exc:
        v.verify(token(keys))
    assert exc.value.status == 503
    assert "sensitive" not in str(exc.value)


def test_cached_key_expires_and_does_not_fail_open(keys):
    now = [0.0]
    v = verifier(keys, clock=lambda: now[0], cache_seconds=10)
    v.verify(token(keys))
    v.fetch_jwks = lambda: (_ for _ in ()).throw(OSError())
    now[0] = 5
    assert v.verify(token(keys)).subject == "sensor"
    now[0] = 11
    with pytest.raises(EdgeFailure) as exc:
        v.verify(token(keys))
    assert exc.value.status == 503


def test_unknown_kid_does_not_authorize(keys):
    raw = jwt.encode(
        {"exp": int(time.time()) + 60}, keys[0], algorithm="RS256", headers={"kid": "unknown"}
    )
    with pytest.raises(EdgeFailure) as exc:
        verifier(keys).verify("Bearer " + raw)
    assert exc.value.status == 401


def test_jwks_fetch_has_total_deadline_against_slow_drip():
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from maintai_ingestion.auth import jwks_fetcher

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            try:
                for byte in b'{"keys":[]}':
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.05)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        start = time.monotonic()
        try:
            jwks_fetcher(f"http://127.0.0.1:{server.server_port}/jwks", 0.15)()
        except Exception:
            pass
        assert time.monotonic() - start < 0.4
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)
