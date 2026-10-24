"""TLS OIDC/JWKS fixture with real RSA signatures and client-credentials HTTP.

This verifies our provider/client integration; it is not a production IdP deployment.
"""

import base64
import ipaddress
import json
import ssl
import threading
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import jwt
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi.testclient import TestClient

from simulator.client import BackendClient
from simulator.control.api import create_app
from simulator.control.auth import IngestionCredentials
from simulator.scheduling.engine import SensorEngine
from simulator.settings import RuntimeSettings


@pytest.fixture
def oidc(tmp_path, monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    ca, pem = tmp_path / "ca.pem", tmp_path / "key.pem"
    ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    pem.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update(kid="fixture-key", use="sig", alg="RS256")
    token_calls = []
    issuer = ""

    def issue(**overrides):
        return jwt.encode(
            {
                "iss": issuer,
                "aud": "simulator-control",
                "sub": "alice",
                "iat": int(time.time()),
                "exp": int(time.time()) + 120,
                "scope": "simulator:control",
                "roles": ["SIMULATOR_OPERATOR"],
                "machine_ids": ["M-001"],
                **overrides,
            },
            key,
            algorithm="RS256",
            headers={"kid": "fixture-key"},
        )

    class Handler(BaseHTTPRequestHandler):
        def respond(self, status, value):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/jwks":
                self.respond(200, {"keys": [jwk]})
            else:
                self.respond(
                    200,
                    {
                        "issuer": issuer,
                        "jwks_uri": issuer + "/jwks",
                        "token_endpoint": issuer + "/token",
                    },
                )

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            token_calls.append(body)
            if (
                self.headers.get("Authorization")
                != "Basic " + base64.b64encode(b"sensor:fixture-secret").decode()
            ):
                return self.respond(401, {"error": "invalid_client"})
            self.respond(
                200,
                {
                    "access_token": issue(sub="sensor", aud="ingestion", scope="sensor:write"),
                    "token_type": "Bearer",
                    "expires_in": 300,
                },
            )

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(ca, pem)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    issuer = f"https://127.0.0.1:{server.server_port}"
    monkeypatch.setenv("SSL_CERT_FILE", str(ca))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield issuer, issue, key, token_calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def test_real_tls_jwks_signed_tokens_scope_and_expiry(runtime_settings, oidc, tmp_path):
    issuer, issue, _, _ = oidc
    token_file = tmp_path / "ingestion.token"
    token_file.write_text("fixture-service-token")
    settings = RuntimeSettings.model_validate(
        {
            **runtime_settings.model_dump(),
            "auth_mode": "jwt",
            "jwt_issuer": issuer,
            "jwks_url": issuer + "/jwks",
            "ingestion_auth": "token_file",
            "ingestion_token_file": token_file,
            "backend_base_url": "https://ingestion.example.invalid",
        }
    )
    engine = SensorEngine(settings)
    with TestClient(create_app(settings, engine=engine, background=False)) as client:
        base = "/api/v1/simulator/machines"
        assert client.get(base).status_code == 401
        headers = {"Authorization": "Bearer " + issue(), "Idempotency-Key": "signed-command"}
        result = client.get(base, headers=headers)
        assert result.status_code == 200
        assert [m["machine_id"] for m in result.json()["items"]] == ["M-001"]
        result = client.post(
            base + "/M-001/pause", headers=headers, json={"expected_control_version": 0}
        )
        assert result.status_code == 200
        assert result.json()["last_controlled_by"] == "alice"
        assert client.get(base + "/M-002/control", headers=headers).status_code == 403
        for override, status in [
            ({"exp": 1}, 401),
            ({"aud": "business"}, 401),
            ({"roles": ["VIEWER"]}, 403),
        ]:
            assert (
                client.get(
                    base, headers={"Authorization": "Bearer " + issue(**override)}
                ).status_code
                == status
            )
        assert client.get(base, headers={"Authorization": "Bearer invalid"}).status_code == 401


def test_real_oauth_client_credentials_are_cached_and_used_for_ingestion(
    runtime_settings, oidc, tmp_path
):
    issuer, _, key, calls = oidc
    secret = tmp_path / "secret"
    secret.write_text("fixture-secret")
    settings = RuntimeSettings.model_validate(
        {
            **runtime_settings.model_dump(),
            "auth_mode": "jwt",
            "jwt_issuer": issuer,
            "jwks_url": issuer + "/jwks",
            "ingestion_auth": "oauth",
            "oauth_token_url": issuer + "/token",
            "oauth_client_id": "sensor",
            "oauth_secret_file": secret,
            "backend_base_url": "https://ingestion.example.invalid",
        }
    )
    payload = {"event_id": "evt_oauth", "machine_id": "M-001"}

    def accept(request):
        claims = jwt.decode(
            request.headers["Authorization"][7:],
            key.public_key(),
            algorithms=["RS256"],
            issuer=issuer,
            audience="ingestion",
        )
        assert claims["scope"] == "sensor:write"
        return httpx.Response(
            202,
            json={
                "event_id": "evt_oauth",
                "machine_id": "M-001",
                "ingestion_status": "ACCEPTED",
                "processing_status": "PENDING",
                "server_accepted_at": "2026-10-20T00:00:00Z",
                "status_url": "https://business.example.invalid/status",
                "correlation_id": request.headers["X-Correlation-ID"],
            },
        )

    credentials = IngestionCredentials(settings)
    with BackendClient(
        str(settings.backend_base_url),
        credentials=credentials,
        transport=httpx.MockTransport(accept),
    ) as client:
        assert client.send(payload).outcome == "accepted"
        assert client.send(payload).outcome == "accepted"
    assert len(calls) == 1
    assert b"grant_type=client_credentials" in calls[0]
