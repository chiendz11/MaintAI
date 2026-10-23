"""Linux host-network image smoke using fixture JWKS, never production credentials."""

import json
import os
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

pytestmark = pytest.mark.integration


def docker(*args, check=True):
    return subprocess.run(
        ["docker", *args], check=check, capture_output=True, text=True, timeout=45
    )


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_installed_image_jwt_nonroot_and_sigterm(payload):
    if os.environ.get("MAINTAI_INGESTION_CONTAINER_TEST") != "1":
        pytest.skip("Docker image smoke explicitly disabled")
    image = os.environ.get("MAINTAI_INGESTION_IMAGE", "maintai-ingestion:m1-local")
    bootstrap = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
    if not bootstrap.startswith(("127.0.0.1:", "localhost:")) or "," in bootstrap:
        pytest.fail("Only isolated local broker is allowed")
    docker("image", "inspect", image)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update(kid="smoke", alg="RS256", use="sig")
    body = json.dumps({"keys": [jwk]}).encode()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    issuer = f"http://127.0.0.1:{server.server_port}"
    port = free_port()
    name = "maintai-m1-smoke-" + uuid4().hex[:10]
    env = {
        "INGESTION_ENV": "test",
        "INGESTION_HOST": "127.0.0.1",
        "INGESTION_PORT": str(port),
        "AUTH_MODE": "oidc",
        "OIDC_ISSUER": issuer,
        "OIDC_JWKS_URL": issuer + "/jwks",
        "OIDC_ROLES_CLAIM": "roles",
        "OIDC_MACHINES_CLAIM": "machine_ids",
        "BUSINESS_PUBLIC_ORIGIN": "http://127.0.0.1:8003",
        "KAFKA_LOCAL_MODE": "true",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
        "INGESTION_PROBE_INTERVAL_SECONDS": ".2",
    }
    args = ["run", "-d", "--name", name, "--network", "host"]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    try:
        docker(*args, image)
        assert (
            docker("inspect", "--format", "{{.Config.User}}", name).stdout.strip() == "10001:10001"
        )
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=3, trust_env=False) as c:
            deadline = time.monotonic() + 30
            while True:
                try:
                    if c.get("/health/ready").status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                if time.monotonic() > deadline:
                    pytest.fail("Image did not become ready")
                time.sleep(0.2)
            assert c.post("/api/v1/sensor-readings", json=payload).status_code == 401
            token = jwt.encode(
                {
                    "iss": issuer,
                    "aud": "ingestion",
                    "sub": "smoke",
                    "exp": int(time.time()) + 60,
                    "roles": ["SENSOR_SERVICE"],
                    "machine_ids": ["M-001"],
                },
                key,
                algorithm="RS256",
                headers={"kid": "smoke"},
            )
            payload["event_id"] = "evt_image_" + uuid4().hex
            r = c.post(
                "/api/v1/sensor-readings",
                json=payload,
                headers={"Authorization": "Bearer " + token},
            )
            assert r.status_code == 202
            assert c.get("/health/live").json()["service"] == "ingestion-api"
        docker("stop", "--time", "20", name)
        assert docker("inspect", "--format", "{{.State.ExitCode}}", name).stdout.strip() == "0"
        logs = docker("logs", name).stdout
        assert token not in logs
    finally:
        docker("rm", "-f", name, check=False)
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_installed_image_fails_closed_without_config():
    if os.environ.get("MAINTAI_INGESTION_CONTAINER_TEST") != "1":
        pytest.skip("Docker image smoke explicitly disabled")
    image = os.environ.get("MAINTAI_INGESTION_IMAGE", "maintai-ingestion:m1-local")
    result = docker("run", "--rm", image, check=False)
    assert result.returncode == 2
    assert "Invalid ingestion configuration" in result.stderr
