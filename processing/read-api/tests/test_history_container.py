"""Installed Read API enables M6 using actual distinct SQL roles and signed JWTs."""

import base64
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
from processing.shared.history_meta.persistence import grant_metadata
from psycopg import sql
from sqlalchemy.engine import make_url

from .test_history_snapshots import QUERY, insert

pytestmark = pytest.mark.integration


def test_installed_read_api_m6_roles_delegation_and_csv(pipeline_database, tmp_path):
    if (
        os.environ.get("MAINTAI_M3_CONTAINER_TEST") != "1"
        or os.environ.get("MAINTAI_M6_INTEGRATION") != "1"
    ):
        pytest.skip("Enable approved M3 containers and M6 integration")
    db = pipeline_database
    insert(db, "installed-m6", 1)
    roles = ["m6_ro_" + uuid4().hex, "m6_meta_" + uuid4().hex]
    password, name = uuid4().hex, "maintai-m6-read-" + uuid4().hex
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    document = {
        "keys": [
            {
                **jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key(), as_dict=True),
                "kid": "fixture",
                "alg": "RS256",
                "use": "sig",
            }
        ]
    }

    class Keys(BaseHTTPRequestHandler):
        def do_GET(self):
            raw = json.dumps(document).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_args):
            pass

    issuer_server = ThreadingHTTPServer(("127.0.0.1", 0), Keys)
    issuer = f"http://127.0.0.1:{issuer_server.server_port}"
    thread = threading.Thread(target=issuer_server.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    cursor = tmp_path / "cursor.json"
    cursor.write_text(
        json.dumps(
            {"active": "fixture", "keys": {"fixture": base64.b64encode(os.urandom(32)).decode()}}
        )
    )
    tmp_path.chmod(0o755)
    cursor.chmod(0o644)
    try:
        with db.factory() as connection:
            for role in roles:
                connection.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(role), sql.Literal(password)
                    )
                )
            connection.execute(
                sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(roles[0]))
            )
            for table in ("sensor_events", "predictions", "risk_decisions", "processing_results"):
                connection.execute(
                    sql.SQL("GRANT SELECT ON {} TO {}").format(
                        sql.Identifier(table), sql.Identifier(roles[0])
                    )
                )
            grant_metadata(connection, role=roles[1])
        urls = [
            make_url(db.dsn)
            .set(username=role, password=password)
            .render_as_string(hide_password=False)
            for role in roles
        ]
        env = {
            "PROCESSING_READ_DATABASE_URL": urls[0],
            "PROCESSING_READ_PROFILE": "local-test",
            "PROCESSING_READ_OIDC_ISSUER": issuer,
            "PROCESSING_READ_OIDC_AUDIENCE": "processing-internal",
            "PROCESSING_READ_OIDC_JWKS_URL": issuer + "/keys",
            "PROCESSING_READ_ROLES_CLAIM": "roles",
            "PROCESSING_READ_MACHINES_CLAIM": "machines",
            "PROCESSING_READ_HOST": "127.0.0.1",
            "PROCESSING_READ_PORT": str(port),
            "PROCESSING_HISTORY_ENABLED": "true",
            "PROCESSING_HISTORY_METADATA_DATABASE_URL": urls[1],
            "PROCESSING_HISTORY_DELEGATION_ISSUER": issuer + "/business",
            "PROCESSING_HISTORY_DELEGATION_JWKS_URL": issuer + "/keys",
            "PROCESSING_HISTORY_CURSOR_KEY_FILE": "/fixture/cursor.json",
        }
        args = [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--network",
            "host",
            "--read-only",
            "--tmpfs",
            "/tmp",
            "--mount",
            f"type=bind,source={cursor},target=/fixture/cursor.json,readonly",
        ]
        for field, value in env.items():
            args.extend(["-e", field + "=" + value])
        args.append("maintai-processing-read-api:m3-local")
        started = subprocess.run(args, capture_output=True, text=True, timeout=30)
        assert started.returncode == 0, "Installed M6 container did not start"
        origin = f"http://127.0.0.1:{port}"
        with httpx.Client(trust_env=False, timeout=2) as client:
            for _ in range(100):
                try:
                    if client.get(origin + "/health/ready").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.1)
            else:
                pytest.fail("Installed M6 readiness failed")
            now = int(time.time())
            common = {"iat": now, "nbf": now, "exp": now + 60, "machines": ["M-001"]}

            def sign(claims):
                return jwt.encode(
                    {**common, **claims}, key, algorithm="RS256", headers={"kid": "fixture"}
                )

            headers = {
                "Authorization": "Bearer "
                + sign(
                    {
                        "iss": issuer,
                        "aud": "processing-internal",
                        "sub": "service",
                        "roles": ["BUSINESS_SERVICE"],
                    }
                ),
                "X-MaintAI-History-Profile": "M6-HISTORY-01/1.0.0",
                "X-MaintAI-History-Delegation": sign(
                    {
                        "iss": issuer + "/business",
                        "aud": "processing-history",
                        "sub": "tech",
                        "azp": "service",
                        "jti": uuid4().hex,
                        "roles": ["TECHNICIAN"],
                    }
                ),
            }
            result = client.get(
                origin + "/api/v1/internal/machines/M-001/predictions",
                headers=headers,
                params=QUERY,
            )
            assert result.status_code == 200
            assert result.json()["items"][0]["source_event_id"] == "installed-m6"
            response = client.post(
                origin + "/api/v1/internal/history-exports",
                headers={**headers, "Idempotency-Key": "installed-csv"},
                json={
                    "profile": "M6-HISTORY-01/1.0.0",
                    "machine_ids": ["M-001"],
                    **QUERY,
                    "format": "CSV",
                    "resource": "PROCESSING",
                },
            )
            assert response.status_code == 200 and "installed-m6" in response.text
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)
        issuer_server.shutdown()
        issuer_server.server_close()
        thread.join(timeout=3)
        with db.factory() as connection:
            for role in roles:
                if connection.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)
                ).fetchone():
                    connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
                    connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
