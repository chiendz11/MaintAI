"""Nghiệm thu local: HTTPS/JWKS container -> authenticated HGB container."""

import argparse
import hashlib
import json
import math
import ssl
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


ROOT = Path(__file__).resolve().parents[3]

SERVER = r'''
import json
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path != "/keys":
            self.send_error(404)
            return
        body = Path("/fixture/keys.json").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

server = ThreadingHTTPServer(("0.0.0.0", 8443), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/fixture/server.pem", "/fixture/server-key.pem")
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
'''


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def docker(*args, input_text=None, timeout=90):
    result = subprocess.run(
        ["docker", *args],
        input=input_text,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode:
        # Không truyền token hoặc private key qua đối số docker.
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def write_certificate_fixture(server_dir, trust_dir):
    now = datetime.now(timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "MaintAI integration test CA")
    ])

    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=0), critical=True
        )
        .sign(ca_key, hashes.SHA256())
    )

    server_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "jwks")
        ]))
        .issuer_name(ca.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("jwks")]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    (trust_dir / "ca.pem").write_bytes(
        ca.public_bytes(serialization.Encoding.PEM)
    )
    (server_dir / "server.pem").write_bytes(
        certificate.public_bytes(serialization.Encoding.PEM)
    )
    (server_dir / "server-key.pem").write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )


def publish_key(directory, key, kid):
    public = json.loads(
        jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())
    )
    public.update({"kid": kid, "alg": "RS256", "use": "sig"})
    temporary = directory / "keys.next"
    temporary.write_text(
        json.dumps({"keys": [public]}), encoding="utf-8"
    )
    temporary.replace(directory / "keys.json")


def token(key, kid, role="PROCESSING_SERVICE", expired=False):
    now = int(time.time())
    return jwt.encode(
        {
            "iss": "https://maintai-fixture.invalid",
            "aud": "maintai-ml-fixture",
            "sub": "processing-integration-fixture",
            "exp": now - 60 if expired else now + 1800,
            "roles": [role],
        },
        key,
        algorithm="RS256",
        headers={"kid": kid},
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument(
        "--image", default="maintai-ml:hgb-auth-candidate-v1"
    )
    args = parser.parse_args()

    bundle = args.bundle.resolve()
    manifest_path = bundle / "bundle-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    metadata = json.loads(
        (bundle / "metadata.json").read_text(encoding="utf-8")
    )

    # Kiểm tra toàn bộ file mà manifest gốc quản lý.
    for relative, expected in manifest["files"].items():
        path = (bundle / relative).resolve()
        if not path.is_relative_to(bundle):
            raise ValueError("Bundle manifest path escapes bundle")
        if sha256(path) != expected:
            raise ValueError(f"Bundle checksum mismatch: {relative}")

    image = json.loads(docker("image", "inspect", args.image))[0]
    command = image["Config"].get("Cmd") or []
    if "authenticated_local:create_app" not in command:
        raise ValueError("Image default command does not enable auth")

    # Dùng immutable local image ID trong toàn bộ lượt chạy.
    # Đây là config ID, không gọi nó là registry OCI digest.
    image_id = image["Id"]

    suffix = uuid.uuid4().hex[:12]
    network = f"maintai-auth-check-{suffix}"
    jwks_name = f"maintai-jwks-{suffix}"
    ml_name = f"maintai-ml-auth-{suffix}"

    output = ROOT / "ml-service/artifacts/integration_checks" / (
        "authenticated-container-" + suffix
    )
    output.mkdir(parents=True, exist_ok=False)

    report = {
        "scope": "local_two_container_auth_and_hgb_integration",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "image_reference": args.image,
        "docker_image_config_id": image_id,
        "bundle_manifest_sha256": sha256(manifest_path),
        "model_version": metadata["model_version"],
        "threshold": metadata["threshold"],
        "checks": {},
        "result": "FAIL",
        "approved_for_release": False,
        "test_set_used": False,
        "limitations": [
            "Fixture issuer and CA; not production IdP.",
            "API HTTP is published on host loopback only.",
            "JWKS outage after ML restart tests cold-cache failure.",
            "This does not replace a cache-expiry integration test.",
            "Processing Worker is not part of this test.",
        ],
    }

    def check(name, passed):
        report["checks"][name] = bool(passed)
        print(f"{name}: {'PASS' if passed else 'FAIL'}", flush=True)
        if not passed:
            raise AssertionError(name)

    created_containers = []
    network_created = False

    try:
        with tempfile.TemporaryDirectory(prefix="maintai-auth-fixture-") as tmp:
            fixture = Path(tmp)
            server_dir = fixture / "server"
            trust_dir = fixture / "trust"
            server_dir.mkdir()
            trust_dir.mkdir()
            write_certificate_fixture(server_dir, trust_dir)
            (server_dir / "server.py").write_text(SERVER, encoding="utf-8")

            old_key = rsa.generate_private_key(
                public_exponent=65537, key_size=2048
            )
            new_key = rsa.generate_private_key(
                public_exponent=65537, key_size=2048
            )
            publish_key(server_dir, old_key, "old")

            try:
                docker("network", "create", "--driver", "bridge", network)
                network_created = True

                docker(
                    "run", "-d", "--name", jwks_name,
                    "--network", network, "--network-alias", "jwks",
                    "--read-only",
                    "--mount",
                    f"type=bind,source={server_dir},target=/fixture,readonly",
                    "--entrypoint", "python",
                    image_id, "/fixture/server.py",
                )
                created_containers.append(jwks_name)

                docker(
                    "run", "-d", "--name", ml_name,
                    "--network", network,
                    "--read-only",
                    "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m",
                    "-p", "127.0.0.1::8001",
                    "-e", "MAINTAI_LOCAL_ONLY=1",
                    "-e", "MAINTAI_AUTH_ISSUER=https://maintai-fixture.invalid",
                    "-e", "MAINTAI_AUTH_AUDIENCE=maintai-ml-fixture",
                    "-e", "MAINTAI_AUTH_ROLES_CLAIM=roles",
                    "-e", "MAINTAI_AUTH_JWKS_URL=https://jwks:8443/keys",
                    "-e", "MAINTAI_AUTH_CA_FILE=/trust/ca.pem",
                    "--mount",
                    f"type=bind,source={bundle},target=/models/current,readonly",
                    "--mount",
                    f"type=bind,source={trust_dir},target=/trust,readonly",
                    image_id,
                )
                created_containers.append(ml_name)

                def save_diagnostics():
                    diagnostics = {}
                    for name in (jwks_name, ml_name):
                        item = {}
                        try:
                            inspected = json.loads(
                                docker("inspect", name)
                            )[0]
                            item["state"] = inspected.get("State", {})
                            item["ports"] = inspected.get(
                                "NetworkSettings", {}
                            ).get("Ports", {})
                            item["port_bindings"] = inspected.get(
                                "HostConfig", {}
                            ).get("PortBindings", {})
                        except Exception as exc:
                            item["inspect_error"] = str(exc)

                        try:
                            result = subprocess.run(
                                ["docker", "logs", "--tail", "100", name],
                                capture_output=True,
                                text=True,
                                timeout=15,
                            )
                            item["logs"] = result.stdout + result.stderr
                        except Exception as exc:
                            item["logs_error"] = str(exc)

                        diagnostics[name] = item

                    path = output / "container-diagnostics.json"
                    path.write_text(
                        json.dumps(diagnostics, indent=2) + "\n",
                        encoding="utf-8",
                    )
                    print("Container diagnostics:", path.resolve())

                    for name, item in diagnostics.items():
                        print("\nContainer:", name)
                        print("State:", item.get("state", {}).get("Status"))
                        print("Ports:", item.get("ports"))
                        print(item.get("logs", ""))

                def origin():
                    inspected = json.loads(docker("inspect", ml_name))[0]
                    state = inspected.get("State", {})
                    status = state.get("Status")

                    if status in ("exited", "dead", "removing"):
                        raise RuntimeError(
                            f"ML container is {status}; "
                            f"exit_code={state.get('ExitCode')}"
                        )

                    if not state.get("Running"):
                        raise ConnectionError(
                            f"ML container is not running yet: {status}"
                        )

                    ports = (
                        inspected.get("NetworkSettings", {}).get("Ports")
                        or {}
                    )
                    bindings = ports.get("8001/tcp") or []

                    matches = [
                        binding
                        for binding in bindings
                        if binding.get("HostIp") == "127.0.0.1"
                        and binding.get("HostPort")
                    ]

                    if not bindings:
                        raise ConnectionError(
                            "Docker has not exposed a host binding for 8001/tcp"
                        )

                    if len(matches) != 1:
                        raise RuntimeError(
                            f"Unexpected API port bindings: {bindings}"
                        )

                    return (
                        "http://127.0.0.1:"
                        + str(matches[0]["HostPort"])
                    )

                with httpx.Client(
                    timeout=5, trust_env=False, follow_redirects=False
                ) as client:
                    def wait_status(path, expected, seconds=45):
                        deadline = time.monotonic() + seconds
                        last_observation = "No response"

                        while time.monotonic() < deadline:
                            try:
                                response = client.get(origin() + path)
                                last_observation = (
                                    f"HTTP {response.status_code}: "
                                    f"{response.text[:500]}"
                                )
                                if response.status_code == expected:
                                    return response

                            except (httpx.TransportError, ConnectionError) as exc:
                                last_observation = (
                                    f"{type(exc).__name__}: {exc}"
                                )

                            except Exception:
                                save_diagnostics()
                                raise

                            time.sleep(0.5)

                        save_diagnostics()
                        raise RuntimeError(
                            f"Timeout waiting for {path} -> {expected}. "
                            f"Last observation: {last_observation}"
                        )

                    ready = wait_status("/health/ready", 200)
                    check("startup_ready", ready.json()["status"] == "UP")

                    body = json.loads((
                        ROOT / "contracts/v1.3/examples/http/ml/predict-request.json"
                    ).read_text(encoding="utf-8"))

                    def predict(bearer=None, *, malformed=False):
                        headers = {
                            "X-Request-ID": "req-auth-container",
                            "X-Correlation-ID": "cor-auth-container",
                        }
                        if bearer is not None:
                            headers["Authorization"] = "Bearer " + bearer
                        if malformed:
                            headers["Content-Type"] = "application/json"
                            return client.post(
                                origin() + "/api/v1/predict",
                                content="{broken", headers=headers,
                            )
                        return client.post(
                            origin() + "/api/v1/predict",
                            json=body, headers=headers,
                        )

                    def blocked(response, status, code):
                        value = response.json()
                        return (
                            response.status_code == status
                            and value.get("error", {}).get("code") == code
                            and "prediction" not in value
                            and "failure_probability" not in value
                        )

                    check(
                        "missing_token_before_body_validation",
                        blocked(predict(malformed=True), 401, "UNAUTHORIZED"),
                    )
                    check(
                        "wrong_role",
                        blocked(
                            predict(token(old_key, "old", role="BUSINESS_SERVICE")),
                            403, "FORBIDDEN",
                        ),
                    )
                    check(
                        "expired_token",
                        blocked(
                            predict(token(old_key, "old", expired=True)),
                            401, "UNAUTHORIZED",
                        ),
                    )

                    valid = predict(token(old_key, "old"))
                    check("valid_token_http_200", valid.status_code == 200)
                    prediction = valid.json()

                    # Score offline của chính pipeline trong cùng image.
                    offline_script = (
                        "import joblib, pandas as pd\n"
                        "p = joblib.load('/models/current/pipeline.joblib')\n"
                        f"features = {body['features']!r}\n"
                        "frame = pd.DataFrame([features], "
                        "columns=list(p.feature_names_in_))\n"
                        "print(float(p.predict_proba(frame)[0, 1]))\n"
                    )
                    offline = float(docker(
                        "exec", "-i", ml_name, "python", "-",
                        input_text=offline_script,
                    ))

                    check(
                        "api_score_matches_offline",
                        math.isclose(
                            prediction["failure_probability"], offline,
                            rel_tol=1e-10, abs_tol=1e-10,
                        ),
                    )
                    check(
                        "model_threshold_and_identity",
                        prediction["model_version"] == metadata["model_version"]
                        and prediction["preprocessor_version"]
                        == metadata["preprocessor_version"]
                        and prediction["model_threshold"] == metadata["threshold"]
                        and prediction["event_id"] == body["event_id"]
                        and prediction["prediction"] == (
                            "FAILURE" if offline >= metadata["threshold"]
                            else "NORMAL"
                        ),
                    )
                    check(
                        "trace_headers",
                        valid.headers.get("X-Request-ID") == "req-auth-container"
                        and valid.headers.get("X-Correlation-ID")
                        == "cor-auth-container",
                    )

                    # Xoay khóa qua JWKS endpoint thật.
                    publish_key(server_dir, new_key, "new")
                    time.sleep(5.2)
                    check(
                        "new_signing_key_accepted",
                        predict(token(new_key, "new")).status_code == 200,
                    )
                    check(
                        "removed_signing_key_rejected",
                        blocked(
                            predict(token(old_key, "old")), 401, "UNAUTHORIZED"
                        ),
                    )

                    docker("stop", "-t", "2", jwks_name)
                    check(
                        "valid_cached_key_during_short_outage",
                        predict(token(new_key, "new")).status_code == 200,
                    )

                    # Restart ML để kiểm tra không có cache và JWKS đang down.
                    docker("restart", "-t", "2", ml_name)
                    wait_status("/health/live", 200)
                    unavailable = wait_status("/health/ready", 503)
                    check(
                        "cold_cache_outage_readiness",
                        unavailable.json()["checks"].get("auth_ready") is False,
                    )
                    check(
                        "cold_cache_outage_blocks_prediction",
                        blocked(
                            predict(token(new_key, "new")), 500, "INTERNAL_ERROR"
                        ),
                    )

                    docker("start", jwks_name)
                    wait_status("/health/ready", 200)
                    check(
                        "recovery_without_model_or_threshold_change",
                        predict(token(new_key, "new")).status_code == 200,
                    )

                    report["verified_offline_score"] = offline
                    report["result"] = "PASS"

            finally:
                # Chỉ dọn các resource do chính lượt kiểm thử này tạo.
                for name in reversed(created_containers):
                    result = subprocess.run(
                        ["docker", "rm", "-f", name],
                        capture_output=True, text=True, timeout=30,
                    )
                    if result.returncode:
                        report.setdefault("cleanup_errors", []).append(name)

                if network_created:
                    result = subprocess.run(
                        ["docker", "network", "rm", network],
                        capture_output=True, text=True, timeout=30,
                    )
                    if result.returncode:
                        report.setdefault("cleanup_errors", []).append(network)

    finally:
        report_path = output / "result.json"
        report_path.write_text(
            json.dumps(report, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        print("Evidence:", report_path.resolve())

    print("Authenticated HGB container integration:", report["result"])
    print("Production IdP / Processing Worker evaluated: NO")
    print("Approved for release: NO")


if __name__ == "__main__":
    main()