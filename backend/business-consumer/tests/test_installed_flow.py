"""Installed Business/Read API/Consumer on two real DBs; ML/provenance/issuer are fixtures."""

import json
import os
import socket
import subprocess
import threading
import time
from contextlib import ExitStack
from datetime import UTC, datetime
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import jwt
import psycopg
import pytest
from backend.shared.persistence.migration import upgrade as business_upgrade
from backend.shared.persistence.permissions import grant_business_permissions
from backend.shared.persistence.registry import RegistryRepository
from cryptography.hazmat.primitives.asymmetric import rsa
from maintai_messaging.config import KafkaSettings
from maintai_messaging.ingestion import IngestionGateway, SourceIdentity
from maintai_messaging.postgres import OutboxRelay, connect
from maintai_messaging.producer import KafkaPublisher
from psycopg import sql
from sqlalchemy.engine import make_url

from .test_kafka_runtime import prepare_new_test_groups

ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.integration


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def docker(*args, timeout=45):
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def wait_ready(origin):
    deadline = time.monotonic() + 25
    with httpx.Client(trust_env=False, timeout=2) as http:
        while time.monotonic() < deadline:
            try:
                response = http.get(origin + "/health/ready")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
    pytest.fail("Installed runtime readiness did not become healthy")


def test_installed_status_live_consumer_two_db_roles_and_sigterm(business_database, tmp_path):
    if os.environ.get("MAINTAI_M3_CONTAINER_TEST") != "1":
        pytest.skip("M3 installed HTTP/Consumer flow requires explicit container opt-in")
    from maintai_messaging.consumer import Record
    from maintai_messaging.contracts import canonical_json, default_contracts
    from processing.shared.inference_client.client import PredictionAttempt
    from processing.shared.persistence.connection import upgrade as processing_upgrade
    from processing.worker.tests.test_worker_transaction import Providers, handler

    root_dsn = business_database.dsn
    suffix = uuid4().hex
    databases = {owner: "m3_" + owner + "_" + suffix for owner in ("business", "processing")}
    dsns = {
        owner: make_url(root_dsn).set(database=name).render_as_string(hide_password=False)
        for owner, name in databases.items()
    }
    roles = {role: "m3_" + role + "_" + suffix for role in ("api", "reader", "consumer")}
    password = uuid4().hex
    containers, exit_errors = [], []
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    document = {
        "keys": [
            {
                **json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())),
                "kid": "m3-test",
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

        def log_message(self, *args):
            pass

    keys = ThreadingHTTPServer(("127.0.0.1", 0), Keys)
    keys_thread = threading.Thread(target=keys.serve_forever, daemon=True)
    keys_thread.start()
    issuer = f"http://127.0.0.1:{keys.server_port}"

    def token(audience, role, machines=None):
        return jwt.encode(
            {
                "iss": issuer,
                "aud": audience,
                "sub": "m3-fixture",
                "roles": [role],
                "machines": machines or ["M-001"],
                "exp": int(time.time()) + 180,
            },
            key,
            algorithm="RS256",
            headers={"kid": "m3-test"},
        )

    def role_dsn(owner, role):
        return (
            make_url(dsns[owner])
            .set(username=roles[role], password=password)
            .render_as_string(hide_password=False)
        )

    def launch(role, image, env, mounts=()):
        env_file = tmp_path / (role + ".env")
        env_file.write_text("\n".join(f"{k}={v}" for k, v in env.items()) + "\n")
        env_file.chmod(0o600)
        name = "maintai-m3-" + role + "-" + suffix
        docker(
            "run",
            "--detach",
            "--name",
            name,
            "--network",
            "host",
            "--read-only",
            "--env-file",
            str(env_file),
            *mounts,
            image,
        )
        containers.append(name)
        assert docker("inspect", "--format", "{{.Config.User}}", name) == "10001:10001"

    try:
        with psycopg.connect(root_dsn, autocommit=True) as connection:
            for name in databases.values():
                connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            for name in roles.values():
                connection.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(name), sql.Literal(password)
                    )
                )
        business_upgrade(dsns["business"])
        processing_upgrade(dsns["processing"])
        business_factory = partial(connect, dsns["business"])
        processing_factory = partial(connect, dsns["processing"])
        RegistryRepository(business_factory).provision(
            [
                {"machine_id": "M-001", "display_name": "Integration fixture"},
            ]
        )
        with business_factory() as connection:
            grant_business_permissions(
                connection, reader_role=roles["api"], consumer_role=roles["consumer"]
            )
        with processing_factory() as connection:
            connection.execute(
                sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                    sql.Identifier(roles["reader"])
                )
            )
            connection.execute(
                sql.SQL("GRANT SELECT ON sensor_events,processing_results TO {}").format(
                    sql.Identifier(roles["reader"])
                )
            )
        with connect(role_dsn("processing", "reader")) as connection:
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute("DELETE FROM sensor_events")
        with connect(role_dsn("processing", "consumer")) as connection:
            with pytest.raises(psycopg.errors.InsufficientPrivilege), connection.transaction():
                connection.execute("SELECT * FROM sensor_events")

        kafka = KafkaSettings(
            bootstrap_servers=os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:39092"),
            local_mode=True,
            security_protocol="PLAINTEXT",
            group_prefix="m3-flow-" + suffix + "-",
        )
        prepare_new_test_groups(kafka)
        read_port, api_port, consumer_port = free_port(), free_port(), free_port()
        read_url, api_url = f"http://127.0.0.1:{read_port}", f"http://127.0.0.1:{api_port}"
        credential = tmp_path / "service-token"
        credential.write_text(token("processing-internal", "BUSINESS_SERVICE"))
        credential.chmod(0o444)
        launch(
            "read",
            "maintai-processing-read-api:m3-local",
            {
                "PROCESSING_READ_PROFILE": "local-test",
                "PROCESSING_READ_HOST": "127.0.0.1",
                "PROCESSING_READ_PORT": read_port,
                "PROCESSING_READ_DATABASE_URL": role_dsn("processing", "reader"),
                "PROCESSING_READ_OIDC_ISSUER": issuer,
                "PROCESSING_READ_OIDC_AUDIENCE": "processing-internal",
                "PROCESSING_READ_OIDC_JWKS_URL": issuer + "/jwks",
                "PROCESSING_READ_ROLES_CLAIM": "roles",
                "PROCESSING_READ_MACHINES_CLAIM": "machines",
            },
        )
        launch(
            "api",
            "maintai-business-registry:m2-local",
            {
                "BUSINESS_PROFILE": "local-test",
                "BUSINESS_HOST": "127.0.0.1",
                "BUSINESS_PORT": api_port,
                "BUSINESS_DATABASE_URL": role_dsn("business", "api"),
                "BUSINESS_OIDC_ISSUER": issuer,
                "BUSINESS_OIDC_AUDIENCE": "business-test",
                "BUSINESS_OIDC_JWKS_URL": issuer + "/jwks",
                "BUSINESS_ROLES_CLAIM": "roles",
                "BUSINESS_MACHINES_CLAIM": "machines",
                "BUSINESS_PROCESSING_READ_URL": read_url,
                "BUSINESS_PROCESSING_TOKEN_FILE": "/run/secrets/status-token",
            },
            ("-v", str(credential) + ":/run/secrets/status-token:ro"),
        )
        launch(
            "consumer",
            "maintai-business-consumer:m3-local",
            {
                "BUSINESS_CONSUMER_PROFILE": "local-test",
                "BUSINESS_CONSUMER_HOST": "127.0.0.1",
                "BUSINESS_CONSUMER_PORT": consumer_port,
                "BUSINESS_DATABASE_URL": role_dsn("business", "consumer"),
                "KAFKA_LOCAL_MODE": "true",
                "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
                "KAFKA_BOOTSTRAP_SERVERS": kafka.bootstrap_servers,
                "KAFKA_GROUP_PREFIX": kafka.group_prefix,
            },
        )
        for origin in (read_url, api_url, f"http://127.0.0.1:{consumer_port}"):
            wait_ready(origin)
        second_api = None
        if os.environ.get("MAINTAI_M4_CONTAINER_TEST") == "1":
            second_port = free_port()
            second_api = f"http://127.0.0.1:{second_port}"
            launch(
                "api2",
                "maintai-business-registry:m2-local",
                {
                    "BUSINESS_PROFILE": "local-test",
                    "BUSINESS_HOST": "127.0.0.1",
                    "BUSINESS_PORT": second_port,
                    "BUSINESS_DATABASE_URL": role_dsn("business", "api"),
                    "BUSINESS_OIDC_ISSUER": issuer,
                    "BUSINESS_OIDC_AUDIENCE": "business-test",
                    "BUSINESS_OIDC_JWKS_URL": issuer + "/jwks",
                    "BUSINESS_ROLES_CLAIM": "roles",
                    "BUSINESS_MACHINES_CLAIM": "machines",
                },
            )
            wait_ready(second_api)

        class Capture:
            def __init__(self, publisher):
                self.publisher, self.settings = publisher, publisher.settings

            def publish(self, topic, payload):
                delivery = self.publisher.publish(topic, payload)
                self.event = default_contracts().decode(topic, canonical_json(payload))
                self.record = Record(
                    topic,
                    delivery.partition,
                    delivery.offset,
                    self.event.key.encode(),
                    self.event.wire_bytes,
                )

        class CurrentProviders(Providers):
            def predict(self, **kwargs):
                attempt = super().predict(**kwargs)
                response = attempt.response.model_copy(update={"predicted_at": datetime.now(UTC)})
                return PredictionAttempt(response, None, 1)

        with KafkaPublisher(kafka, "ingestion") as publisher:
            capture = Capture(publisher)
            gateway = IngestionGateway(capture, api_url)
            request = json.loads(
                (ROOT / "contracts/kafka/examples/sensor-request.json").read_text()
            )
            request.update(event_id="m3/fixture " + suffix, sequence_number=1)
            ack = gateway.handle(
                canonical_json(request),
                identity=SourceIdentity("sensor-fixture", frozenset({"M-001"})),
                request_id="req-m3",
                correlation_id="cor-m3",
            )
        assert ack.status == 202
        headers = {
            "Authorization": "Bearer " + token("business-test", "SENSOR_SERVICE"),
            "X-Correlation-ID": "cor-m3",
        }
        with httpx.Client(trust_env=False, timeout=5) as http, ExitStack() as streams:
            stream_responses = []
            if second_api is not None:
                for origin in (api_url, second_api):
                    response = streams.enter_context(
                        http.stream(
                            "GET",
                            origin + "/api/v1/events/stream?machine_id=M-001",
                            headers={"Authorization": "Bearer " + token("business-test", "VIEWER")},
                        )
                    )
                    assert response.status_code == 200
                    stream_responses.append(response)
            initial = http.get(ack.body["status_url"], headers=headers)
            assert initial.status_code == 404
            assert initial.json()["error"]["retryable"] is True
            database = SimpleNamespace(factory=processing_factory)
            handler(database, CurrentProviders())(capture.event, capture.record)
            completed = http.get(ack.body["status_url"], headers=headers)
            assert completed.status_code == 200
            assert completed.json()["processing_status"] == "COMPLETED"
            assert completed.headers["X-Correlation-ID"] == "cor-m3"
            with KafkaPublisher(kafka, "outbox") as publisher:
                published = OutboxRelay(processing_factory, publisher).run_once()
            # HIGH fixture emits both its canonical outcome and an Alert Signal.
            # M3 Business consumes only the outcome; Alert lifecycle remains separate.
            assert published == 2
            for stream in stream_responses:
                for line in stream.iter_lines():
                    if line.startswith("data: "):
                        message = json.loads(line[6:])
                        if message["type"] == "prediction.completed":
                            assert message["data"]["source_event_id"] == request["event_id"]
                            break
                else:
                    pytest.fail("Installed API replica did not receive committed SSE prediction")
            with processing_factory() as connection:
                rows = connection.execute(
                    "SELECT topic,status FROM maintai_kafka_outbox"
                ).fetchall()
                assert {(r["topic"], r["status"]) for r in rows} == {
                    ("inference.results", "PUBLISHED"),
                    ("alert.signals", "PUBLISHED"),
                }
            viewer = {"Authorization": "Bearer " + token("business-test", "VIEWER")}
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                live = http.get(api_url + "/api/v1/machines/M-001/live", headers=viewer)
                assert live.status_code == 200
                value = live.json()
                if value["latest_valid_prediction"] is not None:
                    break
                time.sleep(0.1)
            else:
                pytest.fail("Installed Consumer did not expose its projection through Business API")
            assert value["latest_processing"]["processing_status"] == "COMPLETED"
            assert value["latest_valid_prediction"]["source_event_id"] == request["event_id"]
            assert value["telemetry"]["event_id"] == request["event_id"]
            # Alert storage belongs to a later phase: never fake zero active alerts.
            for resource in ("", "/M-001"):
                unavailable = http.get(api_url + "/api/v1/machines" + resource, headers=viewer)
                assert unavailable.status_code == 503
                assert unavailable.json()["error"]["code"] == "DEPENDENCY_UNAVAILABLE"
            assert (
                http.get(api_url + "/api/v1/machines/M-001/live", headers=headers).status_code
                == 403
            )
    finally:
        for name in reversed(containers):
            subprocess.run(
                ["docker", "stop", "--time", "35", name], capture_output=True, timeout=45
            )
            result = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.ExitCode}}", name],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0 or result.stdout.strip() != "0":
                exit_errors.append(name)
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)
        keys.shutdown()
        keys.server_close()
        keys_thread.join(timeout=5)
        with psycopg.connect(root_dsn, autocommit=True) as connection:
            for name in databases.values():
                connection.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
                )
            for name in roles.values():
                connection.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))
    assert exit_errors == []
