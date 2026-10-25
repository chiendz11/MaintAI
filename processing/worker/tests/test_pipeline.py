"""M1 HTTP + real Kafka/Worker/DB/Relay; registry and ML are HTTP fixtures."""

import asyncio
import json
import socket
import threading
import time
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from consumer.config import WorkerSettings
from consumer.main import run_worker
from maintai_messaging.admin import create_local_topics
from maintai_messaging.config import KafkaSettings
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.producer import KafkaPublisher
from relay.main import create_relay

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def source_boundary(counter, *, timeout=15):
    """Capture fixture offsets before publishing, allowing bounded leader startup."""
    deadline = time.monotonic() + timeout
    transient = {
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._TIMED_OUT,
        KafkaError.REQUEST_TIMED_OUT,
    }

    def remaining():
        return min(5, max(0, deadline - time.monotonic()))

    while True:
        try:
            # Refresh metadata after leader changes; discard any partial boundary.
            metadata = counter.list_topics("sensor.readings", timeout=remaining())
            return [
                TopicPartition(
                    "sensor.readings",
                    partition,
                    counter.get_watermark_offsets(
                        TopicPartition("sensor.readings", partition), timeout=remaining()
                    )[1],
                )
                for partition in metadata.topics["sensor.readings"].partitions
            ]
        except KafkaException as error:
            if error.args[0].code() not in transient or time.monotonic() >= deadline:
                raise
            time.sleep(min(0.1, remaining()))


@pytest.mark.parametrize("stop_during_inference", [False, True])
def test_http_to_terminal_outcomes_and_alert_signal_with_real_broker(
    pipeline_database, tmp_path, stop_during_inference, request
):
    from maintai_ingestion.app import create_app
    from maintai_ingestion.config import AppSettings

    contracts = default_contracts()
    prefix = "m2-pipeline-" + uuid4().hex + "-"
    kafka = KafkaSettings(
        local_mode=True,
        security_protocol="PLAINTEXT",
        bootstrap_servers="127.0.0.1:39092",
        group_prefix=prefix,
        delivery_timeout_ms=3000,
        producer_git_sha="1" * 40,
    )
    create_local_topics(kafka)
    detail = json.loads(
        (ROOT / "contracts/v1.3/examples/http/backend/machine-detail.json").read_text()
    )
    identity = json.loads((ROOT / "contracts/kafka/examples/completed.json").read_text())
    ml_calls = []

    class FixtureHTTP(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send_json(self, status, payload):
            raw = canonical_json(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            assert self.path == "/api/v1/machines/M-001"
            self.send_json(200, detail)

        def do_POST(self):
            assert self.path == "/api/v1/predict"
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            ml_calls.append(body["event_id"])
            if body["features"]["torque_nm"] == 999:
                if stop_during_inference and body["event_id"] in expected:
                    # The CLI signals set this same Event. Stop admission while
                    # the current HTTP retry/transaction must still finish.
                    stop.set()
                self.send_json(
                    503,
                    {
                        "error": {
                            "code": "MODEL_NOT_READY",
                            "message": "fixture",
                            "retryable": True,
                            "details": {},
                        }
                    },
                )
                return
            self.send_json(
                200,
                {
                    "prediction_id": "pred-" + body["event_id"],
                    "event_id": body["event_id"],
                    "prediction": "FAILURE",
                    "failure_probability": 0.8,
                    "model_threshold": 0.5,
                    "model_version": identity["model_version"],
                    "preprocessor_version": identity["preprocessor_version"],
                    "inference_ms": 1,
                    "predicted_at": "2026-10-24T00:00:00Z",
                },
            )

    server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHTTP)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    policy = tmp_path / "fixture-policy.json"
    policy.write_bytes(
        canonical_json(
            {
                "policy_version": "fixture-policy",
                "thresholds": {"watch": 0.4, "high": 0.65, "critical": 0.85},
                "create_alert_for": ["HIGH", "CRITICAL"],
            }
        )
    )
    origin = f"http://127.0.0.1:{server.server_port}"
    env = {
        "PROCESSING_PROFILE": "local-fixture",
        "KAFKA_LOCAL_MODE": "true",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_BOOTSTRAP_SERVERS": kafka.bootstrap_servers,
        "KAFKA_GROUP_PREFIX": prefix,
        "PRODUCER_GIT_SHA": "1" * 40,
        "PROCESSING_DATABASE_URL": pipeline_database.dsn,
        "BUSINESS_BASE_URL": origin,
        "ML_BASE_URL": origin,
        "PROCESSING_SCHEMA_DIRECTORY": str(ROOT / "contracts/v1.3/schemas"),
        "PROCESSING_RISK_POLICY_FILE": str(policy),
        "PROCESSING_MODEL_IDENTITY_FIXTURE": str(ROOT / "contracts/kafka/examples/completed.json"),
        "PROCESSING_HEALTH_PORT": str(free_port()),
    }
    if stop_during_inference:
        registry_service = request.getfixturevalue("business_registry_service")
        env["BUSINESS_BASE_URL"] = registry_service.origin
        env["BUSINESS_BEARER_TOKEN"] = registry_service.token
        env["PROCESSING_EXECUTION_MODE"] = "keyed"
        from maintai_messaging.ordering import activate_processing_ordering

        with pipeline_database.factory() as connection:
            activate_processing_ordering(connection, legacy_writers_drained=True)
    settings = WorkerSettings.from_env(env)
    sample = json.loads((ROOT / "contracts/kafka/examples/sensor-request.json").read_text())
    expected = {}
    stop = threading.Event()
    try:
        # A new test group starts at an explicit fixture boundary BEFORE these
        # readings are published. Retained earlier test runs reuse source seq1..3
        # and are not a valid ordered source. Never delete topics or reset a live group.
        counter = Consumer(
            kafka.consumer_config(contracts.topology["profiles"]["processing"]["group_id"])
        )
        try:
            boundary = source_boundary(counter)
            counter.assign(boundary)
            counter.commit(offsets=boundary, asynchronous=False)
        finally:
            counter.close()
        ingestion = AppSettings.from_env(
            {
                "INGESTION_ENV": "test",
                "INGESTION_HOST": "127.0.0.1",
                "AUTH_MODE": "local-fixture",
                "LOCAL_SOURCE_SUBJECT": "verified-source",
                "LOCAL_ALLOWED_MACHINE_IDS": "M-001",
                "BUSINESS_PUBLIC_ORIGIN": "http://127.0.0.1:8003",
                "KAFKA_LOCAL_MODE": "true",
                "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            }
        )

        async def publish_http():
            app = create_app(replace(ingestion, kafka=kafka))
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
                ) as client:
                    for i, kind in enumerate(
                        ("PREDICTION_COMPLETED", "DATA_INVALID", "INFERENCE_UNAVAILABLE"), 1
                    ):
                        payload = json.loads(json.dumps(sample))
                        payload.update(event_id="evt-" + uuid4().hex, sequence_number=i)
                        if kind == "DATA_INVALID":
                            payload["features"]["rotational_speed_rpm"] = 0
                        if kind == "INFERENCE_UNAVAILABLE":
                            payload["features"]["torque_nm"] = 999
                        expected[payload["event_id"]] = kind
                        assert (
                            await client.post("/api/v1/sensor-readings", json=payload)
                        ).status_code == 202

        asyncio.run(publish_http())
        total = len(expected)

        async def consume():
            task = asyncio.create_task(run_worker(settings, stop, max_records=total))
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=45)
            finally:
                stop.set()
                await asyncio.wait_for(task, timeout=15)

        asyncio.run(consume())
        assert ml_calls.count(next(k for k, v in expected.items() if v == "DATA_INVALID")) == 0
        assert (
            ml_calls.count(next(k for k, v in expected.items() if v == "INFERENCE_UNAVAILABLE"))
            == 2
        )
        with pipeline_database.factory() as connection:
            rows = connection.execute("SELECT event_id,kind FROM processing_results").fetchall()
        assert {
            row["event_id"]: row["kind"] for row in rows if row["event_id"] in expected
        } == expected
        with KafkaPublisher(kafka, "outbox") as publisher:
            relay = create_relay(
                pipeline_database.factory,
                publisher,
                mode="ordered" if stop_during_inference else "legacy",
            )
            while relay.run_once():
                pass
        output = Consumer(kafka.consumer_config("output-" + uuid4().hex))
        output.subscribe(["inference.results", "alert.signals"])
        found = []
        deadline = time.monotonic() + 15
        try:
            while time.monotonic() < deadline and len(found) < 4:
                message = output.poll(0.2)
                if message is None:
                    continue
                assert not message.error(), message.error()
                decoded = contracts.decode(message.topic(), message.value())
                if decoded.payload.get("event_id") in expected:
                    found.append(decoded)
        finally:
            output.close()
        assert len(found) == 4
        assert len([event for event in found if event.topic == "alert.signals"]) == 1
        assert {
            event.payload["event_id"]: event.kind
            for event in found
            if event.topic == "inference.results"
        } == expected
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize(
    "code", [KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE]
)
def test_source_boundary_recovers_from_initial_leader_error(code):
    from types import SimpleNamespace

    class StartingBroker:
        failed = False

        def list_topics(self, topic, timeout):
            return SimpleNamespace(topics={topic: SimpleNamespace(partitions={0: None})})

        def get_watermark_offsets(self, partition, timeout):
            if not self.failed:
                self.failed = True
                raise KafkaException(KafkaError(code))
            return (0, 42)

    boundary = source_boundary(StartingBroker())
    assert [(p.topic, p.partition, p.offset) for p in boundary] == [("sensor.readings", 0, 42)]


@pytest.mark.parametrize(
    "code", [KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.TOPIC_AUTHORIZATION_FAILED]
)
def test_source_boundary_preserves_terminal_error_and_bounds_retries(code):
    from types import SimpleNamespace

    failure = KafkaException(KafkaError(code))
    attempts = []

    class FailedBroker:
        def list_topics(self, topic, timeout):
            return SimpleNamespace(topics={topic: SimpleNamespace(partitions={0: None})})

        def get_watermark_offsets(self, partition, timeout):
            attempts.append(partition.partition)
            raise failure

    started = time.monotonic()
    with pytest.raises(KafkaException) as caught:
        source_boundary(FailedBroker(), timeout=0.02)
    assert caught.value is failure
    assert time.monotonic() - started < 1
    if code == KafkaError.TOPIC_AUTHORIZATION_FAILED:
        assert attempts == [0]
