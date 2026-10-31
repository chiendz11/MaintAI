"""M1 HTTP + real Kafka/Worker/DB/Relay; registry and ML are HTTP fixtures."""

import asyncio
import json
import os
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
from confluent_kafka.admin import AdminClient
from consumer.config import WorkerSettings
from consumer.main import run_worker
from maintai_messaging.admin import create_local_topics
from maintai_messaging.config import KafkaSettings
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import ConfigurationError
from maintai_messaging.producer import KafkaPublisher
from relay.main import create_relay

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


TRANSIENT_METADATA_ERRORS = frozenset(
    {
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._TIMED_OUT,
        KafkaError.REQUEST_TIMED_OUT,
    }
)


def metadata_pending(metadata, expected, *, stage):
    """Distinguish startup propagation from a genuinely incompatible test broker."""
    pending = []
    for name, spec in expected.items():
        topic = metadata.topics.get(name)
        prefix = f"{stage}: topic={name} expected={spec['partitions']}"
        if topic is None:
            pending.append(f"{prefix} observed=missing")
            continue
        if topic.error:
            detail = f"{prefix} error={topic.error.name()}"
            if topic.error.code() not in TRANSIENT_METADATA_ERRORS:
                raise ConfigurationError(detail)
            pending.append(detail)
            continue
        count = len(topic.partitions)
        if count == 0:
            pending.append(f"{prefix} observed=0")
            continue
        if count != spec["partitions"]:
            raise ConfigurationError(f"{prefix} observed={count}")
        for partition, state in topic.partitions.items():
            detail = f"{prefix} observed={count} partition={partition} leader={state.leader}"
            if state.error:
                detail += f" error={state.error.name()}"
                if state.error.code() not in TRANSIENT_METADATA_ERRORS:
                    raise ConfigurationError(detail)
                pending.append(detail)
            elif state.leader < 0:
                pending.append(detail)
    return "; ".join(pending)


class ReadyTopicAdmin:
    """Test-only adapter: preserve SDK creation; await complete metadata on read."""

    def __init__(self, client, expected, *, timeout=30):
        self.client, self.expected, self.timeout = client, expected, timeout

    def create_topics(self, *args, **kwargs):
        return self.client.create_topics(*args, **kwargs)

    def list_topics(self, *, timeout):
        deadline = time.monotonic() + self.timeout
        detail = "bootstrap: metadata unavailable"
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"bootstrap metadata deadline: {detail}")
            try:
                metadata = self.client.list_topics(timeout=min(timeout, 5, remaining))
                detail = metadata_pending(metadata, self.expected, stage="bootstrap")
                if not detail:
                    return metadata
            except KafkaException as error:
                code = error.args[0]
                if code.code() not in TRANSIENT_METADATA_ERRORS:
                    error.add_note("bootstrap: metadata call failed")
                    raise
                detail = f"bootstrap: metadata error={code.name()}"
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))


def source_boundary(counter, *, timeout=15):
    """Capture all fixture offsets before publish; never commit an empty/partial view."""
    expected = {"sensor.readings": default_contracts().topology["topics"]["sensor.readings"]}
    deadline = time.monotonic() + timeout
    detail = "offsets: metadata unavailable"
    last_error = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if last_error is not None:
                last_error.add_note(detail)
                raise last_error
            raise TimeoutError(f"offsets metadata deadline: {detail}")
        try:
            metadata = counter.list_topics("sensor.readings", timeout=min(5, remaining))
            detail = metadata_pending(metadata, expected, stage="offsets")
            last_error = None
            if not detail:
                boundary = []
                for partition, state in metadata.topics["sensor.readings"].partitions.items():
                    detail = (
                        "offsets: topic=sensor.readings "
                        f"expected={expected['sensor.readings']['partitions']} "
                        f"observed={len(metadata.topics['sensor.readings'].partitions)} "
                        f"partition={partition} leader={state.leader}"
                    )
                    budget = deadline - time.monotonic()
                    if budget <= 0:
                        raise TimeoutError(f"offsets deadline: {detail}")
                    _, high = counter.get_watermark_offsets(
                        TopicPartition("sensor.readings", partition),
                        timeout=min(5, budget),
                    )
                    boundary.append(TopicPartition("sensor.readings", partition, high))
                return boundary
        except KafkaException as error:
            last_error = error
            code = error.args[0]
            if code.code() not in TRANSIENT_METADATA_ERRORS or time.monotonic() >= deadline:
                error.add_note(detail)
                raise
            detail += f" error={code.name()}"
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))


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
        bootstrap_servers=os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:39092"),
        group_prefix=prefix,
        delivery_timeout_ms=3000,
        producer_git_sha="1" * 40,
    )
    create_local_topics(
        kafka,
        contracts=contracts,
        client=ReadyTopicAdmin(AdminClient(kafka.client_config()), contracts.topology["topics"]),
    )
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
    class StartingBroker:
        failed = False

        def list_topics(self, topic, timeout):
            return topology_snapshot()

        def get_watermark_offsets(self, partition, timeout):
            if not self.failed:
                self.failed = True
                raise KafkaException(KafkaError(code))
            return (0, 42)

    boundary = source_boundary(StartingBroker())
    assert [(p.topic, p.partition, p.offset) for p in boundary] == [
        ("sensor.readings", i, 42) for i in range(12)
    ]


@pytest.mark.parametrize(
    "code", [KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.TOPIC_AUTHORIZATION_FAILED]
)
def test_source_boundary_preserves_terminal_error_and_bounds_retries(code):
    failure = KafkaException(KafkaError(code))
    attempts = []

    class FailedBroker:
        def list_topics(self, topic, timeout):
            return topology_snapshot()

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


class MetadataSnapshots:
    """Controlled Admin/Consumer network boundary; actual SDK bootstrap stays real."""

    def __init__(self, snapshots):
        self.snapshots = iter(snapshots)
        self.last = None

    def create_topics(self, topics, **kwargs):
        from concurrent.futures import Future

        futures = {}
        for topic in topics:
            future = Future()
            future.set_result(None)
            futures[topic.topic] = future
        return futures

    def list_topics(self, *args, **kwargs):
        self.last = next(self.snapshots, self.last)
        if isinstance(self.last, Exception):
            raise self.last
        return self.last

    def get_watermark_offsets(self, partition, timeout):
        return (0, 42)


def topology_snapshot(*, missing=False, count=12, error=None, leader=1):
    from types import SimpleNamespace

    return SimpleNamespace(
        topics={}
        if missing
        else {
            "sensor.readings": SimpleNamespace(
                error=error,
                partitions={i: SimpleNamespace(error=None, leader=leader) for i in range(count)},
            )
        }
    )


@pytest.mark.parametrize(
    "initial",
    [
        topology_snapshot(missing=True),
        topology_snapshot(count=0),
        topology_snapshot(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
        topology_snapshot(leader=-1),
    ],
)
def test_bootstrap_waits_for_expected_topic_metadata(initial):
    from types import SimpleNamespace

    expected = {
        "sensor.readings": {"partitions": 12, "retention_ms": 60000, "max_message_bytes": 262144}
    }
    contracts = SimpleNamespace(topology={"topics": expected})
    settings = KafkaSettings(local_mode=True, security_protocol="PLAINTEXT")
    raw = MetadataSnapshots([initial, topology_snapshot()])
    assert create_local_topics(
        settings, contracts=contracts, client=ReadyTopicAdmin(raw, expected, timeout=1)
    ) == ["sensor.readings"]


@pytest.mark.parametrize(
    "snapshot,fragment",
    [
        (topology_snapshot(count=1), "expected=12 observed=1"),
        (
            topology_snapshot(error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)),
            "TOPIC_AUTHORIZATION_FAILED",
        ),
    ],
)
def test_bootstrap_rejects_permanent_metadata_failure(snapshot, fragment):
    from maintai_messaging.errors import ConfigurationError

    admin = ReadyTopicAdmin(
        MetadataSnapshots([snapshot, topology_snapshot()]),
        {"sensor.readings": {"partitions": 12}},
        timeout=1,
    )
    with pytest.raises(ConfigurationError, match=fragment):
        admin.list_topics(timeout=10)


def test_bootstrap_metadata_deadline_has_topic_diagnostics():
    admin = ReadyTopicAdmin(
        MetadataSnapshots([topology_snapshot(missing=True)]),
        {"sensor.readings": {"partitions": 12}},
        timeout=0.02,
    )
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="bootstrap.*sensor.readings.*missing"):
        admin.list_topics(timeout=10)
    assert time.monotonic() - started < 1


@pytest.mark.parametrize(
    "initial",
    [
        topology_snapshot(missing=True),
        topology_snapshot(count=0),
        topology_snapshot(leader=-1),
    ],
)
def test_boundary_requires_complete_metadata(initial):
    boundary = source_boundary(MetadataSnapshots([initial, topology_snapshot()]), timeout=1)
    assert [(p.partition, p.offset) for p in boundary] == [(i, 42) for i in range(12)]


@pytest.mark.parametrize(
    "snapshot,fragment",
    [
        (topology_snapshot(count=1), "expected=12 observed=1"),
        (
            topology_snapshot(error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)),
            "TOPIC_AUTHORIZATION_FAILED",
        ),
    ],
)
def test_boundary_rejects_permanent_metadata_failure(snapshot, fragment):
    from maintai_messaging.errors import ConfigurationError

    with pytest.raises(ConfigurationError, match=fragment):
        source_boundary(MetadataSnapshots([snapshot, topology_snapshot()]), timeout=1)


def test_boundary_drops_partial_capture_before_retry():
    class ChangingOffsets(MetadataSnapshots):
        failed = False

        def get_watermark_offsets(self, partition, timeout):
            if partition.partition == 1 and not self.failed:
                self.failed = True
                raise KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION))
            return (0, 84 if self.failed else 42)

    boundary = source_boundary(ChangingOffsets([topology_snapshot()]), timeout=1)
    assert [(p.partition, p.offset) for p in boundary] == [(i, 84) for i in range(12)]


def test_boundary_missing_metadata_expires_with_diagnostics():
    with pytest.raises(TimeoutError, match="offsets.*sensor.readings.*missing"):
        source_boundary(MetadataSnapshots([topology_snapshot(missing=True)]), timeout=0.02)


@pytest.mark.parametrize(
    "code", [KafkaError.LEADER_NOT_AVAILABLE, KafkaError.TOPIC_AUTHORIZATION_FAILED]
)
def test_bootstrap_classifies_metadata_call_errors(code):
    error = KafkaException(KafkaError(code))
    admin = ReadyTopicAdmin(
        MetadataSnapshots([error, topology_snapshot()]),
        {"sensor.readings": {"partitions": 12}},
        timeout=1,
    )
    if code == KafkaError.LEADER_NOT_AVAILABLE:
        assert len(admin.list_topics(timeout=10).topics["sensor.readings"].partitions) == 12
    else:
        with pytest.raises(KafkaException) as caught:
            admin.list_topics(timeout=10)
        assert caught.value is error


def test_pending_topic_does_not_hide_another_topic_partition_mismatch():
    metadata = topology_snapshot(count=1)
    admin = ReadyTopicAdmin(
        MetadataSnapshots([metadata]),
        {"not-visible": {"partitions": 12}, "sensor.readings": {"partitions": 12}},
        timeout=1,
    )
    with pytest.raises(
        ConfigurationError, match="bootstrap.*sensor.readings.*expected=12 observed=1"
    ):
        admin.list_topics(timeout=10)


def test_boundary_partition_error_is_not_a_successful_snapshot():
    metadata = topology_snapshot()
    metadata.topics["sensor.readings"].partitions[3].error = KafkaError(
        KafkaError.TOPIC_AUTHORIZATION_FAILED
    )
    with pytest.raises(
        ConfigurationError, match="offsets.*partition=3.*TOPIC_AUTHORIZATION_FAILED"
    ):
        source_boundary(MetadataSnapshots([metadata]), timeout=1)
