"""Real broker/DB Consumer wiring; outcome payloads are explicit contract fixtures."""

import json
import os
import re
import socket
import threading
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from backend.shared.persistence.migration import upgrade
from backend.shared.persistence.projections import ProjectionRepository
from backend.shared.persistence.registry import RegistryRepository
from business_consumer.config import Settings
from business_consumer.runtime import serve
from confluent_kafka import ConsumerGroupTopicPartitions, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, OffsetSpec
from maintai_messaging.config import KafkaSettings
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.producer import KafkaPublisher

ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.integration


def prepare_new_test_groups(settings):
    """Bootstrap fresh UUID test groups at captured offsets; never reset live groups."""
    if not settings.local_mode or not re.fullmatch(
        r"m3-(?:flow-)?[0-9a-f]{32}-", settings.group_prefix
    ):
        raise ValueError("Only isolated UUID M3 test groups may bootstrap offsets")
    admin = AdminClient(settings.client_config())
    contracts = default_contracts()
    names = ("sensor.readings", "inference.results")
    for future in admin.create_topics(
        [
            NewTopic(
                name,
                num_partitions=contracts.topology["topics"][name]["partitions"],
                replication_factor=1,
                config={"min.insync.replicas": "1"},
            )
            for name in names
        ]
    ).values():
        try:
            future.result(timeout=15)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    deadline = time.monotonic() + 30
    while True:
        metadata = admin.list_topics(timeout=3)
        ready = True
        for name in names:
            topic = metadata.topics.get(name)
            if topic is None:
                ready = False
                continue
            if topic.error is not None:
                if topic.error.code() not in {
                    KafkaError.LEADER_NOT_AVAILABLE,
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                    KafkaError.NOT_LEADER_FOR_PARTITION,
                }:
                    raise KafkaException(topic.error)
                ready = False
                continue
            count = len(topic.partitions)
            if count and count != 12:
                pytest.fail(f"Canonical topic {name} has unexpected partition count {count}")
            ready = (
                ready
                and count == 12
                and all(p.leader >= 0 and p.error is None for p in topic.partitions.values())
            )
        if ready:
            break
        if time.monotonic() >= deadline:
            pytest.fail("M3 broker topic metadata/leadership did not become ready")
        time.sleep(0.1)
    transient = {
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.REQUEST_TIMED_OUT,
        KafkaError._TIMED_OUT,
        KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
        KafkaError.COORDINATOR_NOT_AVAILABLE,
        KafkaError.NOT_COORDINATOR,
    }

    def result(future):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("M3 bootstrap deadline exceeded")
        return future.result(timeout=min(remaining, 5))

    def retry(exc, stage):
        if isinstance(exc, KafkaException) and exc.args[0].code() not in transient:
            raise exc
        if time.monotonic() >= deadline:
            pytest.fail(f"M3 {stage} did not become ready before bootstrap deadline")
        time.sleep(0.1)

    requested = {(name, p) for name in names for p in range(12)}
    while True:
        try:
            # Capture all topics again after a transient failure, discarding every
            # partial previous result. No offsets are written until capture is complete.
            admin.list_topics(timeout=3)
            futures = admin.list_offsets(
                {TopicPartition(name, p): OffsetSpec.latest() for name, p in sorted(requested)}
            )
            if {(tp.topic, tp.partition) for tp in futures} != requested:
                raise ValueError("M3 broker returned an incomplete offset boundary")
            captured = {}
            for tp, future in futures.items():
                offset = result(future).offset
                if type(offset) is not int or offset < 0:
                    raise ValueError("M3 broker returned an invalid boundary offset")
                captured[(tp.topic, tp.partition)] = offset
            break
        except (KafkaException, TimeoutError) as exc:
            retry(exc, "offset capture")

    for role in ("business-live", "business-prediction"):
        profile = contracts.topology["profiles"][role]
        topic = profile["topic"]
        partitions = [TopicPartition(topic, p, captured[(topic, p)]) for p in range(12)]
        request = ConsumerGroupTopicPartitions(
            settings.group_prefix + profile["group_id"], partitions
        )
        while True:
            try:
                for future in admin.alter_consumer_group_offsets([request]).values():
                    reply = result(future)
                    returned = reply.topic_partitions or []
                    if {(p.topic, p.partition) for p in returned} != {
                        (topic, p) for p in range(12)
                    }:
                        raise ValueError("M3 group bootstrap acknowledgement is incomplete")
                    for partition in returned:
                        if partition.error is not None:
                            raise KafkaException(partition.error)
                break
            except (KafkaException, TimeoutError) as exc:
                # The groups are new and no consumers have started: repeating the
                # identical captured boundary is safe even after an uncertain response.
                retry(exc, "test group initialization")


def test_two_groups_materialize_real_kafka_and_shutdown_without_consuming_alerts(business_database):
    db = business_database
    upgrade(db.dsn, schema=db.schema)
    RegistryRepository(db.factory).provision([{"machine_id": "M-001", "display_name": "One"}])
    settings = KafkaSettings(
        bootstrap_servers=os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:39092"),
        local_mode=True,
        security_protocol="PLAINTEXT",
        group_prefix="m3-" + uuid4().hex + "-",
    )
    prepare_new_test_groups(settings)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    stop, errors = threading.Event(), []

    def run():
        try:
            serve(
                Settings(db.dsn, settings, "127.0.0.1", port),
                stop=stop,
                connection_factory=db.factory,
            )
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    publishers = {
        "sensor.readings": KafkaPublisher(settings, "ingestion"),
        "inference.results": KafkaPublisher(settings, "outbox"),
    }
    thread.start()
    try:
        for name, seq in (("completed", 9), ("sensor-reading", 10), ("invalid", 11)):
            payload = json.loads((ROOT / f"contracts/kafka/examples/{name}.json").read_text())
            payload.update(sequence_number=seq, event_id=f"m3-{seq}")
            if "result_id" in payload:
                payload["result_id"] = "m3-result-" + str(seq)
            topic = "sensor.readings" if name == "sensor-reading" else "inference.results"
            publishers[topic].publish_event(
                default_contracts().decode(topic, canonical_json(payload))
            )
        repository = ProjectionRepository(db.factory)
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if errors:
                raise errors[0]
            value = repository.live("M-001")
            if (
                value["telemetry"]
                and value["latest_valid_prediction"]
                and value["latest_processing"]["processing_status"] == "INVALID"
            ):
                break
            time.sleep(0.1)
        else:
            pytest.fail("Business groups did not materialize their durable projections")
        assert value["telemetry"]["sequence_number"] == 10
        assert value["latest_valid_prediction"]["sequence_number"] == 9
        assert value["latest_processing"]["sequence_number"] == 11
        response = httpx.get(f"http://127.0.0.1:{port}/health/ready", timeout=3)
        assert response.status_code == 200
        with db.factory() as conn:
            groups = conn.execute(
                "SELECT DISTINCT consumer_group FROM maintai_kafka_inbox"
            ).fetchall()
            assert {r["consumer_group"] for r in groups} == {
                settings.group_prefix + "business-live-v1",
                settings.group_prefix + "business-prediction-v1",
            }
    finally:
        stop.set()
        for publisher in publishers.values():
            publisher.close()
        thread.join(timeout=30)
    assert not thread.is_alive()
    assert errors == []
