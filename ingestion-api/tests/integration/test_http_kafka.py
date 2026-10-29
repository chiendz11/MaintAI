"""Opt-in HTTP→real local Kafka boundary; no DB, ML or S3 claims."""

import hashlib
import os
import time
from dataclasses import replace
from uuid import uuid4

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from maintai_messaging.admin import create_local_topics
from maintai_messaging.config import KafkaSettings
from maintai_messaging.contracts import default_contracts
from starlette.testclient import TestClient

from maintai_ingestion.app import create_app

from ..kafka_bootstrap import ReadyTopicAdmin

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def broker():
    if os.environ.get("MAINTAI_INGESTION_INTEGRATION") != "1":
        pytest.skip("Real Kafka explicitly disabled")
    bootstrap = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
    if not bootstrap.startswith(("127.0.0.1:", "localhost:")) or "," in bootstrap:
        pytest.fail("Requires isolated loopback broker")
    cfg = KafkaSettings(
        bootstrap_servers=bootstrap,
        local_mode=True,
        security_protocol="PLAINTEXT",
        delivery_timeout_ms=3000,
    )
    contracts = default_contracts()
    create_local_topics(
        cfg,
        contracts=contracts,
        client=ReadyTopicAdmin(AdminClient(cfg.client_config()), contracts.topology["topics"]),
    )
    return cfg


def find_records(broker, topic, event_id, count=1, timeout=10):
    consumer = Consumer(
        {**broker.consumer_config("m1-" + uuid4().hex), "auto.offset.reset": "earliest"}
    )
    consumer.subscribe([topic])
    found = []
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline and len(found) < count:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            assert not msg.error(), msg.error()
            decoded = default_contracts().decode(topic, msg.value())
            payload = decoded.payload
            if payload.get("event_id") == event_id or payload.get("request_id") == event_id:
                assert msg.key().decode() == decoded.key
                found.append(payload)
    finally:
        consumer.close()
    assert len(found) >= count, f"Expected {count} matching Kafka records"
    return found


def test_http_accepted_ack_and_preserved_invalid_features(settings, payload, broker):
    payload["event_id"] = "evt_" + uuid4().hex
    payload["features"]["rotational_speed_rpm"] = -5
    app = create_app(replace(settings, kafka=broker))
    with TestClient(app) as c:
        r = c.post("/api/v1/sensor-readings", json=payload)
        assert r.status_code == 202
        assert c.get("/health/ready").status_code == 200
    wire = find_records(broker, "sensor.readings", payload["event_id"])[0]
    assert wire["schema_version"] == "1.3.0" and wire["features"]["rotational_speed_rpm"] == -5
    assert wire["sequence_number"] == payload["sequence_number"]
    assert wire["correlation_id"] == r.headers["x-correlation-id"]


@pytest.mark.parametrize("body,status", [(b"{", 400), (b"{}", 422)])
def test_http_quarantine_has_durable_hash_only(settings, broker, body, status):
    request_id = "req_" + uuid4().hex
    with TestClient(create_app(replace(settings, kafka=broker))) as c:
        r = c.post(
            "/api/v1/sensor-readings",
            content=body,
            headers={"Content-Type": "application/json", "X-Request-ID": request_id},
        )
    assert r.status_code == status
    wire = find_records(broker, "sensor.rejected", request_id)[0]
    assert wire["source_identity"] == "verified-source"
    assert wire["payload_sha256"] == hashlib.sha256(body).hexdigest()
    assert wire["payload_disposition"] == "HASH_ONLY"
    assert wire["rejection_id"] == r.json()["error"]["details"]["rejection_id"]


def test_retry_preserves_identity_without_exactly_once_promise(settings, payload, broker):
    payload["event_id"] = "evt_" + uuid4().hex
    with TestClient(create_app(replace(settings, kafka=broker))) as c:
        for _ in range(2):
            assert c.post("/api/v1/sensor-readings", json=payload).status_code == 202
    wires = find_records(broker, "sensor.readings", payload["event_id"], count=2)
    assert all(
        w["event_id"] == payload["event_id"]
        and w["sequence_number"] == payload["sequence_number"]
        and w["features"] == payload["features"]
        for w in wires
    )


def test_unreachable_broker_never_returns_false_ack(settings, payload, broker):
    unreachable = replace(broker, bootstrap_servers="127.0.0.1:1", delivery_timeout_ms=1000)
    with TestClient(create_app(replace(settings, kafka=unreachable))) as c:
        assert c.get("/health/ready").status_code == 503
        started = time.monotonic()
        r = c.post("/api/v1/sensor-readings", json=payload)
        assert r.status_code in {503, 504} and r.json()["error"]["retryable"]
        assert time.monotonic() - started < 5
        r = c.post(
            "/api/v1/sensor-readings", content=b"{", headers={"Content-Type": "application/json"}
        )
        assert r.status_code in {503, 504}
