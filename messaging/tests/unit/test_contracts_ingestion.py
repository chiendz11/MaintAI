import hashlib
import shutil
from dataclasses import replace

import pytest

from maintai_messaging.config import KafkaSettings
from maintai_messaging.contracts import canonical_json, strict_json
from maintai_messaging.errors import ConfigurationError, ContractViolation, PublishUncertain
from maintai_messaging.ingestion import IngestionGateway, SourceIdentity
from maintai_messaging.producer import KafkaPublisher

from ..conftest import ProducerClient


def test_tampered_contract_bundle_is_rejected(contracts, tmp_path):
    from maintai_messaging.contracts import ContractRegistry

    root = tmp_path / "contracts"
    shutil.copytree(contracts.directory, root / "kafka")
    for relative in ["v1.3/schemas/runtime.schema.json", "v1.3/events/alert-signal.schema.json"]:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(contracts.root / relative, destination)
    target = root / "kafka/topology.json"
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(ContractViolation, match="checksum"):
        ContractRegistry(root)


def test_all_fixture_outcomes_and_reader_only_legacy(contracts, example):
    assert contracts.check_examples() == 15
    legacy = contracts.decode("inference.results", canonical_json(example("legacy-invalid")))
    assert legacy.legacy and legacy.kind == "DATA_INVALID"
    assert legacy.source_event_id == legacy.payload["source_event_id"]
    with pytest.raises(ContractViolation, match="read-only"):
        contracts.encode("inference.results", legacy.payload)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"secret":1,"secret":2}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b"null",
        b'{"x":1e999}',
        b'{"x":"\\ud800"}',
    ],
)
def test_strict_json_has_no_coercion_or_duplicate_keys(raw):
    with pytest.raises(ContractViolation):
        strict_json(raw)


@pytest.mark.parametrize(
    "setting",
    [
        {"KAFKA_ENABLE_AUTO_COMMIT": "true"},
        {"KAFKA_ENABLE_AUTO_OFFSET_STORE": "true"},
        {"KAFKA_ACKS": "1"},
        {"KAFKA_ENABLE_IDEMPOTENCE": "false"},
        {"KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"},
    ],
)
def test_unsafe_configs_fail_before_client_creation(setting):
    with pytest.raises(ConfigurationError):
        KafkaSettings.from_env(setting)


def test_secret_not_exposed_in_settings_repr(settings):
    assert "do-not-log" not in repr(replace(settings, password="do-not-log"))


def gateway(settings, contracts, client=None):
    publisher = KafkaPublisher(
        settings, "ingestion", contracts=contracts, client=client or ProducerClient()
    )
    return IngestionGateway(publisher, "https://business.example.invalid", contracts=contracts)


def identity(payload):
    return SourceIdentity("verified-sensor", frozenset({payload["machine_id"]}))


def test_valid_envelope_with_invalid_features_is_accepted_only_after_ack(
    settings, contracts, example
):
    payload = example("sensor-request")
    payload["features"]["rotational_speed_rpm"] = -5
    client = ProducerClient()
    service = gateway(settings, contracts, client)
    response = service.handle(
        canonical_json(payload), identity=identity(payload), request_id="req", correlation_id="cor"
    )
    assert response.status == 202
    assert client.pending == []  # Callback ACK was serviced before success returned.
    assert client.calls[0][0] == "sensor.readings"
    assert strict_json(client.calls[0][1]["value"])["features"]["rotational_speed_rpm"] == -5
    assert response.headers["Location"].startswith("https://business.example.invalid/")
    assert response.body["status_url"] == response.headers["Location"]


@pytest.mark.parametrize(
    ("raw", "status"), [(b'{"secret":"private",', 400), (b"{}", 422), (b"[]", 422)]
)
def test_rejection_archived_hash_only_before_client_error(settings, contracts, raw, status):
    client = ProducerClient()
    service = gateway(settings, contracts, client)
    response = service.handle(
        raw,
        identity=SourceIdentity("sensor", frozenset({"M-001"})),
        request_id="req",
        correlation_id="cor",
    )
    assert response.status == status
    assert client.pending == [] and client.calls[0][0] == "sensor.rejected"
    event = strict_json(client.calls[0][1]["value"])
    assert event["payload_disposition"] == "HASH_ONLY"
    assert event["payload_sha256"] == hashlib.sha256(raw).hexdigest()
    assert b"private" not in client.calls[0][1]["value"]


def test_broker_failure_does_not_claim_rejection_durability(settings, contracts):
    client = ProducerClient(delivery_error=RuntimeError("not acknowledged"))
    response = gateway(settings, contracts, client).handle(
        b"{}",
        identity=SourceIdentity("sensor", frozenset({"M-001"})),
        request_id="req",
        correlation_id="cor",
    )
    assert response.status == 504 and response.body["error"]["retryable"] is True
    assert "rejection_id" not in response.body


def test_unauthorized_and_oversize_never_reach_quarantine(settings, contracts):
    client = ProducerClient()
    service = gateway(settings, contracts, client)
    assert (
        service.handle(b"{}", identity=None, request_id="req", correlation_id="cor").status == 401
    )
    oversized = b"x" * (settings.max_payload_bytes + 1)
    assert (
        service.handle(
            oversized,
            identity=SourceIdentity("sensor", frozenset({"M-001"})),
            request_id="req",
            correlation_id="cor",
        ).status
        == 413
    )
    assert not client.calls


def test_machine_scope_enforced_before_publish(settings, contracts, example):
    client = ProducerClient()
    response = gateway(settings, contracts, client).handle(
        canonical_json(example("sensor-request")),
        identity=SourceIdentity("sensor", frozenset({"OTHER"})),
        request_id="req",
        correlation_id="cor",
    )
    assert response.status == 403 and not client.calls


def test_retry_receipt_metadata_does_not_conflict_with_reading_identity(contracts, example):
    first = example("sensor-reading")
    second = {**first, "server_accepted_at": "2026-10-19T00:01:00Z", "correlation_id": "retry"}
    a, b = [contracts.decode("sensor.readings", canonical_json(p)) for p in [first, second]]
    assert a.identity_sha256 == b.identity_sha256
    second["features"]["torque_nm"] += 1
    c = contracts.decode("sensor.readings", canonical_json(second))
    assert c.identity_sha256 != a.identity_sha256


def test_partition_key_and_schema_headers_come_from_validated_payload(settings, contracts, example):
    client = ProducerClient(queue_failures=2)
    publisher = KafkaPublisher(settings, "ingestion", contracts=contracts, client=client)
    payload = example("sensor-reading")
    ack = publisher.publish("sensor.readings", payload)
    assert ack.offset == 7
    _, call = client.calls[0]
    assert call["key"] == payload["machine_id"].encode()
    assert dict(call["headers"])["schema_version"] == b"1.3.0"


def test_uncertain_ack_is_bounded_and_not_success(settings, contracts, example, monkeypatch):
    import maintai_messaging.producer as module

    class Clock:
        now = 0

        def monotonic(self):
            self.now += 0.2
            return self.now

    monkeypatch.setattr(module, "time", Clock())
    publisher = KafkaPublisher(
        settings, "ingestion", contracts=contracts, client=ProducerClient(never_ack=True)
    )
    with pytest.raises(PublishUncertain):
        publisher.publish("sensor.readings", example("sensor-reading"))


def test_publisher_cannot_bypass_its_role(settings, contracts, example):
    client = ProducerClient()
    publisher = KafkaPublisher(settings, "ingestion", contracts=contracts, client=client)
    with pytest.raises(ContractViolation):
        publisher.publish("inference.results", example("completed"))
    assert not client.calls
