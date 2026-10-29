import base64
import gzip
import json
import threading

import pytest
from maintai_messaging.archive import S3RecordArchive
from maintai_messaging.consumer import ConsumerRunner, DurableReceipt, Record
from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import ConfigurationError, ConsumerFailed, IdentityConflict
from maintai_messaging.outcomes import ModelIdentity, OutcomeFactory, Prediction, RiskDecision

from ..conftest import ConsumerClient, Message, ObjectStore


def message(example, offset=0):
    payload = example("sensor-reading")
    return Message(canonical_json(payload), key=payload["machine_id"].encode(), offset=offset)


def test_offset_progress_after_durable_receipts_is_contiguous(settings, contracts, example):
    native = [message(example, offset=3), message(example, offset=5)]
    client = ConsumerClient(native)
    persisted = []

    def durable(_event, record):
        assert len(client.commits) == len(persisted)
        persisted.append(record.offset)
        return DurableReceipt.for_record(record)

    runner = ConsumerRunner(settings, "processing", durable, contracts=contracts, client=client)
    assert runner.run(max_records=2) == 2
    assert persisted == [3, 5]
    assert [p.offset for p in client.commits] == [4, 6]
    assert client.closed


def test_handler_failure_never_commits_or_processes_next_record(settings, contracts, example):
    client = ConsumerClient([message(example), message(example, 1)])
    called = []

    def failed(_event, record):
        called.append(record.offset)
        raise OSError("database unavailable")

    runner = ConsumerRunner(settings, "processing", failed, contracts=contracts, client=client)
    with pytest.raises(ConsumerFailed):
        runner.run(max_records=2)
    assert called == [0] and client.commits == [] and client.closed


def test_commit_failure_does_not_advance_or_process_later_record(settings, contracts, example):
    client = ConsumerClient(
        [message(example), message(example, 1)], commit_error=OSError("coordinator unavailable")
    )
    persisted = []

    def durable(_event, record):
        persisted.append(record.offset)
        return DurableReceipt.for_record(record)

    runner = ConsumerRunner(settings, "processing", durable, contracts=contracts, client=client)
    with pytest.raises(OSError):
        runner.run(max_records=2)
    assert persisted == [0] and not client.commits and client.closed


@pytest.mark.parametrize("receipt", [None, DurableReceipt("wrong-topic", 0, 0, "a" * 64)])
def test_returning_without_proof_of_durability_never_commits(settings, contracts, example, receipt):
    client = ConsumerClient([message(example)])
    runner = ConsumerRunner(
        settings, "processing", lambda *_: receipt, contracts=contracts, client=client
    )
    with pytest.raises(ConsumerFailed):
        runner.run(max_records=1)
    assert not client.commits


def test_polling_continues_while_handler_is_running(settings, contracts, example):
    gate = threading.Event()

    def during_wait(client):
        if client.poll_count >= 4:
            gate.set()

    client = ConsumerClient([message(example)], during_wait=during_wait)

    def slow(_event, record):
        assert gate.wait(2)
        return DurableReceipt.for_record(record)

    runner = ConsumerRunner(settings, "processing", slow, contracts=contracts, client=client)
    runner.run(max_records=1)
    assert client.poll_count >= 4 and client.pauses and len(client.commits) == 1


def test_lost_assignment_during_work_cannot_commit_stale_offset(settings, contracts, example):
    gate, stop = threading.Event(), threading.Event()

    def lose_assignment(client):
        client.callbacks["on_lost"](client, client.assignment())
        client.assign([])
        stop.set()
        gate.set()

    client = ConsumerClient([message(example)], during_wait=lose_assignment)

    def slow(_event, record):
        assert gate.wait(2)
        return DurableReceipt.for_record(record)

    runner = ConsumerRunner(settings, "processing", slow, contracts=contracts, client=client)
    runner.run(stop)
    assert not client.commits and client.closed


def test_schema_poison_and_wrong_key_fail_closed(settings, contracts, example):
    for native in [
        Message(b'{"schema_version":"unknown"}'),
        Message(message(example).value(), key=b"wrong"),
    ]:
        client = ConsumerClient([native])
        handler_called = []
        runner = ConsumerRunner(
            settings,
            "processing",
            lambda *args, called=handler_called: called.append(args),
            contracts=contracts,
            client=client,
        )
        with pytest.raises(ConsumerFailed):
            runner.run(max_records=1)
        assert not client.commits and not handler_called


@pytest.mark.parametrize("name", ["completed", "invalid", "unavailable"])
def test_business_rejects_retired_outcome_before_effects_or_offset_commit(
    settings, contracts, example, name
):
    payload = example("negative-legacy-" + name)
    client = ConsumerClient(
        [Message(canonical_json(payload), topic="inference.results", key=b"M-001")]
    )
    called = []
    runner = ConsumerRunner(
        settings,
        "business-prediction",
        lambda *args: called.append(args),
        contracts=contracts,
        client=client,
    )
    with pytest.raises(ConsumerFailed):
        runner.run(max_records=1)
    assert not called and not client.commits and client.closed


def test_storage_preserves_retired_wire_as_evidence_without_interpreting_it(
    settings, contracts, example
):
    raw = canonical_json(example("negative-legacy-completed"))
    client = ConsumerClient([Message(raw, topic="inference.results", key=b"M-001")])
    store = ObjectStore()
    runner = ConsumerRunner(
        settings,
        "storage-outcomes",
        S3RecordArchive(store, "fixture-bucket"),
        contracts=contracts,
        client=client,
    )
    assert runner.run(max_records=1) == 1
    body = next(v["Body"] for k, v in store.objects.items() if k.endswith(".jsonl.gz"))
    assert base64.b64decode(json.loads(gzip.decompress(body))["value_base64"]) == raw
    assert len(client.commits) == 1


def test_raw_archive_replay_is_immutable_and_preserves_wire_bytes():
    client = ObjectStore()
    archive = S3RecordArchive(client, "fixture-bucket")
    record = Record(
        "sensor.readings",
        2,
        19,
        b"key",
        b"\xff\x00invalid-json",
        (("tag", b"one"), ("tag", None)),
        123,
    )
    first = archive(None, record)
    before = {key: value["Body"] for key, value in client.objects.items()}
    assert archive(None, record) == first
    assert before == {key: value["Body"] for key, value in client.objects.items()}
    assert len(client.objects) == 2
    raw = next(value["Body"] for key, value in client.objects.items() if key.endswith(".jsonl.gz"))
    frame = json.loads(gzip.decompress(raw))
    assert base64.b64decode(frame["value_base64"]) == record.value
    assert frame["headers"] == [["tag", "b25l"], ["tag", None]]
    manifest = next(
        json.loads(value["Body"])
        for key, value in client.objects.items()
        if key.endswith(".manifest.json")
    )
    assert manifest["record_count"] == 1
    assert manifest["object"]["version_id"]


def test_empty_optional_s3_endpoint_uses_default_provider(monkeypatch, settings):
    import boto3

    captured = {}
    client = ObjectStore()

    def create(service, **options):
        captured.update(options)
        assert service == "s3"
        return client

    monkeypatch.setattr(boto3, "client", create)
    monkeypatch.setenv("S3_RAW_BUCKET", "fixture-bucket")
    monkeypatch.setenv("S3_ENDPOINT_URL", "")
    monkeypatch.setenv("S3_ALLOW_UNVERSIONED", "false")
    archive = S3RecordArchive.from_env(settings)
    assert archive.client is client and captured["endpoint_url"] is None


def test_archive_partial_upload_retries_without_losing_original_object():
    client = ObjectStore(fail_manifest=True)
    archive = S3RecordArchive(client, "fixture-bucket")
    record = Record("sensor.rejected", 0, 1, b"sensor", b'{"redacted":true}')
    with pytest.raises(OSError):
        archive(None, record)
    assert len(client.objects) == 1
    original = next(iter(client.objects.values()))["Body"]
    client.fail_manifest = False
    archive(None, record)
    assert len(client.objects) == 2
    assert (
        next(value["Body"] for key, value in client.objects.items() if key.endswith(".jsonl.gz"))
        == original
    )


def test_failed_manifest_has_no_consumer_commit(settings, contracts):
    archive = S3RecordArchive(ObjectStore(fail_manifest=True), "fixture-bucket")
    client = ConsumerClient([Message(b"opaque-source-bytes")])
    runner = ConsumerRunner(
        settings, "storage-accepted", archive, contracts=contracts, client=client
    )
    with pytest.raises(ConsumerFailed):
        runner.run(max_records=1)
    assert not client.commits


def test_conflicting_archive_bytes_and_missing_version_are_rejected():
    client = ObjectStore()
    archive = S3RecordArchive(client, "fixture-bucket")
    a = Record("sensor.readings", 0, 0, b"machine", b"first")
    archive(None, a)
    with pytest.raises(IdentityConflict):
        archive(None, Record(a.topic, a.partition, a.offset, a.key, b"changed"))
    with pytest.raises(ConfigurationError, match="VersionId"):
        S3RecordArchive(ObjectStore(versioned=False), "fixture-bucket")(None, a)


def test_outcomes_use_actual_source_and_provenance_without_calculating_risk(contracts, example):
    source = contracts.decode("sensor.readings", canonical_json(example("sensor-reading")))
    record = Record(source.topic, 2, 42, source.key.encode(), source.wire_bytes)
    factory = OutcomeFactory(
        source,
        record,
        dq_ruleset_version="fixture-dq",
        dq_ruleset_sha256="a" * 64,
        producer_git_sha="b" * 40,
        contracts=contracts,
    )
    completed = example("completed")
    model = ModelIdentity(
        **{field: completed[field] for field in ModelIdentity.__dataclass_fields__}
    )
    prediction = Prediction("fixture-pred", "FAILURE", 0.8, 0.5, completed["completed_at"], model)
    risk = RiskDecision("fixture-risk", "fixture-policy", "c" * 64, "HIGH", 0.7)
    outcome = factory.completed(prediction, risk)
    assert outcome["source_offset"] == 42 and outcome["source_partition"] == 2
    assert (
        outcome["risk_score"] == 0.7
        and outcome["model_artifact_sha256"] == model.model_artifact_sha256
    )
    signal = factory.alert_signal(outcome)
    assert signal["severity"] == "HIGH" and signal["event_id"] == source.message_id
    invalid = factory.invalid([{"field": "features.rotational_speed_rpm", "code": "RANGE"}])
    assert "prediction" not in invalid and "risk_score" not in invalid
    assert factory.unavailable("ML_TIMEOUT", 2)["kind"] == "INFERENCE_UNAVAILABLE"
