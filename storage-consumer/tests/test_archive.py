import base64
import gzip
import hashlib
import json
import threading
from dataclasses import replace

import pytest
from botocore.exceptions import ClientError
from maintai_messaging.archive import S3RecordArchive
from maintai_messaging.consumer import ConsumerRunner, Record
from maintai_messaging.errors import ConsumerFailed, IdentityConflict

from maintai_storage.archive import RetryingArchive, ShutdownRequested
from maintai_storage.consumer import ObservedConsumer
from maintai_storage.metrics import Metrics
from maintai_storage.state import State

from .conftest import ConsumerClient, ObjectStore, provider_error


def handler(settings, store, stop=None):
    state = State(settings.roles)
    metrics = Metrics(state)
    wrapped = RetryingArchive(
        S3RecordArchive(store, settings.bucket),
        "storage-accepted",
        settings,
        state,
        metrics,
        stop if stop is not None else threading.Event(),
        jitter=lambda _a, b: b,
    )
    return wrapped, state, metrics


def test_raw_evidence_keeps_exact_bytes_and_replay_identity(settings, record):
    store = ObjectStore()
    archive, _, metrics = handler(settings, store)
    receipt = archive(None, record)
    before = {key: obj["Body"] for key, obj in store.objects.items()}
    assert archive(None, record) == receipt
    assert before == {key: obj["Body"] for key, obj in store.objects.items()}
    body = next(body for key, body in before.items() if key.endswith("jsonl.gz"))
    frame = json.loads(gzip.decompress(body))
    assert base64.b64decode(frame["value_base64"]) == record.value
    assert frame["headers"] == [["correlation_id", "cHJpdmF0ZS1pZA=="], ["duplicate", None]]
    manifest = json.loads(next(b for k, b in before.items() if k.endswith("manifest.json")))
    assert manifest["object"]["sha256"] == hashlib.sha256(body).hexdigest()
    assert manifest["object"]["version_id"]
    assert 'status="COMMITTED"} 2.0' in metrics.render().decode()


def test_partial_upload_retry_finishes_manifest_then_allows_offset(settings, record):
    store = ObjectStore()
    store.failure, store.remaining, store.fail_suffix = provider_error(), 1, ".manifest.json"
    archive, state, metrics = handler(settings, store)
    client = ConsumerClient([record])
    runner = ConsumerRunner(
        settings.kafka,
        "storage-accepted",
        archive,
        client=ObservedConsumer(client, "storage-accepted", state),
    )
    assert runner.run(max_records=1) == 1
    assert len(store.objects) == 2 and client.positions[0].offset == record.offset + 1
    assert store.calls.count(next(k for k in store.objects if k.endswith("jsonl.gz"))) == 2
    text = metrics.render().decode()
    assert 'status="RETRY"} 1.0' in text and 'status="COMMITTED"} 1.0' in text
    assert state.oldest_age() == 0


def test_exhausted_s3_failure_never_commits_and_restart_reuses_partial_object(settings, record):
    store = ObjectStore()
    store.failure, store.remaining, store.fail_suffix = provider_error(), 99, ".manifest.json"
    archive, state, metrics = handler(settings, store)
    client = ConsumerClient([record])
    runner = ConsumerRunner(
        settings.kafka,
        "storage-accepted",
        archive,
        client=ObservedConsumer(client, "storage-accepted", state),
    )
    with pytest.raises(ConsumerFailed):
        runner.run(max_records=1)
    assert client.positions == [] and len(store.objects) == 1
    original = next(iter(store.objects.values()))["VersionId"]
    assert len(store.calls) == 2 * settings.retry_attempts
    assert 'status="FAILED"} 1.0' in metrics.render().decode()
    store.remaining = 0
    second, second_state, _ = handler(settings, store)
    replay = ConsumerClient([record])
    ConsumerRunner(
        settings.kafka,
        "storage-accepted",
        second,
        client=ObservedConsumer(replay, "storage-accepted", second_state),
    ).run(max_records=1)
    assert len(store.objects) == 2 and replay.positions[0].offset == record.offset + 1
    assert next(iter(store.objects.values()))["VersionId"] == original


@pytest.mark.parametrize(
    "code,status", [("AccessDenied", 403), ("NoSuchBucket", 404), ("InvalidAccessKeyId", 403)]
)
def test_permanent_failure_does_not_blindly_retry(settings, record, code, status, caplog):
    store = ObjectStore()
    store.failure, store.remaining = provider_error(code, status), 99
    archive, _, metrics = handler(settings, store)
    with pytest.raises(ClientError):
        archive(None, record)
    assert len(store.calls) == 1
    assert 'status="FAILED"} 1.0' in metrics.render().decode()
    assert "SECRET_DO_NOT_LOG" not in caplog.text
    assert "private-id" not in caplog.text


def test_coordinate_conflict_is_terminal(settings, record):
    store = ObjectStore()
    archive, _, _ = handler(settings, store)
    archive(None, record)
    with pytest.raises(IdentityConflict):
        archive(None, replace(record, value=b"different bytes"))
    assert len(store.calls) == 3


def test_commit_failure_is_replay_safe_even_after_manifest(settings, record):
    store = ObjectStore()
    archive, state, _ = handler(settings, store)
    client = ConsumerClient([record], commit_error=TimeoutError())
    with pytest.raises(TimeoutError):
        ConsumerRunner(
            settings.kafka,
            "storage-accepted",
            archive,
            client=ObservedConsumer(client, "storage-accepted", state),
        ).run(max_records=1)
    assert len(store.objects) == 2 and not client.positions
    before = {k: v["VersionId"] for k, v in store.objects.items()}
    archive(None, record)
    assert before == {k: v["VersionId"] for k, v in store.objects.items()}


def test_shutdown_interrupts_retry_delay_without_a_receipt(settings, record):
    stop = threading.Event()
    store = ObjectStore()
    store.failure, store.remaining = provider_error(), 99
    archive, _, _ = handler(settings, store, stop)

    def stop_during_wait(_timeout):
        stop.set()
        return True

    stop.wait = stop_during_wait
    with pytest.raises(ShutdownRequested):
        archive(None, record)
    assert len(store.calls) == 1


def test_rejected_hash_only_is_copied_without_reconstructing_body(settings):
    from maintai_messaging.contracts import ContractRegistry

    registry = ContractRegistry()
    payload = (registry.directory / "examples" / "rejected.json").read_bytes()
    record = Record("sensor.rejected", 0, 1, b"sensor", payload)
    store = ObjectStore()
    archive, _, _ = handler(settings, store)
    archive(None, record)
    raw = next(v["Body"] for k, v in store.objects.items() if k.endswith("jsonl.gz"))
    original = json.loads(base64.b64decode(json.loads(gzip.decompress(raw))["value_base64"]))
    assert original["payload_disposition"] == "HASH_ONLY"
    assert "raw_body" not in original and "body" not in original


def test_instrumentation_failure_cannot_prevent_durable_receipt(settings, record, monkeypatch):
    store = ObjectStore()
    archive, _, metrics = handler(settings, store)
    monkeypatch.setattr(metrics.batches, "labels", lambda **_kw: 1 / 0)
    assert archive(None, record).offset == record.offset
    assert len(store.objects) == 2
