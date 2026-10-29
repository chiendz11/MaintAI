"""Failure/recovery contracts. Memory fixtures do not replace the MinIO CI job."""

import copy
import hashlib
import io
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
from maintai_messaging.archive import S3RecordArchive
from maintai_messaging.contracts import canonical_json
from maintai_messaging.errors import ConfigurationError, IdentityConflict

from maintai_storage.catalog import (
    CatalogPublisher,
    CatalogReader,
    CatalogStore,
    now_utc,
    validate,
)
from maintai_storage.catalog_runtime import CatalogRuntime, KafkaAudit
from maintai_storage.cli import main
from maintai_storage.settings import Settings

from .conftest import provider_error


class VersionedS3:
    def __init__(self):
        self.versions, self.latest = {}, {}
        self.sequence = 0
        self.before_pointer = None
        self.lose_ack = False

    def put_object(self, **args):
        key = args["Key"]
        if key.endswith("/current.json") and self.before_pointer:
            hook, self.before_pointer = self.before_pointer, None
            hook()
        previous = self.latest.get(key)
        if args.get("IfNoneMatch") == "*" and previous:
            raise provider_error("PreconditionFailed", 412)
        if "IfMatch" in args and (not previous or previous["ETag"] != args["IfMatch"]):
            raise provider_error("PreconditionFailed", 412)
        self.sequence += 1
        obj = {
            "body": args["Body"],
            "VersionId": str(self.sequence),
            "ETag": '"' + hashlib.sha256(args["Body"]).hexdigest() + '"',
        }
        self.versions[(key, obj["VersionId"])] = obj
        self.latest[key] = obj
        if key.endswith("/current.json") and self.lose_ack:
            self.lose_ack = False
            raise TimeoutError("fixture upload ACK lost")
        return {"VersionId": obj["VersionId"], "ETag": obj["ETag"]}

    def get_object(self, *, Bucket, Key, VersionId=None):
        obj = self.versions.get((Key, VersionId)) if VersionId else self.latest.get(Key)
        if obj is None:
            raise provider_error("NoSuchKey", 404)
        return {"Body": io.BytesIO(obj["body"]), "VersionId": obj["VersionId"], "ETag": obj["ETag"]}

    def get_paginator(self, name):
        assert name == "list_objects_v2"

        def paginate(**args):
            keys = sorted(k for k in self.latest if k.startswith(args["Prefix"]))
            for index in range(0, len(keys), 2):
                yield {"Contents": [{"Key": k} for k in keys[index : index + 2]]}

        return SimpleNamespace(paginate=paginate)


def coverage(partition=0, next_offset=1, *, high=None):
    return {
        "topic": "sensor.readings",
        "partition": partition,
        "topic_id": "fixture-topic",
        "start_offset": 0,
        "archived_next_offset": next_offset,
        "indexed_next_offset": next_offset,
        "captured_log_end_offset": next_offset if high is None else high,
        "captured_at": now_utc(),
        "gaps": [],
        "status": "COMPLETE" if high is None or high <= next_offset else "LAGGING",
    }


def archived(s3, record, *, offset=0, partition=0, payload=None):
    payload = (
        payload
        if payload is not None
        else {
            "event_id": "same-event",
            "machine_id": "M-001",
            "received_at": "2026-10-29T10:00:00Z",
            "features": {"bad": "raw"},
        }
    )
    record = replace(record, offset=offset, partition=partition, value=canonical_json(payload))
    S3RecordArchive(s3, "fixture-bucket")(None, record)
    return CatalogStore(s3, "fixture-bucket").receipt(record.topic, partition, offset)


class Audit:
    def __init__(self, offsets=(0,), *, high=1, low=0):
        self.offsets, self.high, self.low = list(offsets), high, low

    def describe(self):
        return "fixture-cluster", [("sensor.readings", 0, "fixture-topic")]

    def scan(self, topic, partition, start, *, limit, stop):
        offsets = [o for o in self.offsets if o >= max(start, self.low)][:limit]
        return self.low, self.high, offsets, offsets[-1] + 1 if offsets else self.high


def test_catalog_preserves_same_event_duplicate_receipts_and_invalid_features(record):
    s3 = VersionedS3()
    first, second = archived(s3, record), archived(s3, record, offset=1)
    store = CatalogStore(s3, "fixture-bucket")
    pointer = CatalogPublisher(store).publish([first, second], [coverage(next_offset=2)], "c")
    reader = CatalogReader(store)
    generations = list(reader.generations(pointer))
    entries = list(reader.entries(generations[0]))
    assert len({e["receipt_id"] for e in entries}) == 2
    assert {e["source_event_id"] for e in entries} == {"same-event"}
    assert {e["received_at"] for e in entries} == {"2026-10-29T10:00:00.000000Z"}
    assert first["receipt_kind"] == "ACCEPTED"
    assert reader.raw(first)  # Exact original compressed bytes; no DQ normalization.


def test_rejected_and_poison_do_not_invent_machine_or_time(record):
    s3 = VersionedS3()
    rejected = archived(s3, replace(record, topic="sensor.rejected"), payload={"request_id": "req"})
    assert rejected["received_at"] is None and rejected["machine_id"] is None
    assert rejected["receipt_kind"] == "REJECTED" and rejected["request_id"] == "req"
    poison = replace(record, offset=3, value=b"not-json")
    S3RecordArchive(s3, "fixture-bucket")(None, poison)
    entry = CatalogStore(s3, "fixture-bucket").receipt(record.topic, 0, 3)
    assert entry["machine_id"] is None and entry["received_at"] is None


def test_restart_after_raw_commit_recovers_from_manifests_not_group_offsets(settings, record):
    s3 = VersionedS3()
    archived(s3, record)
    # Archive happened earlier; a new catalog runtime has no in-memory receipt state.
    runtime = CatalogRuntime(settings, s3, Audit())
    pointer = runtime.sync(threading.Event())
    current, _ = runtime.store.current()
    assert current[0] == pointer
    assert current[1]["partitions"][0]["status"] == "COMPLETE"
    assert len(list(CatalogReader(runtime.store).entries(current[1]))) == 1
    assert CatalogRuntime(settings, s3, Audit()).sync(threading.Event()) == pointer


def test_missing_raw_blocks_prefix_even_if_later_offset_is_archived(settings, record):
    s3 = VersionedS3()
    archived(s3, record, offset=1)
    runtime = CatalogRuntime(settings, s3, Audit([0, 1], high=2))
    runtime.sync(threading.Event())
    state = runtime.store.current()[0][1]["partitions"][0]
    assert state["indexed_next_offset"] == 0 and state["status"] == "LAGGING"
    archived(s3, record)
    runtime.sync(threading.Event())
    assert runtime.store.current()[0][1]["partitions"][0]["indexed_next_offset"] == 2


def test_broker_proven_offset_control_holes_are_not_missing_readings(settings, record):
    s3 = VersionedS3()
    archived(s3, record, offset=5)
    runtime = CatalogRuntime(settings, s3, Audit([5], high=8))
    # Audit proof says offset6–7 contain no visible user record (e.g. control markers).
    runtime.audit.scan = lambda *a, **kw: (0, 8, [5], 8)
    runtime.sync(threading.Event())
    state = runtime.store.current()[0][1]["partitions"][0]
    assert state["status"] == "COMPLETE" and state["indexed_next_offset"] == 8
    assert state["gaps"] == []


def test_retention_loss_stays_unknown_and_rebuild_restores_older_receipts(settings, record):
    s3 = VersionedS3()
    archived(s3, record, offset=0)
    archived(s3, record, offset=5)
    runtime = CatalogRuntime(settings, s3, Audit([5], low=5, high=6))
    runtime.sync(threading.Event())
    assert runtime.store.current()[0][1]["partitions"][0]["status"] == "UNKNOWN"
    assert runtime.rebuild(threading.Event()) == 2
    current, _ = runtime.store.current()
    receipts = {
        e["receipt_id"]
        for g in CatalogReader(runtime.store).generations()
        for e in CatalogReader(runtime.store).entries(g)
    }
    assert receipts == {"sensor.readings:0:0", "sensor.readings:0:5"}
    assert current[1]["partitions"][0]["status"] == "UNKNOWN"


def test_two_publishers_cas_retry_keeps_both_partitions(record):
    s3 = VersionedS3()
    one, two = archived(s3, record), archived(s3, record, partition=1)
    store = CatalogStore(s3, "fixture-bucket")
    publisher = CatalogPublisher(store)
    s3.before_pointer = lambda: publisher.publish([two], [coverage(1)], "c")
    pointer = publisher.publish([one], [coverage(0)], "c")
    current, _ = store.current()
    assert {p["partition"] for p in current[1]["partitions"]} == {0, 1}
    reader = CatalogReader(store)
    assert {e["receipt_id"] for g in reader.generations(pointer) for e in reader.entries(g)} == {
        one["receipt_id"],
        two["receipt_id"],
    }


def test_lost_pointer_ack_reconciles_without_duplicate_generation(record):
    s3 = VersionedS3()
    entry = archived(s3, record)
    s3.lose_ack = True
    store = CatalogStore(s3, "fixture-bucket")
    pointer = CatalogPublisher(store).publish([entry], [coverage()], "c")
    assert len(list(CatalogReader(store).generations(pointer))) == 1


def test_stale_coverage_and_changed_cluster_or_topic_cannot_overwrite(record):
    s3 = VersionedS3()
    store = CatalogStore(s3, "fixture-bucket")
    pub = CatalogPublisher(store)
    pub.publish([archived(s3, record)], [coverage(next_offset=2)], "c")
    pub.publish([], [coverage(next_offset=1)], "c")
    assert store.current()[0][1]["partitions"][0]["indexed_next_offset"] == 2
    with pytest.raises(IdentityConflict):
        pub.publish([], [coverage()], "another-cluster")
    changed = {**coverage(next_offset=3), "topic_id": "recreated-topic"}
    with pytest.raises(IdentityConflict):
        pub.publish([], [changed], "c")


def test_exact_versions_survive_latest_overwrite_and_detect_corruption(record):
    s3 = VersionedS3()
    entry = archived(s3, record)
    store = CatalogStore(s3, "fixture-bucket")
    ref = entry["raw_object_ref"]
    original = store.read_ref(ref, raw=True)
    s3.put_object(Bucket="fixture-bucket", Key=ref["key"], Body=b"latest is different")
    assert store.read_ref(ref, raw=True) == original
    s3.versions[(ref["key"], ref["version_id"])]["body"] = b"corrupt exact version"
    with pytest.raises(IdentityConflict):
        CatalogReader(store).raw(entry)


def test_reference_scope_and_manifest_locator_mismatch_fail_closed(record):
    s3 = VersionedS3()
    entry = archived(s3, record)
    reader = CatalogReader(CatalogStore(s3, "fixture-bucket"))
    other = copy.deepcopy(entry)
    other["raw_object_ref"]["bucket"] = "another-bucket"
    with pytest.raises(IdentityConflict):
        reader.raw(other)
    with pytest.raises(ConfigurationError):
        reader.store.read_ref({**entry["raw_object_ref"], "key": "private/model.joblib"}, raw=True)
    other = {**entry, "offset": 99}
    with pytest.raises(IdentityConflict):
        reader.raw(other)


def test_reader_budget_never_claims_partial_chain_complete(record):
    s3 = VersionedS3()
    store = CatalogStore(s3, "fixture-bucket")
    pub = CatalogPublisher(store)
    pub.publish([archived(s3, record)], [coverage()], "c")
    pub.publish([archived(s3, record, offset=1)], [coverage(next_offset=2)], "c")
    with pytest.raises(ConfigurationError):
        list(CatalogReader(store).generations(max_generations=1))


def test_catalog_checkpoint_not_in_chain_fails_instead_of_reporting_caught_up(record):
    s3 = VersionedS3()
    store = CatalogStore(s3, "fixture-bucket")
    CatalogPublisher(store).publish([archived(s3, record)], [coverage()], "c")
    with pytest.raises(IdentityConflict):
        list(CatalogReader(store).generations(stop_id="a" * 64))


def test_deep_poison_payload_is_kept_without_blocking_catalog(record):
    s3 = VersionedS3()
    poison = replace(record, offset=0, value=b"[" * 2000 + b"0" + b"]" * 2000)
    S3RecordArchive(s3, "fixture-bucket")(None, poison)
    entry = CatalogStore(s3, "fixture-bucket").receipt(record.topic, 0, 0)
    assert entry["machine_id"] is None and entry["received_at"] is None


def test_new_partitions_are_unknown_when_audit_has_not_run(settings):
    stop = threading.Event()
    stop.set()
    runtime = CatalogRuntime(settings, VersionedS3(), Audit())
    runtime.sync(stop)
    state = runtime.store.current()[0][1]["partitions"][0]
    assert state["status"] == "UNKNOWN" and state["captured_log_end_offset"] is None


@pytest.mark.parametrize(
    "override",
    [
        {"STORAGE_CATALOG_PREFIX": "../unsafe"},
        {"STORAGE_CATALOG_PREFIX": "raw-sensor/subdir"},
        {"STORAGE_CATALOG_ENABLED": "true", "S3_ALLOW_UNVERSIONED": "true"},
        {"STORAGE_CATALOG_ENABLED": "true", "STORAGE_ARCHIVE_OUTCOMES": "true"},
        {"STORAGE_CATALOG_BATCH_RECORDS": "101"},
    ],
)
def test_catalog_configuration_rejects_unsafe_profiles(override):
    with pytest.raises(ConfigurationError):
        Settings.from_env(
            {
                "KAFKA_LOCAL_MODE": "true",
                "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
                "S3_RAW_BUCKET": "fixture-bucket",
                **override,
            }
        )


def test_packaged_contract_examples_validate_without_credentials_or_network(monkeypatch):
    monkeypatch.setattr("socket.socket", lambda *a, **kw: pytest.fail("Network called"))
    assert main(["validate-catalog"]) == 0
    with pytest.raises(ConfigurationError):
        validate({**coverage(next_offset=1), "captured_log_end_offset": 2}, "Coverage")


def test_audit_manual_assignment_never_joins_or_commits(settings):
    calls = []

    class Client:
        def get_watermark_offsets(self, *a, **kw):
            return 0, 1

        def assign(self, partitions):
            calls.append(partitions)

        def poll(self, timeout):
            return SimpleNamespace(error=lambda: None, offset=lambda: 0)

        def close(self):
            calls.append("closed")

    def factory(config):
        assert config["enable.auto.commit"] is False
        assert config["enable.auto.offset.store"] is False
        assert config["auto.offset.reset"] == "error"
        return Client()  # No subscribe/commit methods: calling either fails the test.

    audit = KafkaAudit(settings, None, consumer_factory=factory)
    assert audit.scan("sensor.readings", 0, 0, limit=1, stop=threading.Event()) == (0, 1, [0], 1)
    audit.close()
    assert calls[-1] == "closed"
