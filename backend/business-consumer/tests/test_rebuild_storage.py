"""Storage protocol integration: real versioned MinIO, original frames and catalog deltas."""

import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import boto3
import pytest
from backend.shared.projections.generations import bootstrap
from backend.shared.projections.rebuild import RebuildRepository
from backend.shared.projections.sources import RawSource, SourcePending
from botocore.config import Config
from maintai_messaging.archive import S3RecordArchive
from maintai_storage.catalog import CatalogPublisher, CatalogStore

from .test_projections import deliver, event, setup

pytestmark = pytest.mark.integration


@pytest.fixture
def raw_store():
    if os.environ.get("MAINTAI_M8_INTEGRATION") != "1":
        pytest.skip("Enable M8 integration for the isolated MinIO test project")
    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:39000",
        region_name="us-east-1",
        aws_access_key_id="maintai_m6_test",
        aws_secret_access_key="maintai_m6_test_only",
        config=Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 0}),
    )
    bucket = "m8-" + uuid4().hex
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    try:
        yield CatalogStore(client, bucket)
    finally:
        for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket):
            keys = [
                {"Key": o["Key"], "VersionId": o["VersionId"]}
                for o in page.get("Versions", []) + page.get("DeleteMarkers", [])
            ]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        client.delete_bucket(Bucket=bucket)


def prepared(db, store):
    projector = setup(db)
    reading = event("sensor-reading", 2**80)
    deliver(db, projector, reading, 17)
    record = SimpleNamespace(
        topic=reading.topic,
        partition=0,
        offset=17,
        key=reading.key.encode(),
        value=reading.wire_bytes,
        headers=(),
        timestamp_ms=1,
    )
    S3RecordArchive(store.s3, store.bucket)(reading, record)
    entry = store.receipt(reading.topic, 0, 17)
    with db.factory() as connection:
        generation = bootstrap(connection, writers_drained=True)
    repository = RebuildRepository(db.factory)
    identifier = repository.start(
        expected_active=generation,
        operator="fixture",
        source_cluster="fixture-cluster",
        groups={
            "sensor.readings": "business-live-v1",
            "inference.results": "business-prediction-v1",
        },
        deadline=datetime.now(UTC) + timedelta(minutes=5),
        journal_budget=1048576,
    )
    lease = repository.claim("fixture-runner")
    repository.scan_members(lease)
    return repository, identifier, lease, entry


def publish(store, entries, *, cluster="fixture-cluster", end=18):
    return CatalogPublisher(store).publish(
        entries,
        [
            {
                "topic": "sensor.readings",
                "topic_id": "fixture-topic",
                "partition": 0,
                "start_offset": 0,
                "archived_next_offset": end,
                "indexed_next_offset": end,
                "captured_log_end_offset": end,
                "captured_at": datetime.now(UTC).isoformat(),
                "gaps": [],
                "status": "COMPLETE",
            }
        ],
        cluster,
    )


def test_catalog_lag_then_original_frame_and_restart_checkpoint(business_database, raw_store):
    repo, identifier, lease, entry = prepared(business_database, raw_store)
    source = RawSource(repo, raw_store)
    member = repo.pending(lease)[0]
    with pytest.raises(SourcePending):
        source.sync(lease)
    publish(raw_store, [entry])
    # Append more than the SDK's default traversal budget; each call must resume its parent ref.
    for _ in range(101):
        publish(raw_store, [])
    while source.sync(lease):
        source = RawSource(repo, raw_store)
    decoded, ref = source.read(lease, member)
    assert decoded.payload["sequence_number"] == 2**80
    assert ref["entry"]["offset"] == 17
    repo.apply_member(lease, member, decoded, ref)
    repo.verify(identifier)


def test_complete_coverage_without_expected_receipt_is_corruption(business_database, raw_store):
    repo, _, lease, _ = prepared(business_database, raw_store)
    publish(raw_store, [])
    source = RawSource(repo, raw_store)
    while source.sync(lease):
        pass
    with pytest.raises(ValueError, match="missing expected"):
        source.read(lease, repo.pending(lease)[0])


def test_wrong_cluster_never_rebuilds_from_other_environment(business_database, raw_store):
    repo, _, lease, entry = prepared(business_database, raw_store)
    publish(raw_store, [entry], cluster="other-cluster")
    with pytest.raises(ValueError, match="cluster"):
        RawSource(repo, raw_store).sync(lease)
