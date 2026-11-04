"""Real MinIO fixtures exercise exact versions, catalog receipts and corruption."""

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from maintai_messaging.archive import S3RecordArchive
from maintai_messaging.contracts import canonical_json
from processing.shared.history_meta.catalog import Catalog
from processing.shared.history_meta.persistence import Metadata, metadata_schema
from processing.shared.history_meta.schemas import Schemas
from processing.shared.history_meta.security import HistoryError
from processing.shared.history_meta.storage import Objects
from processing.worker.tests.test_worker_transaction import source

ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.integration


@pytest.fixture
def objects():
    if os.environ.get("MAINTAI_M6_INTEGRATION") != "1":
        pytest.skip("Explicit local M6 MinIO test opt-in required")
    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:39000",
        aws_access_key_id="maintai_m6_test",
        aws_secret_access_key="maintai_m6_test_only",
        region_name="us-east-1",
        config=Config(
            connect_timeout=2,
            read_timeout=3,
            retries={"total_max_attempts": 1},
            s3={"addressing_style": "path"},
        ),
    )
    bucket = "m6-" + uuid4().hex
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    try:
        yield client, bucket, Objects(client, raw_bucket=bucket, export_bucket=bucket)
    finally:
        for page in client.get_paginator("list_object_versions").paginate(Bucket=bucket):
            keys = [
                {"Key": obj["Key"], "VersionId": obj["VersionId"]}
                for obj in page.get("Versions", []) + page.get("DeleteMarkers", [])
            ]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        client.delete_bucket(Bucket=bucket)


def put(client, bucket, key, value):
    raw = canonical_json(value)
    version = client.put_object(Bucket=bucket, Key=key, Body=raw)["VersionId"]
    return {
        "bucket": bucket,
        "key": key,
        "version_id": version,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
    }


def provider_catalog(client, bucket):
    from maintai_storage.catalog import CatalogStore

    store = CatalogStore(client, bucket)
    archive = S3RecordArchive(client, bucket)
    entries = []
    for offset in (1, 2):
        event, record = source(offset=offset)
        archive(event, record)
        entries.append(store.receipt("sensor.readings", 0, offset))
    segment = {"schema_version": "1.0.0", "entries": entries}
    ref = store.immutable("raw-catalog/v1/segments/fixture.json", segment)
    now = datetime.now(UTC).isoformat()
    partitions = [
        dict(
            topic="sensor.readings",
            topic_id="native-topic",
            partition=p,
            start_offset=1 if p == 0 else 0,
            archived_next_offset=3 if p == 0 else 0,
            indexed_next_offset=3 if p == 0 else 0,
            captured_log_end_offset=3 if p == 0 else 0,
            captured_at=now,
            gaps=[],
            status="COMPLETE",
        )
        for p in range(12)
    ]
    generation = dict(
        schema_version="1.0.0",
        generation_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        source_cluster_id="native-cluster",
        published_at=now,
        parent_ref=None,
        segments=[ref],
        partitions=partitions,
    )
    ref = store.immutable("raw-catalog/v1/generations/fixture.json", generation)
    pointer = dict(
        schema_version="1.0.0",
        generation_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        generation_ref=ref,
    )
    client.put_object(
        Bucket=bucket, Key="raw-catalog/v1/current.json", Body=canonical_json(pointer)
    )
    return entries, pointer


def test_catalog_preserves_duplicate_receipts_and_exact_bytes(migrated_database, objects):
    client, bucket, store = objects
    entries, _ = provider_catalog(client, bucket)
    catalog = Catalog(
        Metadata(migrated_database.factory, metadata_schema(migrated_database.schema)),
        store,
        Schemas(ROOT / "contracts/v1.3/schemas"),
    )
    assert catalog.sync() == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    coverage = catalog.coverage({"machine_id": "M-001"})
    assert coverage["available_through"] is None
    query = {
        "machine_id": entries[0]["machine_id"],
        "from": "2026-10-01T00:00:00Z",
        "to": "2026-10-31T00:00:00Z",
        "catalog_generation": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    }
    rows = list(catalog.records(query))
    assert [r["receipt_id"] for r in rows] == ["sensor.readings:0:2", "sensor.readings:0:1"]
    assert all(r["validation_status"] == "UNVERIFIED" for r in rows)
    assert rows[0]["features_raw"] == source()[0].payload["features"]


def test_object_checksum_or_version_mismatch_never_returns_success(objects):
    client, bucket, store = objects
    ref = put(client, bucket, "raw-catalog/v1/fixture.json", {"fixture": True})
    assert json.loads(store.read(ref))["fixture"] is True
    with pytest.raises(HistoryError):
        store.read({**ref, "sha256": "0" * 64})
    with pytest.raises(HistoryError):
        store.read({**ref, "version_id": "null"})


def test_catalog_network_does_not_hold_metadata_publication_lock(migrated_database, objects):
    client, bucket, store = objects
    provider_catalog(client, bucket)
    namespace = metadata_schema(migrated_database.schema)
    catalog = Catalog(
        Metadata(migrated_database.factory, namespace),
        store,
        Schemas(ROOT / "contracts/v1.3/schemas"),
    )
    original = catalog.store.read_ref

    def read(reference, **kwargs):
        with migrated_database.factory() as connection:
            row = connection.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS available",
                (namespace + ":catalog",),
            ).fetchone()
            assert row["available"], "Catalog holds a SQL publication lock during S3 reads"
        return original(reference, **kwargs)

    catalog.store.read_ref = read
    catalog.sync()


def test_catalog_rejects_replacement_of_published_receipts(migrated_database, objects):
    from maintai_storage.catalog import CatalogPublisher, CatalogStore

    client, bucket, objects = objects
    entries, _ = provider_catalog(client, bucket)
    catalog = Catalog(
        Metadata(migrated_database.factory, metadata_schema(migrated_database.schema)),
        objects,
        Schemas(ROOT / "contracts/v1.3/schemas"),
    )
    catalog.sync()
    store = CatalogStore(client, bucket)
    current, _ = store.current()
    entries[0]["raw_object_ref"]["sha256"] = "0" * 64
    CatalogPublisher(store).publish(entries, current[1]["partitions"], "native-cluster")
    with pytest.raises(HistoryError):
        catalog.sync()
    with pytest.raises(HistoryError):
        catalog.coverage({})
