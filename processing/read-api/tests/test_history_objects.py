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
    archive = S3RecordArchive(client, bucket)
    entries = []
    for offset in (1, 2):
        event, record = source(offset=offset)
        archive(event, record)
        prefix = f"raw-sensor/topic=sensor.readings/partition=000000/offset={offset:020d}"
        refs = []
        for suffix in (".jsonl.gz", ".manifest.json"):
            response = client.get_object(Bucket=bucket, Key=prefix + suffix)
            raw = response["Body"].read()
            response["Body"].close()
            refs.append(
                {
                    "bucket": bucket,
                    "key": prefix + suffix,
                    "version_id": response["VersionId"],
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "size_bytes": len(raw),
                }
            )
        entries.append(
            {
                "catalog_schema_version": "1.0.0",
                "receipt_id": f"sensor.readings:0:{offset}",
                "topic": "sensor.readings",
                "partition": 0,
                "offset": offset,
                "source_event_id": event.message_id,
                "machine_id": event.payload["machine_id"],
                "received_at": event.payload["received_at"],
                "server_accepted_at": event.payload["server_accepted_at"],
                "source_identity": None,
                "request_id": None,
                "receipt_kind": "ACCEPTED",
                "raw_object_ref": refs[0],
                "raw_manifest_ref": refs[1],
                "event_locator": 0,
            }
        )
    segment = {
        "catalog_schema_version": "1.0.0",
        "segment_id": "segment-fixture",
        "entries": entries,
    }
    ref = put(client, bucket, "raw-catalog/v1/segments/fixture.json", segment)
    now = datetime.now(UTC).isoformat()
    partitions = [
        {
            "topic": "sensor.readings",
            "partition": p,
            "start_offset": 1 if p == 0 else 0,
            "archived_next_offset": 3 if p == 0 else 0,
            "indexed_next_offset": 3 if p == 0 else 0,
            "captured_log_end_offset": 3 if p == 0 else 0,
            "captured_at": now,
            "gaps": [],
            "status": "COMPLETE",
        }
        for p in range(12)
    ]
    generation = {
        "catalog_schema_version": "1.0.0",
        "generation_id": "generation-fixture",
        "published_at": now,
        "parent_ref": None,
        "segments": [
            {
                "segment_id": "segment-fixture",
                "topic": "sensor.readings",
                "partition": 0,
                "min_received_at": entries[0]["received_at"],
                "max_received_at": entries[0]["received_at"],
                "entry_count": 2,
                "object_ref": ref,
            }
        ],
        "partitions": partitions,
    }
    ref = put(client, bucket, "raw-catalog/v1/generations/fixture.json", generation)
    pointer = {
        "catalog_schema_version": "1.0.0",
        "generation_id": "generation-fixture",
        "generation_ref": ref,
    }
    put(client, bucket, "raw-catalog/v1/current.json", pointer)
    return entries, pointer


def test_catalog_preserves_duplicate_receipts_and_exact_bytes(migrated_database, objects):
    client, bucket, store = objects
    entries, _ = provider_catalog(client, bucket)
    catalog = Catalog(
        Metadata(migrated_database.factory, metadata_schema(migrated_database.schema)),
        store,
        Schemas(ROOT / "contracts/v1.3/schemas"),
    )
    assert catalog.sync() == "generation-fixture"
    coverage = catalog.coverage({"machine_id": "M-001"})
    assert coverage["available_through"] is None
    query = {
        "machine_id": entries[0]["machine_id"],
        "from": "2026-10-01T00:00:00Z",
        "to": "2026-10-31T00:00:00Z",
        "catalog_generation": "generation-fixture",
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
    original = store.read

    def read(reference, **kwargs):
        with migrated_database.factory() as connection:
            row = connection.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS available",
                (namespace + ":catalog",),
            ).fetchone()
            assert row["available"], "Catalog holds a SQL publication lock during S3 reads"
        return original(reference, **kwargs)

    store.read = read
    catalog = Catalog(
        Metadata(migrated_database.factory, namespace),
        store,
        Schemas(ROOT / "contracts/v1.3/schemas"),
    )
    catalog.sync()


@pytest.mark.parametrize("mutation", ["omit", "replace"])
def test_catalog_rejects_loss_or_replacement_of_published_receipts(
    migrated_database, objects, mutation
):
    from datetime import timedelta

    from maintai_messaging.contracts import strict_json
    from processing.shared.history_meta.snapshots import timestamp

    client, bucket, store = objects
    _, pointer = provider_catalog(client, bucket)
    catalog = Catalog(
        Metadata(migrated_database.factory, metadata_schema(migrated_database.schema)),
        store,
        Schemas(ROOT / "contracts/v1.3/schemas"),
    )
    catalog.sync()
    generation = strict_json(store.read(pointer["generation_ref"]))
    ref = generation["segments"][0]
    segment = strict_json(store.read(ref["object_ref"]))
    if mutation == "omit":
        segment["entries"] = segment["entries"][1:]
    else:
        segment["entries"][0]["raw_object_ref"]["sha256"] = "0" * 64
    segment["segment_id"] = "changed-segment"
    ref.update(
        segment_id=segment["segment_id"],
        entry_count=len(segment["entries"]),
        object_ref=put(client, bucket, "raw-catalog/v1/segments/changed.json", segment),
    )
    generation.update(
        generation_id="changed-generation",
        parent_ref=pointer["generation_ref"],
        published_at=(
            timestamp(generation["published_at"]) + timedelta(microseconds=1)
        ).isoformat(),
    )
    pointer.update(
        generation_id=generation["generation_id"],
        generation_ref=put(client, bucket, "raw-catalog/v1/generations/changed.json", generation),
    )
    put(client, bucket, "raw-catalog/v1/current.json", pointer)
    with pytest.raises(HistoryError):
        catalog.sync()
    assert catalog.coverage({})["catalog_generation"] == "generation-fixture"
