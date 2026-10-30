"""Kafka + versioned MinIO + installed runtime, enabled explicitly by CI/local harness."""

import json
import os
import subprocess
import threading
import time
import uuid
from dataclasses import replace
from urllib.parse import urlsplit

import pytest
from maintai_messaging.admin import create_local_topics
from maintai_messaging.contracts import ContractRegistry
from maintai_messaging.producer import KafkaPublisher

from maintai_storage.catalog import CatalogReader, CatalogStore
from maintai_storage.runtime import Runtime
from maintai_storage.settings import Settings

pytestmark = pytest.mark.integration


def test_real_catalog_restart_rebuild_and_installed_container(monkeypatch):
    if os.environ.get("MAINTAI_STORAGE_INTEGRATION") != "1":
        pytest.skip("Explicit isolated Kafka/MinIO integration is not enabled")
    endpoint = os.environ.get("STORAGE_TEST_S3_ENDPOINT", "")
    bootstrap = os.environ.get("STORAGE_TEST_KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    assert urlsplit(endpoint).hostname in {"127.0.0.1", "localhost"}
    assert bootstrap.startswith(("127.0.0.1:", "localhost:")) and "," not in bootstrap
    suffix = uuid.uuid4().hex
    for key in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "storage-local-fixture")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "storage-local-fixture-password")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    settings = replace(
        Settings.from_env(
            {
                "KAFKA_LOCAL_MODE": "true",
                "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
                "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
                "KAFKA_GROUP_PREFIX": f"catalog-{suffix}-",
                "S3_RAW_BUCKET": f"catalog-it-{suffix}",
                "S3_ENDPOINT_URL": endpoint,
                "S3_ADDRESSING_STYLE": "path",
                "AWS_REGION": "us-east-1",
                "STORAGE_HTTP_HOST": "127.0.0.1",
                "STORAGE_CATALOG_INTERVAL_SECONDS": "0.1",
                "STORAGE_CATALOG_POLL_SECONDS": "0.5",
            }
        ),
        port=0,
    )
    client = settings.create_s3_client()
    create_local_topics(settings.kafka)
    client.create_bucket(Bucket=settings.bucket)
    client.put_bucket_versioning(
        Bucket=settings.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    registry = ContractRegistry()
    payload = json.loads((registry.directory / "examples/sensor-reading.json").read_text())
    payload["event_id"] = f"catalog-event-{suffix}"
    deliveries = []
    with KafkaPublisher(settings.kafka, "ingestion") as publisher:
        for _ in range(2):
            deliveries.append(publisher.publish("sensor.readings", payload))
        rejected = json.loads((registry.directory / "examples/rejected.json").read_text())
        deliveries.append(publisher.publish("sensor.rejected", rejected))

    def wait_for(predicate, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.1)
        current, _ = CatalogStore(client, settings.bucket).current()
        diagnostic = {
            "errors": [type(error).__name__ for error in errors],
            "coverage": current[1]["partitions"] if current else None,
            "runtime_health": [runtime.state.snapshot(runtime.settings) for runtime in running],
        }
        pytest.fail("Local catalog fixture deadline: " + json.dumps(diagnostic, sort_keys=True))

    # Stage1: raw offsets may commit without any catalog existing yet.
    stop = threading.Event()
    errors = []
    running = []

    def serve(config):
        try:
            runtime = Runtime(config)
            running.append(runtime)
            assert runtime.run(stop) == 0
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=serve, args=(settings,))
    thread.start()
    store = CatalogStore(client, settings.bucket)

    def raw_exists():
        page = client.list_objects_v2(Bucket=settings.bucket, Prefix="raw-sensor/")
        return len([o for o in page.get("Contents", []) if o["Key"].endswith("manifest.json")]) >= 3

    try:
        wait_for(raw_exists)
        assert store.current()[0] is None
        stop.set()
        thread.join(45)
        assert not thread.is_alive() and not errors
        # Stage2: restart with catalog enabled; recover earlier durable raw manifests.
        stop = threading.Event()
        thread = threading.Thread(target=serve, args=(replace(settings, catalog_enabled=True),))
        thread.start()

        def catalog_contains():
            current, _ = store.current()
            if current is None:
                return False
            reader = CatalogReader(store)
            entries = {e["receipt_id"]: e for g in reader.generations() for e in reader.entries(g)}
            return all(f"{d.topic}:{d.partition}:{d.offset}" in entries for d in deliveries)

        wait_for(catalog_contains)
        reader = CatalogReader(store)
        first, _ = store.current()
        entries = {e["receipt_id"]: e for g in reader.generations() for e in reader.entries(g)}
        assert (
            len([e for e in entries.values() if e["source_event_id"] == payload["event_id"]]) == 2
        )
        for delivery in deliveries:
            entry = entries[f"{delivery.topic}:{delivery.partition}:{delivery.offset}"]
            assert reader.raw(entry)
        stop.set()
        thread.join(45)
        assert not thread.is_alive() and not errors
        # Previously pinned generation remains readable after later publisher progress.
        assert list(reader.generations(first[0]))
        # Rebuild from S3 manifests is explicit, independent of group committed offsets.
        from maintai_storage.catalog_runtime import CatalogRuntime

        assert CatalogRuntime(settings, client, None).rebuild(stop=threading.Event()) >= 3
        image = os.environ.get("STORAGE_TEST_IMAGE")
        if image:
            # Installed image has its packaged schemas/examples without source bind mounts/network.
            subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--read-only",
                    "--tmpfs",
                    "/tmp",
                    image,
                    "validate-catalog",
                ],
                check=True,
                timeout=30,
            )
    finally:
        stop.set()
        thread.join(45)
        for page in client.get_paginator("list_object_versions").paginate(Bucket=settings.bucket):
            for item in page.get("Versions", []) + page.get("DeleteMarkers", []):
                client.delete_object(
                    Bucket=settings.bucket, Key=item["Key"], VersionId=item["VersionId"]
                )
        client.delete_bucket(Bucket=settings.bucket)
        client.close()
