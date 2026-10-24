"""Opt-in isolated loopback Kafka + MinIO test; never targets AWS or shared buckets."""

import base64
import gzip
import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import replace
from urllib.parse import urlsplit

import pytest
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, TopicPartition
from maintai_messaging.admin import create_local_topics
from maintai_messaging.contracts import ContractRegistry
from maintai_messaging.producer import KafkaPublisher

from maintai_storage.runtime import Runtime
from maintai_storage.settings import Settings

pytestmark = pytest.mark.integration


def test_real_kafka_receipts_reach_versioned_s3_before_offsets(monkeypatch):
    if os.environ.get("MAINTAI_STORAGE_INTEGRATION") != "1":
        pytest.skip("Explicit isolated Kafka/MinIO integration is not enabled")
    endpoint = os.environ.get("STORAGE_TEST_S3_ENDPOINT", "")
    bootstrap = os.environ.get("STORAGE_TEST_KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    if urlsplit(endpoint).hostname not in {"127.0.0.1", "localhost"}:
        pytest.fail("Integration endpoint must be an isolated loopback S3 fixture")
    if not bootstrap.startswith(("127.0.0.1:", "localhost:")) or "," in bootstrap:
        pytest.fail("Integration broker must be an isolated loopback Kafka fixture")
    suffix = uuid.uuid4().hex
    settings = replace(
        Settings.from_env(
            {
                "KAFKA_LOCAL_MODE": "true",
                "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
                "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
                "KAFKA_GROUP_PREFIX": f"storage-it-{suffix}-",
                "S3_RAW_BUCKET": f"storage-it-{suffix}",
                "S3_ENDPOINT_URL": endpoint,
                "AWS_REGION": "us-east-1",
                "S3_ADDRESSING_STYLE": "path",
                "STORAGE_HTTP_HOST": "127.0.0.1",
                "STORAGE_ARCHIVE_OUTCOMES": "true",
            }
        ),
        port=0,
    )
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    # Explicitly local test credentials only; never fall back to an AWS profile or IRSA.
    monkeypatch.setenv(
        "AWS_ACCESS_KEY_ID", os.environ.get("STORAGE_TEST_S3_ACCESS_KEY", "minioadmin")
    )
    monkeypatch.setenv(
        "AWS_SECRET_ACCESS_KEY", os.environ.get("STORAGE_TEST_S3_SECRET_KEY", "minioadmin")
    )
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    client = settings.create_s3_client()
    registry = ContractRegistry()
    create_local_topics(settings.kafka, contracts=registry)
    client.create_bucket(Bucket=settings.bucket)
    client.put_bucket_versioning(
        Bucket=settings.bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    stop = threading.Event()
    codes, errors = [], []
    runtime = Runtime(settings)

    def serve():
        try:
            codes.append(runtime.run(stop))
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=serve)
    try:
        deliveries = []
        for role, topic, name in (
            ("ingestion", "sensor.readings", "sensor-reading"),
            ("ingestion", "sensor.rejected", "rejected"),
            ("outbox", "inference.results", "invalid"),
        ):
            payload = json.loads((registry.directory / "examples" / (name + ".json")).read_text())
            with KafkaPublisher(settings.kafka, role, contracts=registry) as publisher:
                deliveries.append(
                    (publisher.publish(topic, payload), registry.encode(topic, payload))
                )
        thread.start()
        deadline = time.monotonic() + 60
        verified = set()
        while time.monotonic() < deadline:
            assert not errors and not codes, "Runtime exited before archiving fixtures"
            for delivery, event in deliveries:
                if delivery.topic in verified:
                    continue
                key = (
                    f"{settings.prefix}/topic={delivery.topic}/partition={delivery.partition:06d}"
                    f"/offset={delivery.offset:020d}.manifest.json"
                )
                try:
                    response = client.get_object(Bucket=settings.bucket, Key=key)
                except ClientError as exc:
                    if exc.response["Error"]["Code"] in {"NoSuchKey", "404"}:
                        continue
                    raise
                manifest = json.loads(response["Body"].read())
                response["Body"].close()
                reference = manifest["object"]
                obj = client.get_object(
                    Bucket=settings.bucket, Key=reference["key"], VersionId=reference["version_id"]
                )
                raw = obj["Body"].read()
                obj["Body"].close()
                assert hashlib.sha256(raw).hexdigest() == reference["sha256"]
                assert (
                    base64.b64decode(json.loads(gzip.decompress(raw))["value_base64"])
                    == event.wire_bytes
                )
                profile = next(
                    profile
                    for role, profile in registry.topology["profiles"].items()
                    if role in settings.roles and profile["topic"] == delivery.topic
                )
                observer = Consumer(settings.kafka.consumer_config(profile["group_id"]))
                try:
                    position = observer.committed(
                        [TopicPartition(delivery.topic, delivery.partition)],
                        timeout=5,
                    )[0]
                finally:
                    observer.close()
                if position.offset > delivery.offset:
                    verified.add(delivery.topic)
            if len(verified) == 3:
                break
            time.sleep(0.1)
        assert len(verified) == 3
    finally:
        stop.set()
        if thread.ident is not None:
            thread.join(30)
            assert not thread.is_alive()
        # Only the unique fixture bucket is deleted. Runtime code never creates/deletes buckets.
        for page in client.get_paginator("list_object_versions").paginate(Bucket=settings.bucket):
            for entry in page.get("Versions", []) + page.get("DeleteMarkers", []):
                client.delete_object(
                    Bucket=settings.bucket, Key=entry["Key"], VersionId=entry["VersionId"]
                )
        client.delete_bucket(Bucket=settings.bucket)
        client.close()
    assert errors == [] and codes == [0]
