"""Isolated real Storage runtime for Processing/browser integration tests."""

import threading
import time
from dataclasses import replace
from uuid import uuid4

import boto3
from botocore.config import Config
from maintai_storage.catalog import CatalogReader, CatalogStore
from maintai_storage.runtime import Runtime
from maintai_storage.settings import Settings


class StorageProvider:
    """Run installed Storage consumer/audit/publisher; never publish a test catalog."""

    def __init__(self, bootstrap):
        from processing.shared.history_meta.storage import Objects

        self.client = boto3.client(
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
        suffix = uuid4().hex
        self.raw_bucket, self.export_bucket = (
            "m6-native-raw-" + suffix,
            "m6-native-export-" + suffix,
        )
        self.stop, self.errors = threading.Event(), []
        self.thread = None
        self.store = CatalogStore(self.client, self.raw_bucket)
        for bucket in (self.raw_bucket, self.export_bucket):
            self.client.create_bucket(Bucket=bucket)
            self.client.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
            )
        self.objects = Objects(
            self.client, raw_bucket=self.raw_bucket, export_bucket=self.export_bucket
        )
        self.settings = replace(
            Settings.from_env(
                {
                    "KAFKA_LOCAL_MODE": "true",
                    "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
                    "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
                    "KAFKA_GROUP_PREFIX": "m6-native-" + suffix + "-",
                    "S3_RAW_BUCKET": self.raw_bucket,
                    "STORAGE_HTTP_HOST": "127.0.0.1",
                    "STORAGE_CATALOG_ENABLED": "true",
                    "STORAGE_CATALOG_INTERVAL_SECONDS": "0.1",
                    "STORAGE_CATALOG_POLL_SECONDS": "0.1",
                }
            ),
            port=0,
        )
        # Explicit S3 client avoids changing ambient credential configuration.
        self.runtime = Runtime(self.settings, s3=self.client)
        self.thread = threading.Thread(target=self._serve, name="m6-real-storage")
        self.thread.start()

    def _serve(self):
        try:
            if self.runtime.run(self.stop) != 0:
                self.errors.append("Storage runtime failed")
        except Exception as exc:
            self.errors.append(type(exc).__name__)

    def publish(self):
        """Wait for real audit coverage; this method performs no catalog writes."""
        return self.wait()

    def archive(self, event, record):
        """Wait for a Kafka receipt to be archived/indexed by Storage itself."""
        return self.wait(f"{record.topic}:{record.partition}:{record.offset}")

    def wait(self, receipt=None):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.errors or not self.thread.is_alive():
                raise RuntimeError("Storage provider stopped: " + str(self.errors))
            current, _ = self.store.current()
            if current:
                pointer, generation = current
                states = [p for p in generation["partitions"] if p["topic"] == "sensor.readings"]
                complete = len(states) == 12 and all(
                    p["status"] == "COMPLETE" and not p["gaps"] for p in states
                )
                if complete:
                    if receipt is None:
                        return pointer["generation_id"]
                    reader = CatalogReader(self.store)
                    if any(
                        e["receipt_id"] == receipt
                        for g in reader.generations(max_generations=1000)
                        for e in reader.entries(g)
                    ):
                        return pointer["generation_id"]
            time.sleep(0.1)
        raise TimeoutError("Real Storage coverage did not reach the requested receipt")

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(45)
            if self.thread.is_alive():
                raise RuntimeError("Storage runtime did not stop; retaining test buckets")
        for bucket in (self.raw_bucket, self.export_bucket):
            for page in self.client.get_paginator("list_object_versions").paginate(Bucket=bucket):
                values = [
                    {"Key": o["Key"], "VersionId": o["VersionId"]}
                    for o in page.get("Versions", []) + page.get("DeleteMarkers", [])
                ]
                if values:
                    self.client.delete_objects(Bucket=bucket, Delete={"Objects": values})
            self.client.delete_bucket(Bucket=bucket)
        self.client.close()
