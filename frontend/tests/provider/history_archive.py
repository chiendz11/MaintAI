"""Disposable catalog publisher fixture, not a Storage provider implementation."""

import hashlib
from datetime import UTC, datetime
from uuid import uuid4

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, TopicPartition
from maintai_messaging.archive import S3RecordArchive
from maintai_messaging.contracts import canonical_json

from processing.shared.history_meta.storage import Objects


class ArchiveFixture:
    def __init__(self, bootstrap):
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
        self.raw_bucket, self.export_bucket = (
            "m6-browser-raw-" + uuid4().hex,
            "m6-browser-export-" + uuid4().hex,
        )
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "m6-catalog-" + uuid4().hex,
                "enable.auto.commit": False,
            }
        )
        self.starts = {
            p: self.consumer.get_watermark_offsets(TopicPartition("sensor.readings", p), timeout=5)[
                1
            ]
            for p in range(12)
        }
        self.entries, self.parent = [], None
        for bucket in (self.raw_bucket, self.export_bucket):
            self.client.create_bucket(Bucket=bucket)
            self.client.put_bucket_versioning(
                Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
            )
        self.objects = Objects(
            self.client, raw_bucket=self.raw_bucket, export_bucket=self.export_bucket
        )

    def put(self, key, value):
        raw = canonical_json(value)
        response = self.client.put_object(Bucket=self.raw_bucket, Key=key, Body=raw)
        return {
            "bucket": self.raw_bucket,
            "key": key,
            "version_id": response["VersionId"],
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
        }

    def reference(self, key):
        response = self.client.get_object(Bucket=self.raw_bucket, Key=key)
        try:
            raw = response["Body"].read()
        finally:
            response["Body"].close()
        return {
            "bucket": self.raw_bucket,
            "key": key,
            "version_id": response["VersionId"],
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
        }

    def archive(self, event, record):
        S3RecordArchive(self.client, self.raw_bucket)(event, record)
        prefix = (
            f"raw-sensor/topic=sensor.readings/partition={record.partition:06d}"
            f"/offset={record.offset:020d}"
        )
        self.entries.append(
            {
                "catalog_schema_version": "1.0.0",
                "receipt_id": f"sensor.readings:{record.partition}:{record.offset}",
                "topic": "sensor.readings",
                "partition": record.partition,
                "offset": record.offset,
                "source_event_id": event.message_id,
                "machine_id": event.payload["machine_id"],
                "received_at": event.payload["received_at"],
                "server_accepted_at": event.payload["server_accepted_at"],
                "source_identity": None,
                "request_id": None,
                "receipt_kind": "ACCEPTED",
                "raw_object_ref": self.reference(prefix + ".jsonl.gz"),
                "raw_manifest_ref": self.reference(prefix + ".manifest.json"),
                "event_locator": 0,
            }
        )
        return self.publish()

    def publish(self):
        identifier = uuid4().hex
        now = datetime.now(UTC).isoformat()
        segments, partitions = [], []
        for p in range(12):
            entries = [entry for entry in self.entries if entry["partition"] == p]
            start = self.starts[p]
            frontier = max([start] + [entry["offset"] + 1 for entry in entries])
            captured = self.consumer.get_watermark_offsets(
                TopicPartition("sensor.readings", p), timeout=5
            )[1]
            if sorted(entry["offset"] for entry in entries) != list(range(start, frontier)):
                raise RuntimeError("Fixture capture has a gap; cannot publish complete coverage")
            if captured != frontier:
                raise RuntimeError(
                    "Fixture did not archive every receipt through captured frontier"
                )
            partitions.append(
                {
                    "topic": "sensor.readings",
                    "partition": p,
                    "start_offset": start,
                    "archived_next_offset": frontier,
                    "indexed_next_offset": frontier,
                    "captured_log_end_offset": captured,
                    "captured_at": now,
                    "gaps": [],
                    "status": "COMPLETE",
                }
            )
            if entries:
                segment_id = identifier + "-" + str(p)
                ref = self.put(
                    "raw-catalog/v1/segments/" + segment_id + ".json",
                    {
                        "catalog_schema_version": "1.0.0",
                        "segment_id": segment_id,
                        "entries": entries,
                    },
                )
                times = [entry["received_at"] for entry in entries]
                segments.append(
                    {
                        "segment_id": segment_id,
                        "topic": "sensor.readings",
                        "partition": p,
                        "min_received_at": min(times),
                        "max_received_at": max(times),
                        "entry_count": len(entries),
                        "object_ref": ref,
                    }
                )
        generation = {
            "catalog_schema_version": "1.0.0",
            "generation_id": identifier,
            "published_at": now,
            "parent_ref": self.parent,
            "segments": segments,
            "partitions": partitions,
        }
        ref = self.put("raw-catalog/v1/generations/" + identifier + ".json", generation)
        self.put(
            "raw-catalog/v1/current.json",
            {"catalog_schema_version": "1.0.0", "generation_id": identifier, "generation_ref": ref},
        )
        self.parent = ref
        return identifier

    def close(self):
        self.consumer.close()
        for bucket in (self.raw_bucket, self.export_bucket):
            for page in self.client.get_paginator("list_object_versions").paginate(Bucket=bucket):
                values = [
                    {"Key": item["Key"], "VersionId": item["VersionId"]}
                    for item in page.get("Versions", []) + page.get("DeleteMarkers", [])
                ]
                if values:
                    self.client.delete_objects(Bucket=bucket, Delete={"Objects": values})
            self.client.delete_bucket(Bucket=bucket)
        self.client.close()
