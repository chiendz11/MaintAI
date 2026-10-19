"""Immutable S3 micro-batches: object + verified manifest before a durable receipt."""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import os

from .config import KafkaSettings, flag
from .consumer import DurableReceipt
from .contracts import canonical_json, default_contracts
from .errors import ConfigurationError, IdentityConflict


def deterministic_gzip(value: bytes) -> bytes:
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=output, filename="", mode="wb", mtime=0) as stream:
        stream.write(value)
    return output.getvalue()


class S3RecordArchive:
    """A micro-batch has one Kafka receipt, so replay keys survive rebalances.

    This deliberately favors exact offset identity over time-window-dependent
    batch membership. Archive batching can be optimized behind this same receipt
    interface after measuring object rates. No payloads are put in logs.
    """

    def __init__(self, client, bucket: str, *, prefix="raw-sensor", allow_unversioned=False):
        if not bucket or not prefix.strip("/"):
            raise ConfigurationError("Archive bucket and prefix are required")
        self.client, self.bucket = client, bucket
        self.prefix, self.allow_unversioned = prefix.strip("/"), allow_unversioned

    @classmethod
    def from_env(cls, settings: KafkaSettings):
        import boto3
        from botocore.config import Config

        endpoint = os.environ.get("S3_ENDPOINT_URL") or None
        if endpoint and not endpoint.startswith("https://") and not settings.local_mode:
            raise ConfigurationError("Non-TLS S3 endpoints are local-only")
        allow = flag(os.environ.get("S3_ALLOW_UNVERSIONED", "false"))
        if allow and not settings.local_mode:
            raise ConfigurationError("Unversioned archives are local-only")
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            config=Config(
                connect_timeout=5,
                read_timeout=15,
                retries={"total_max_attempts": 3, "mode": "standard"},
                s3={"addressing_style": "path"},
            ),
        )
        return cls(
            client,
            os.environ.get("S3_RAW_BUCKET", ""),
            prefix=os.environ.get("S3_RAW_PREFIX", "raw-sensor"),
            allow_unversioned=allow,
        )

    def _write_once(self, key, body, content_type):
        digest = hashlib.sha256(body).hexdigest()
        try:
            response = self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
                IfNoneMatch="*",
                ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode(),
                Metadata={"sha256": digest},
            )
        except Exception as exc:
            error = getattr(exc, "response", {})
            code = error.get("Error", {}).get("Code", "")
            if code not in {"PreconditionFailed", "412"}:
                raise
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            stream = response["Body"]
            try:
                existing = stream.read(len(body) + 1)
            finally:
                stream.close()
            if existing != body:
                raise IdentityConflict(
                    "Archive coordinate already contains different bytes"
                ) from exc
        version = response.get("VersionId")
        if not self.allow_unversioned and (not version or version == "null"):
            raise ConfigurationError(
                "Archive requires bucket versioning and an immutable VersionId"
            )
        return {"bucket": self.bucket, "key": key, "version_id": version, "sha256": digest}

    def __call__(self, _event, record):
        if len(record.value or b"") > 1048576:
            raise ConfigurationError("Archive record exceeds the reviewed Kafka message bound")

        def encoded(value):
            return None if value is None else base64.b64encode(value).decode()

        document = {
            "archive_schema_version": "1.0.0",
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "source_timestamp_ms": record.timestamp_ms,
            "key_base64": encoded(record.key),
            "value_base64": encoded(record.value),
            "headers": [[key, encoded(value)] for key, value in record.headers],
        }
        body = deterministic_gzip(canonical_json(document) + b"\n")
        base = (
            f"{self.prefix}/topic={record.topic}"
            f"/partition={record.partition:06d}/offset={record.offset:020d}"
        )
        reference = self._write_once(base + ".jsonl.gz", body, "application/gzip")
        manifest = {
            "schema_version": "1.0.0",
            "topic": record.topic,
            "partition": record.partition,
            "first_offset": record.offset,
            "last_offset": record.offset,
            "record_count": 1,
            "format": "jsonl.gz",
            "object": reference,
        }
        default_contracts().validator("archive/raw-manifest.schema.json").validate(manifest)
        self._write_once(base + ".manifest.json", canonical_json(manifest), "application/json")
        return DurableReceipt.for_record(record)
