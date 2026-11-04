"""Versioned raw catalog: immutable deltas, CAS discovery and verified raw refs.

This module never commits Kafka offsets, changes raw objects, or accesses SQL.
Generations are deltas over an exact parent: consumers upsert by receipt_id and
must finish the parent chain before declaring a generation completely indexed.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
from datetime import UTC, datetime
from functools import lru_cache
from importlib.resources import files

from botocore.exceptions import ClientError
from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import ConfigurationError, IdentityConflict

MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
MAX_FRAME_BYTES = 3 * 1024 * 1024
TOPICS = ("sensor.readings", "sensor.rejected")


def now_utc():
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def sha256(body):
    return hashlib.sha256(body).hexdigest()


@lru_cache(maxsize=8)
def validator(kind):
    schema = json.loads(files("maintai_storage").joinpath("schemas/catalog.json").read_text())
    return Draft202012Validator(
        {"$ref": f"#/$defs/{kind}", "$defs": schema["$defs"]},
        format_checker=FormatChecker(),
    )


def validate(document, kind):
    validator(kind).validate(document)
    entries = document["entries"] if kind == "Segment" else ([document] if kind == "Entry" else [])
    for entry in entries:
        identity = f"{entry['topic']}:{entry['partition']}:{entry['offset']}"
        if entry["receipt_id"] != identity:
            raise IdentityConflict("Catalog entry receipt identity differs from locator")
    coverages = (
        document["partitions"]
        if kind == "Generation"
        else ([document] if kind == "Coverage" else [])
    )
    for state in coverages:
        if (
            not state["start_offset"]
            <= state["indexed_next_offset"]
            <= state["archived_next_offset"]
        ):
            raise ConfigurationError("Invalid catalog coverage prefix ordering")
        if state["status"] == "COMPLETE" and (
            state["gaps"]
            or state["captured_log_end_offset"] is None
            or state["indexed_next_offset"] < state["captured_log_end_offset"]
        ):
            raise ConfigurationError("Catalog COMPLETE has no verified caught-up prefix")


def _missing(exc):
    return isinstance(exc, ClientError) and exc.response["Error"]["Code"] in {
        "NoSuchKey",
        "NoSuchVersion",
        "404",
        "NotFound",
    }


def _conflict(exc):
    return isinstance(exc, ClientError) and exc.response["Error"]["Code"] in {
        "PreconditionFailed",
        "412",
        "ConditionalRequestConflict",
        "409",
    }


def timestamp(value):
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def text_or_none(value):
    # Metadata, not raw body: bounded identifiers only. No payload or provider logs.
    return value if isinstance(value, str) and 0 < len(value) <= 256 else None


class CatalogStore:
    """All reads are bounded; all references use exact versions and SHA-256."""

    def __init__(self, s3, bucket, *, raw_prefix="raw-sensor", prefix="raw-catalog/v1"):
        self.s3, self.bucket, self.raw_prefix, self.prefix = s3, bucket, raw_prefix, prefix

    def read(self, key, *, version=None, limit=MAX_DOCUMENT_BYTES):
        args = {"Bucket": self.bucket, "Key": key}
        if version:
            args["VersionId"] = version
        response = self.s3.get_object(**args)
        stream = response["Body"]
        try:
            body = stream.read(limit + 1)
        finally:
            stream.close()
        if len(body) > limit:
            raise ConfigurationError("Catalog/raw object exceeds bounded reader size")
        return body, response

    def read_ref(self, reference, *, raw=False):
        validate(reference, "ObjectRef")
        allowed = self.raw_prefix + "/" if raw else self.prefix + "/"
        if reference["bucket"] != self.bucket or not reference["key"].startswith(allowed):
            raise ConfigurationError("Object reference outside configured catalog/raw scope")
        body, _ = self.read(reference["key"], version=reference["version_id"])
        if sha256(body) != reference["sha256"]:
            raise IdentityConflict("Catalog/raw checksum differs from immutable reference")
        return body

    def immutable(self, key, document):
        body = canonical_json(document)
        if len(body) > MAX_DOCUMENT_BYTES:
            raise ConfigurationError("Catalog document exceeds publication size budget")
        try:
            response = self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                IfNoneMatch="*",
                ChecksumSHA256=base64.b64encode(bytes.fromhex(sha256(body))).decode(),
            )
        except ClientError as exc:
            if not _conflict(exc):
                raise
            existing, response = self.read(key)
            if existing != body:
                raise IdentityConflict("Immutable catalog key contains different bytes") from exc
        version = response.get("VersionId")
        if not version or version == "null":
            raise ConfigurationError("M6 catalog requires versioned objects")
        reference = {
            "bucket": self.bucket,
            "key": key,
            "version_id": version,
            "sha256": sha256(body),
        }
        # Verify the persisted version, not an ETag or an upload acknowledgement alone.
        self.read_ref(reference)
        return reference

    def current(self):
        try:
            body, response = self.read(self.prefix + "/current.json")
        except ClientError as exc:
            if _missing(exc):
                return None, None
            raise
        pointer = json.loads(body)
        validate(pointer, "Pointer")
        generation = json.loads(self.read_ref(pointer["generation_ref"]))
        validate(generation, "Generation")
        if generation["generation_id"] != pointer["generation_id"]:
            raise IdentityConflict("Catalog pointer generation identity mismatch")
        return (pointer, generation), response["ETag"]

    def receipt(self, topic, partition, offset):
        if topic not in TOPICS or partition < 0 or offset < 0:
            raise ConfigurationError("Unsupported raw receipt locator")
        key = (
            f"{self.raw_prefix}/topic={topic}/partition={partition:06d}"
            f"/offset={offset:020d}.manifest.json"
        )
        body, response = self.read(key)
        manifest = json.loads(body)
        default_contracts().validator("archive/raw-manifest.schema.json").validate(manifest)
        if (
            manifest["topic"],
            manifest["partition"],
            manifest["first_offset"],
            manifest["last_offset"],
            manifest["record_count"],
        ) != (topic, partition, offset, offset, 1):
            raise IdentityConflict("Raw manifest locator differs from requested receipt")
        reference = manifest["object"]
        packed = self.read_ref(reference, raw=True)
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            unpacked = stream.read(MAX_FRAME_BYTES + 1)
        if len(unpacked) > MAX_FRAME_BYTES:
            raise ConfigurationError("Raw frame exceeds decompression budget")
        frame = json.loads(unpacked)
        if (frame["topic"], frame["partition"], frame["offset"]) != (topic, partition, offset):
            raise IdentityConflict("Raw frame and manifest identity differ")
        version = response.get("VersionId")
        if not version or version == "null":
            raise ConfigurationError("M6 catalog requires versioned raw manifests")
        payload = {}
        try:
            value = json.loads(base64.b64decode(frame["value_base64"], validate=True))
            if isinstance(value, dict):
                payload = value
        except (ValueError, TypeError, RecursionError):
            pass  # A poison receipt remains indexed without invented machine/time fields.
        entry = {
            "catalog_schema_version": "1.0.0",
            "receipt_id": f"{topic}:{partition}:{offset}",
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "source_event_id": text_or_none(payload.get("event_id")),
            "machine_id": text_or_none(payload.get("machine_id")),
            "received_at": timestamp(payload.get("received_at")),
            "server_accepted_at": timestamp(payload.get("server_accepted_at")),
            "source_identity": text_or_none(payload.get("source_identity")),
            "request_id": text_or_none(payload.get("request_id")),
            "receipt_kind": "ACCEPTED" if topic == "sensor.readings" else "REJECTED",
            "raw_object_ref": reference,
            "raw_manifest_ref": {
                "bucket": self.bucket,
                "key": key,
                "version_id": version,
                "sha256": sha256(body),
            },
            "event_locator": 0,
        }
        validate(entry, "Entry")
        return entry


class CatalogPublisher:
    def __init__(self, store):
        self.store = store

    def publish(self, entries, updates, cluster_id, *, attempts=5):
        """CAS append; stale coverage never overwrites a newer prefix or drops a partition."""
        for _ in range(attempts):
            current, etag = self.store.current()
            pointer, old = current if current else (None, None)
            if old and old["source_cluster_id"] != cluster_id:
                raise IdentityConflict("Catalog source Kafka cluster changed")
            states = {(p["topic"], p["partition"]): p for p in old["partitions"]} if old else {}
            changed = False
            for update in updates:
                validate(update, "Coverage")
                coordinate = (update["topic"], update["partition"])
                prior = states.get(coordinate)
                if prior:
                    if prior["topic_id"] != update["topic_id"]:
                        raise IdentityConflict("Kafka topic identity changed; new catalog required")
                    if update["start_offset"] != prior["start_offset"]:
                        raise IdentityConflict("Catalog start offset cannot be rewritten")
                    if update["indexed_next_offset"] < prior["indexed_next_offset"]:
                        continue
                    # Evidence of loss is sticky; a stale publisher cannot clear an earlier gap.
                    update = dict(update)
                    update["gaps"] = list(
                        {canonical_json(g): g for g in prior["gaps"] + update["gaps"]}.values()
                    )
                    if update["gaps"]:
                        update["status"] = "UNKNOWN"
                    if update == prior:
                        continue
                states[coordinate] = update
                changed = True
            if not changed and not entries:
                return pointer
            refs = []
            if entries:
                # Bounded deltas reuse the parent chain instead of copying all history.
                ordered = sorted(entries, key=lambda e: (e["topic"], e["partition"], e["offset"]))
                for index in range(0, len(ordered), 100):
                    segment = {"schema_version": "1.0.0", "entries": ordered[index : index + 100]}
                    validate(segment, "Segment")
                    segment_id = sha256(canonical_json(segment))
                    ref = self.store.immutable(
                        f"{self.store.prefix}/segments/{segment_id}.json", segment
                    )
                    refs.append(ref)
            generation = {
                "schema_version": "1.0.0",
                "generation_id": "pending",
                "published_at": now_utc(),
                "source_cluster_id": cluster_id,
                "parent_ref": pointer["generation_ref"] if pointer else None,
                "segments": refs,
                "partitions": [states[key] for key in sorted(states)],
            }
            generation["generation_id"] = sha256(canonical_json(generation))
            validate(generation, "Generation")
            key = f"{self.store.prefix}/generations/{generation['generation_id']}.json"
            reference = self.store.immutable(key, generation)
            published = {
                "schema_version": "1.0.0",
                "generation_id": generation["generation_id"],
                "generation_ref": reference,
            }
            validate(published, "Pointer")
            condition = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
            try:
                self.store.s3.put_object(
                    Bucket=self.store.bucket,
                    Key=self.store.prefix + "/current.json",
                    Body=canonical_json(published),
                    ContentType="application/json",
                    **condition,
                )
                return published
            except Exception as exc:
                # PUT may have succeeded before the connection failed: reconcile identity first.
                actual, _ = self.store.current()
                if actual and actual[0]["generation_id"] == published["generation_id"]:
                    return published
                if not _conflict(exc):
                    raise
        raise TimeoutError("Catalog pointer CAS budget exhausted")


class CatalogReader:
    """Small shared helper for M6. SQL indexing and business auth remain its caller's job."""

    def __init__(self, store):
        self.store = store

    def generations(self, pointer=None, *, stop_id=None, max_generations=100):
        """Yield newest-first immutable deltas; caller persists progress and dedupes receipts.

        On budget exhaustion, fail rather than representing a partial chain as complete.
        A sync consumer can walk bounded windows using generation.parent_ref as its cursor.
        """
        if pointer is None:
            current, _ = self.store.current()
            if current is None:
                raise FileNotFoundError("Catalog has not been published")
            pointer = current[0]
        validate(pointer, "Pointer")
        ref, seen = pointer["generation_ref"], set()
        for _ in range(max_generations):
            generation = json.loads(self.store.read_ref(ref))
            validate(generation, "Generation")
            identity = generation["generation_id"]
            if not seen and identity != pointer["generation_id"]:
                raise IdentityConflict("Pinned pointer generation identity mismatch")
            if identity == stop_id:
                return
            if identity in seen:
                raise IdentityConflict("Catalog parent chain contains a cycle")
            seen.add(identity)
            yield generation
            ref = generation["parent_ref"]
            if ref is None:
                if stop_id is not None:
                    raise IdentityConflict(
                        "Catalog checkpoint is not an ancestor of this generation"
                    )
                return
        raise ConfigurationError("Catalog generation budget exceeded; resume sync from parent_ref")

    def entries(self, generation):
        validate(generation, "Generation")
        for ref in generation["segments"]:
            segment = json.loads(self.store.read_ref(ref))
            validate(segment, "Segment")
            yield from segment["entries"]

    def raw(self, entry):
        validate(entry, "Entry")
        # Re-verify exact referenced bytes; never expose an arbitrary caller-provided URL.
        manifest = json.loads(self.store.read_ref(entry["raw_manifest_ref"], raw=True))
        default_contracts().validator("archive/raw-manifest.schema.json").validate(manifest)
        if manifest["object"] != entry["raw_object_ref"] or (
            manifest["topic"],
            manifest["partition"],
            manifest["first_offset"],
        ) != (entry["topic"], entry["partition"], entry["offset"]):
            raise IdentityConflict("Entry raw reference differs from its manifest")
        return self.store.read_ref(entry["raw_object_ref"], raw=True)
