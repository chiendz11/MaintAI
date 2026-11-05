"""Bounded original-source adapters; no ML calls or reconstructed ingestion payloads."""

import asyncio
import base64
import gzip
import hashlib
import io
import json
from pathlib import Path
from urllib.parse import quote

import httpx
from maintai_messaging.contracts import canonical_json, default_contracts, strict_json
from maintai_storage.catalog import CatalogReader, validate

MAX_WIRE = 1024 * 1024


class SourcePending(RuntimeError):
    """A source may catch up before the operator's immutable deadline."""


def verify(event, member):
    if (event.topic, event.message_id, event.identity_sha256, event.payload["schema_version"]) != (
        member["topic"],
        member["message_id"],
        member["identity_sha256"],
        member["schema_version"],
    ):
        raise ValueError("Original source identity differs from inbox membership")
    return event


class OutcomeSource:
    def __init__(self, origin, token_file, *, transport=None):
        self.origin, self.token_file, self.transport = (
            origin.rstrip("/"),
            Path(token_file),
            transport,
        )

    async def _read(self, member):
        async with asyncio.timeout(3):
            with self.token_file.open("rb") as stream:
                raw = stream.read(16385)
            token = raw.decode("ascii").strip()
            if (
                len(raw) > 16377
                or not token
                or not token.isprintable()
                or any(c.isspace() for c in token)
            ):
                raise ValueError("Invalid service credential file")
            segment = quote(member["message_id"], safe="")
            if member["message_id"] in {".", ".."}:
                segment = segment.replace(".", "%2E")
            async with httpx.AsyncClient(
                transport=self.transport, trust_env=False, timeout=2
            ) as client:
                async with client.stream(
                    "GET",
                    self.origin + "/api/v1/internal/predictions/" + segment,
                    follow_redirects=False,
                    headers={"Authorization": "Bearer " + token, "Accept-Encoding": "identity"},
                ) as response:
                    if response.status_code in {408, 429} or response.status_code >= 500:
                        raise SourcePending("Processing original source temporarily unavailable")
                    if response.status_code != 200:
                        raise ValueError("Processing original source unavailable or unauthorized")
                    if response.headers.get("Content-Encoding", "identity") != "identity":
                        raise ValueError("Unexpected encoded original source")
                    body = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        if len(body) + len(chunk) > MAX_WIRE:
                            raise ValueError("Original source exceeds byte budget")
                        body.extend(chunk)
                    value = strict_json(bytes(body))
            if value.get("result_id") != member["message_id"] or not isinstance(
                value.get("outcome"), dict
            ):
                raise ValueError("Processing original source identity differs")
            wire = canonical_json(value["outcome"])
            event = verify(default_contracts().decode("inference.results", wire), member)
            return event, {
                "kind": "processing-original",
                "result_id": event.message_id,
                "sha256": hashlib.sha256(wire).hexdigest(),
            }

    def read(self, member):
        try:
            return asyncio.run(self._read(member))
        except (httpx.HTTPError, TimeoutError) as exc:
            raise SourcePending("Processing original source temporarily unavailable") from exc


class RawSource:
    def __init__(self, repository, store):
        self.repository, self.store = repository, store
        self.reader = CatalogReader(store)

    def sync(self, lease):
        """One immutable delta per call; progress survives restart and long chains."""
        repo = self.repository
        with repo.factory() as connection, connection.transaction():
            row = repo._locked(connection, lease["job_id"], lease=lease)
            state = json.loads(bytes(row["checkpoint"]))
            source_cluster = json.loads(bytes(row["input"]))["source_cluster"]
        ref = state.get("catalog_next")
        if ref is None:
            current, _ = self.store.current()
            if current is None:
                raise SourcePending("Storage catalog has not been published")
            pointer, generation = current
            if pointer["generation_id"] == state.get("catalog_head") and state.get("catalog_done"):
                return False
            ref = pointer["generation_ref"]
            state.update(
                catalog_head=pointer["generation_id"],
                catalog_done=False,
                catalog_coverage=generation["partitions"],
            )
        # SDK validates immutable object SHA, allowed bucket/prefix and pinned version.
        generation = json.loads(self.store.read_ref(ref))
        validate(generation, "Generation")
        if generation["source_cluster_id"] != source_cluster:
            raise ValueError("Storage source cluster differs from approved job input")
        coverage = {(p["topic"], p["partition"]): p for p in state["catalog_coverage"]}
        for partition in generation["partitions"]:
            head = coverage.get((partition["topic"], partition["partition"]))
            if head is None or (head["topic_id"], head["start_offset"]) != (
                partition["topic_id"],
                partition["start_offset"],
            ):
                raise ValueError("Catalog topic identity or coverage lineage changed")
        entries = self.reader.entries(generation)
        with repo.factory() as connection, connection.transaction():
            repo._locked(connection, lease["job_id"], lease=lease)
            seen = connection.execute(
                "SELECT walk_id FROM projection_catalog_seen WHERE job_id=%s AND generation_id=%s",
                (row["job_id"], generation["generation_id"]),
            ).fetchone()
        if seen:
            # Reaching a previous completed chain terminates a newly appended prefix.
            if seen["walk_id"] == state["catalog_head"] or not state.get(
                "catalog_previous_complete"
            ):
                raise ValueError("Catalog chain contains a cycle")
            state.update(catalog_done=True, catalog_next=None)
        else:
            # Iterate verified segments outside a SQL transaction. A segment contains <=100 entries.
            for entry in entries:
                if entry["topic"] != "sensor.readings":
                    continue
                with repo.factory() as connection, connection.transaction():
                    repo._locked(connection, lease["job_id"], lease=lease)
                    connection.execute(
                        """INSERT INTO projection_raw_entries
                        (job_id,topic,source_partition,source_offset,entry,catalog_ref)
                        SELECT %s,%s,%s,%s,%s,%s WHERE EXISTS(SELECT 1 FROM projection_members
                            WHERE job_id=%s AND topic=%s AND source_partition=%s
                            AND source_offset=%s)
                        ON CONFLICT DO NOTHING""",
                        (
                            row["job_id"],
                            entry["topic"],
                            entry["partition"],
                            entry["offset"],
                            canonical_json(entry),
                            canonical_json(ref),
                            row["job_id"],
                            entry["topic"],
                            entry["partition"],
                            entry["offset"],
                        ),
                    )
            state["catalog_next"] = generation["parent_ref"]
            if generation["parent_ref"] is None:
                state.update(catalog_done=True, catalog_previous_complete=True)
        with repo.factory() as connection, connection.transaction():
            actual = repo._locked(connection, lease["job_id"], lease=lease)
            checkpoint = json.loads(bytes(actual["checkpoint"]))
            checkpoint.update({k: v for k, v in state.items() if k.startswith("catalog_")})
            connection.execute(
                "INSERT INTO projection_catalog_seen VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                (row["job_id"], generation["generation_id"], state["catalog_head"]),
            )
            connection.execute(
                "UPDATE projection_rebuild_jobs SET checkpoint=%s WHERE job_id=%s",
                (canonical_json(checkpoint), row["job_id"]),
            )
        return not checkpoint.get("catalog_done", False)

    def read(self, lease, member):
        with self.repository.factory() as connection:
            row = self.repository._locked(connection, lease["job_id"], lease=lease)
            state = json.loads(bytes(row["checkpoint"]))
            entry = connection.execute(
                """SELECT * FROM projection_raw_entries WHERE job_id=%s AND topic=%s
                AND source_partition=%s AND source_offset=%s""",
                (
                    row["job_id"],
                    member["topic"],
                    member["source_partition"],
                    member["source_offset"],
                ),
            ).fetchone()
        if not state.get("catalog_done"):
            raise SourcePending("Storage catalog traversal is not complete")
        if entry is None:
            coverage = next(
                (
                    p
                    for p in state.get("catalog_coverage", [])
                    if (p["topic"], p["partition"]) == (member["topic"], member["source_partition"])
                ),
                None,
            )
            if (
                coverage
                and coverage["start_offset"]
                <= member["source_offset"]
                < coverage["indexed_next_offset"]
            ):
                raise ValueError("Published coverage is missing expected exact raw receipt")
            raise SourcePending("Storage catalog has not indexed expected receipt")
        catalog_entry = json.loads(bytes(entry["entry"]))
        packed = self.reader.raw(catalog_entry)
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            frame_body = stream.read(3 * MAX_WIRE + 1)
        if len(frame_body) > 3 * MAX_WIRE:
            raise ValueError("Raw frame exceeds decompression budget")
        frame = strict_json(frame_body)
        if (frame["topic"], frame["partition"], frame["offset"]) != (
            member["topic"],
            member["source_partition"],
            member["source_offset"],
        ):
            raise ValueError("Raw frame locator differs from expected receipt")
        wire = base64.b64decode(frame["value_base64"], validate=True)
        if len(wire) > MAX_WIRE:
            raise ValueError("Raw Kafka frame exceeds byte budget")
        event = verify(default_contracts().decode("sensor.readings", wire), member)
        if base64.b64decode(frame["key_base64"], validate=True) != event.key.encode():
            raise ValueError("Original raw Kafka key differs from machine identity")
        return event, {
            "kind": "storage-original",
            "entry": catalog_entry,
            "catalog_ref": json.loads(bytes(entry["catalog_ref"])),
        }
