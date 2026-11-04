"""Checkpointed native delta imports. Network calls never hold database locks."""

import time
from uuid import uuid4

from jsonschema import ValidationError
from maintai_messaging.contracts import canonical_json, strict_json
from maintai_messaging.errors import ConfigurationError, IdentityConflict
from maintai_storage.catalog import validate
from psycopg import sql

from .security import HistoryError
from .snapshots import timestamp


def incompatible():
    raise IdentityConflict("Storage catalog identity or immutable history changed")


def partitions(manifest):
    validate(manifest, "Generation")
    result = {}
    for item in manifest["partitions"]:
        key = (item["topic"], item["partition"])
        if key in result:
            incompatible()
        result[key] = item
    return result


def compatible(new, old):
    if new["source_cluster_id"] != old["source_cluster_id"]:
        incompatible()
    states = partitions(new)
    for key, prior in partitions(old).items():
        current = states.get(key)
        if (
            current is None
            or any(current[field] != prior[field] for field in ("topic_id", "start_offset"))
            or any(
                current[field] < prior[field]
                for field in ("indexed_next_offset", "archived_next_offset")
            )
        ):
            incompatible()


class NativeIndex:
    def __init__(self, meta, store, reader, source_key):
        self.meta, self.store, self.reader, self.key = meta, store, reader, source_key

    def execute(self, connection, statement, tables, args=()):
        return connection.execute(
            sql.SQL(statement).format(*(self.meta.table(table) for table in tables)), args
        )

    def owned(self, connection, owner, fence):
        row = self.execute(
            connection,
            "SELECT * FROM {} WHERE source_key=%s "
            "AND lease_owner=%s AND fence=%s "
            "AND lease_until>clock_timestamp() FOR UPDATE",
            ["native_catalog_heads"],
            (self.key, owner, fence),
        ).fetchone()
        if row is None:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        self.execute(
            connection,
            "UPDATE {} SET lease_until=clock_timestamp()+interval '30 seconds' WHERE source_key=%s",
            ["native_catalog_heads"],
            (self.key,),
        )
        return row

    def claim(self, pointer, generation, *, expected_fence=None):
        owner = uuid4().hex
        with self.meta.factory() as connection:
            self.execute(
                connection,
                "INSERT INTO {}(source_key) VALUES(%s) ON CONFLICT DO NOTHING",
                ["native_catalog_heads"],
                (self.key,),
            )
            head = self.execute(
                connection,
                "SELECT *,lease_until>clock_timestamp() AS leased "
                "FROM {} WHERE source_key=%s FOR UPDATE",
                ["native_catalog_heads"],
                (self.key,),
            ).fetchone()
            if head["leased"] or (expected_fence is not None and head["fence"] != expected_fence):
                return None
            active = None
            if head["active_id"]:
                active = self.execute(
                    connection,
                    "SELECT * FROM {} WHERE source_key=%s AND generation_id=%s",
                    ["native_catalog_imports"],
                    (self.key, head["active_id"]),
                ).fetchone()
                compatible(generation, strict_json(bytes(active["manifest"])))
                if generation["generation_id"] == active["generation_id"]:
                    if canonical_json(pointer["generation_ref"]) != bytes(active["reference"]):
                        incompatible()
                    self.execute(
                        connection,
                        "UPDATE {} SET blocked=FALSE WHERE source_key=%s",
                        ["native_catalog_heads"],
                        (self.key,),
                    )
                    return generation["generation_id"]
            pending = self.execute(
                connection,
                "SELECT * FROM {} WHERE source_key=%s AND state='BUILDING'",
                ["native_catalog_imports"],
                (self.key,),
            ).fetchone()
            if pending is not None:
                compatible(generation, strict_json(bytes(pending["manifest"])))
            if pending is None:
                sequence = head["next_sequence"] + 1
                self.execute(
                    connection,
                    "INSERT INTO {}(source_key,generation_id,import_sequence,"
                    "manifest,reference,anchor_id,next_ref,state) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,'BUILDING')",
                    ["native_catalog_imports"],
                    (
                        self.key,
                        generation["generation_id"],
                        sequence,
                        canonical_json(generation),
                        canonical_json(pointer["generation_ref"]),
                        head["active_id"],
                        canonical_json(pointer["generation_ref"]),
                    ),
                )
                self.execute(
                    connection,
                    "UPDATE {} SET next_sequence=%s WHERE source_key=%s",
                    ["native_catalog_heads"],
                    (sequence, self.key),
                )
                pending = self.execute(
                    connection,
                    "SELECT * FROM {} WHERE source_key=%s AND state='BUILDING'",
                    ["native_catalog_imports"],
                    (self.key,),
                ).fetchone()
            fence = head["fence"] + 1
            self.execute(
                connection,
                "UPDATE {} SET lease_owner=%s,fence=%s,"
                "lease_until=clock_timestamp()+interval '30 seconds' WHERE source_key=%s",
                ["native_catalog_heads"],
                (owner, fence, self.key),
            )
        return owner, fence, pending, active

    def sync(self):
        # Bind even validation failures to the state observed BEFORE network I/O.
        # A slow response must not disable a head completed by a newer lease.
        with self.meta.factory() as connection:
            observed = self.execute(
                connection,
                "SELECT fence FROM {} WHERE source_key=%s",
                ["native_catalog_heads"],
                (self.key,),
            ).fetchone()
        fence = observed["fence"] if observed else 0
        owner = None
        try:
            current, _ = self.store.current()
            if current is None:
                raise HistoryError(503, "HISTORY_UNAVAILABLE")
            pointer, generation = current
            partitions(generation)
            claim = self.claim(pointer, generation, expected_fence=fence)
            if claim is None or isinstance(claim, str):
                return claim
            owner, fence, pending, active = claim
            return self.walk(owner, fence, pending, active)
        except (ConfigurationError, IdentityConflict, ValidationError, ValueError):
            self.block(fence)
            raise
        finally:
            if owner is not None:
                with self.meta.factory() as connection:
                    self.execute(
                        connection,
                        "UPDATE {} SET lease_until=NULL,lease_owner=NULL "
                        "WHERE source_key=%s AND lease_owner=%s AND fence=%s",
                        ["native_catalog_heads"],
                        (self.key, owner, fence),
                    )

    def walk(self, owner, fence, pending, active):
        target = strict_json(bytes(pending["manifest"]))
        reference = strict_json(bytes(pending["next_ref"])) if pending["next_ref"] else None
        segment_index = pending["segment_index"]
        target_id = pending["generation_id"]
        anchor_ref = bytes(active["reference"]) if active else None
        deadline, segments = time.monotonic() + 10, 0
        for _ in range(100):
            if reference is None:
                if pending["anchor_id"] is not None:
                    incompatible()
                return self.activate(owner, fence, pending)
            if anchor_ref == canonical_json(reference):
                return self.activate(owner, fence, pending)
            doc = strict_json(self.store.read_ref(reference))
            compatible(target, doc)
            identifier = doc["generation_id"]
            if (
                reference == strict_json(bytes(pending["reference"]))
                and identifier != target_id
                or identifier == pending["anchor_id"]
            ):
                incompatible()
            with self.meta.factory() as connection:
                self.owned(connection, owner, fence)
                prior = self.execute(
                    connection,
                    "SELECT * FROM {} WHERE source_key=%s AND target_id=%s AND generation_id=%s",
                    ["native_catalog_visited"],
                    (self.key, target_id, identifier),
                ).fetchone()
                if prior and (
                    prior["finished"] or bytes(prior["reference"]) != canonical_json(reference)
                ):
                    incompatible()
                self.execute(
                    connection,
                    "INSERT INTO {}(source_key,target_id,generation_id,reference) "
                    "VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    ["native_catalog_visited"],
                    (self.key, target_id, identifier, canonical_json(reference)),
                )
            while segment_index < len(doc["segments"]):
                if segments >= 100 or time.monotonic() >= deadline:
                    return None
                # Reuse the provider's schema/reference checks, one bounded segment at a time.
                entries = list(
                    self.reader.entries({**doc, "segments": [doc["segments"][segment_index]]})
                )
                with self.meta.factory() as connection:
                    self.owned(connection, owner, fence)
                    for entry in entries:
                        self.insert(connection, entry, pending, doc)
                    segment_index += 1
                    self.execute(
                        connection,
                        "UPDATE {} SET segment_index=%s WHERE source_key=%s AND generation_id=%s",
                        ["native_catalog_imports"],
                        (segment_index, self.key, target_id),
                    )
                segments += 1
            with self.meta.factory() as connection:
                self.owned(connection, owner, fence)
                self.execute(
                    connection,
                    "UPDATE {} SET finished=TRUE WHERE source_key=%s "
                    "AND target_id=%s AND generation_id=%s",
                    ["native_catalog_visited"],
                    (self.key, target_id, identifier),
                )
                reference, segment_index = doc["parent_ref"], 0
                self.execute(
                    connection,
                    "UPDATE {} SET next_ref=%s,segment_index=0 "
                    "WHERE source_key=%s AND generation_id=%s",
                    ["native_catalog_imports"],
                    (canonical_json(reference) if reference else None, self.key, target_id),
                )
            if time.monotonic() >= deadline:
                return None
        return None

    def insert(self, connection, entry, pending, generation):
        state = partitions(generation).get((entry["topic"], entry["partition"]))
        if (
            state is None
            or not state["start_offset"] <= entry["offset"] < state["indexed_next_offset"]
        ):
            incompatible()
        raw = canonical_json(entry)
        machine = entry["machine_id"]
        if machine is not None and "\x00" in machine:
            machine = None
        result = self.execute(
            connection,
            "INSERT INTO {}(source_key,receipt_id,first_sequence,"
            "machine_id,received_at,topic,partition,source_offset,payload) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING 1",
            ["native_catalog_receipts"],
            (
                self.key,
                entry["receipt_id"],
                pending["import_sequence"],
                machine,
                timestamp(entry["received_at"]) if entry["received_at"] else None,
                entry["topic"],
                entry["partition"],
                entry["offset"],
                raw,
            ),
        ).fetchone()
        if result is None:
            old = self.execute(
                connection,
                "SELECT payload FROM {} WHERE source_key=%s AND receipt_id=%s",
                ["native_catalog_receipts"],
                (self.key, entry["receipt_id"]),
            ).fetchone()
            if bytes(old["payload"]) != raw:
                incompatible()

    def activate(self, owner, fence, pending):
        with self.meta.factory() as connection:
            head = self.owned(connection, owner, fence)
            if head["active_id"] != pending["anchor_id"]:
                incompatible()
            self.execute(
                connection,
                "UPDATE {} SET state='READY' WHERE source_key=%s AND generation_id=%s",
                ["native_catalog_imports"],
                (self.key, pending["generation_id"]),
            )
            self.execute(
                connection,
                "UPDATE {} SET active_id=%s,blocked=FALSE WHERE source_key=%s",
                ["native_catalog_heads"],
                (pending["generation_id"], self.key),
            )
        return pending["generation_id"]

    def block(self, fence):
        with self.meta.factory() as connection:
            self.execute(
                connection,
                "UPDATE {} SET blocked=TRUE WHERE source_key=%s AND fence=%s",
                ["native_catalog_heads"],
                (self.key, fence),
            )

    def active(self):
        with self.meta.factory() as connection:
            return self.execute(
                connection,
                "SELECT i.* FROM {} h JOIN {} i ON "
                "i.source_key=h.source_key AND i.generation_id=h.active_id "
                "WHERE h.source_key=%s AND NOT h.blocked",
                ["native_catalog_heads", "native_catalog_imports"],
                (self.key,),
            ).fetchone()
