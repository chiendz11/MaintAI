"""Read/index the Storage-owned protocol; never publish or rewrite raw objects."""

import base64
import gzip
import io
import tempfile
from datetime import UTC, datetime

from maintai_messaging.contracts import canonical_json, default_contracts, strict_json
from psycopg import sql

from .security import HistoryError, digest
from .snapshots import timestamp


class Catalog:
    def __init__(self, metadata, objects, schemas, *, processing_link=None):
        self.meta, self.objects, self.schemas = metadata, objects, schemas
        self.processing_link = processing_link

    def sync(self):
        if self.objects is None or self.meta is None:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        pointer = self.schemas.validate("Pointer", strict_json(self.objects.pointer()))
        generation = self.schemas.validate(
            "Generation", strict_json(self.objects.read(pointer["generation_ref"]))
        )
        if pointer["generation_id"] != generation["generation_id"]:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        partitions = {}
        for partition in generation["partitions"]:
            key = (partition["topic"], partition["partition"])
            if (
                key in partitions
                or not partition["start_offset"]
                <= partition["indexed_next_offset"]
                <= partition["archived_next_offset"]
                or (partition["status"] == "COMPLETE" and partition["gaps"])
            ):
                raise HistoryError(503, "HISTORY_UNAVAILABLE")
            partitions[key] = partition
        identifier = generation["generation_id"]
        # Download and validate before holding the publication transaction/lock.
        with tempfile.SpooledTemporaryFile(max_size=1024 * 1024) as staged:
            total, total_bytes = 0, 0
            for reference in generation["segments"]:
                partition = partitions.get((reference["topic"], reference["partition"]))
                if partition is None:
                    raise HistoryError(503, "HISTORY_UNAVAILABLE")
                segment = self.schemas.validate(
                    "Segment", strict_json(self.objects.read(reference["object_ref"]))
                )
                if (
                    segment["segment_id"] != reference["segment_id"]
                    or len(segment["entries"]) != reference["entry_count"]
                ):
                    raise HistoryError(503, "HISTORY_UNAVAILABLE")
                for entry in segment["entries"]:
                    expected = f"{entry['topic']}:{entry['partition']}:{entry['offset']}"
                    if (
                        (entry["topic"], entry["partition"])
                        != (reference["topic"], reference["partition"])
                        or entry["receipt_id"] != expected
                        or not partition["start_offset"]
                        <= entry["offset"]
                        < partition["indexed_next_offset"]
                    ):
                        raise HistoryError(503, "HISTORY_UNAVAILABLE")
                    raw = canonical_json(entry) + b"\n"
                    total += 1
                    total_bytes += len(raw)
                    if total > 1000000 or total_bytes > 512 * 1024 * 1024:
                        raise HistoryError(503, "QUERY_TOO_LARGE")
                    staged.write(raw)
            staged.seek(0)
            with self.meta.factory() as connection, connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))", (self.meta.schema + ":catalog",)
                )
                existing = connection.execute(
                    sql.SQL("SELECT * FROM {} WHERE generation_id=%s").format(
                        self.meta.table("raw_catalog_sync_state")
                    ),
                    (identifier,),
                ).fetchone()
                if existing is not None:
                    if (
                        digest(strict_json(bytes(existing["manifest"]))) != digest(generation)
                        or existing["state"] != "ACTIVE"
                    ):
                        raise HistoryError(503, "HISTORY_UNAVAILABLE")
                    return identifier
                active = connection.execute(
                    sql.SQL("SELECT manifest FROM {} WHERE state='ACTIVE'").format(
                        self.meta.table("raw_catalog_sync_state")
                    )
                ).fetchone()
                if active:
                    previous = strict_json(bytes(active["manifest"]))
                    if timestamp(generation["published_at"]) <= timestamp(previous["published_at"]):
                        raise HistoryError(503, "HISTORY_UNAVAILABLE")
                    for old in previous["partitions"]:
                        new = partitions.get((old["topic"], old["partition"]))
                        if (
                            new is None
                            or old["start_offset"] != new["start_offset"]
                            or new["indexed_next_offset"] < old["indexed_next_offset"]
                        ):
                            raise HistoryError(503, "HISTORY_UNAVAILABLE")
                connection.execute(
                    sql.SQL(
                        "INSERT INTO {}(generation_id,manifest,state) VALUES(%s,%s,'BUILDING')"
                    ).format(self.meta.table("raw_catalog_sync_state")),
                    (identifier, canonical_json(generation)),
                )
                for raw in staged:
                    entry = strict_json(raw)
                    machine = entry["machine_id"]
                    if machine is not None and "\x00" in machine:
                        machine = None
                    connection.execute(
                        sql.SQL("""INSERT INTO {}(generation_id,receipt_id,
                        machine_id,received_at,topic,partition,source_offset,payload)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""").format(
                            self.meta.table("raw_catalog_entries")
                        ),
                        (
                            identifier,
                            entry["receipt_id"],
                            machine,
                            None
                            if entry["received_at"] is None
                            else timestamp(entry["received_at"]),
                            entry["topic"],
                            entry["partition"],
                            entry["offset"],
                            canonical_json(entry),
                        ),
                    )
                if (
                    active
                    and connection.execute(
                        sql.SQL(
                            "SELECT 1 FROM {} old_entry LEFT JOIN {} new_entry "
                            "ON new_entry.generation_id=%s "
                            "AND new_entry.receipt_id=old_entry.receipt_id "
                            "WHERE old_entry.generation_id=%s "
                            "AND old_entry.payload IS DISTINCT FROM new_entry.payload LIMIT 1"
                        ).format(
                            self.meta.table("raw_catalog_entries"),
                            self.meta.table("raw_catalog_entries"),
                        ),
                        (identifier, previous["generation_id"]),
                    ).fetchone()
                ):
                    raise HistoryError(503, "HISTORY_UNAVAILABLE")
                connection.execute(
                    sql.SQL("UPDATE {} SET state='RETIRED' WHERE state='ACTIVE'").format(
                        self.meta.table("raw_catalog_sync_state")
                    )
                )
                connection.execute(
                    sql.SQL("UPDATE {} SET state='ACTIVE' WHERE generation_id=%s").format(
                        self.meta.table("raw_catalog_sync_state")
                    ),
                    (identifier,),
                )
        return identifier

    def coverage(self, query):
        if self.meta is None or self.objects is None:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        with self.meta.factory() as connection:
            row = connection.execute(
                sql.SQL(
                    "SELECT generation_id,manifest,created_at FROM {} WHERE state='ACTIVE'"
                ).format(self.meta.table("raw_catalog_sync_state"))
            ).fetchone()
        if row is None:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        manifest = strict_json(bytes(row["manifest"]))
        age = (datetime.now(UTC) - timestamp(manifest["published_at"])).total_seconds()
        if not -5 <= age <= 120:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        partitions = [p for p in manifest["partitions"] if p["topic"] == "sensor.readings"]
        if (
            len(partitions) != 12
            or {p["partition"] for p in partitions} != set(range(12))
            or any(p["status"] != "COMPLETE" or p["gaps"] for p in partitions)
        ):
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        return {
            "catalog_generation": row["generation_id"],
            "partition_coverage": partitions,
            "available_from": None,
            "available_through": None,
        }

    def records(self, query):
        last = None
        while True:
            statement = sql.SQL("""SELECT payload,received_at,topic,partition,source_offset FROM {}
                WHERE generation_id=%s AND machine_id=ANY(%s) AND received_at>=%s AND received_at<%s
                AND topic='sensor.readings'""").format(self.meta.table("raw_catalog_entries"))
            params = [
                query["catalog_generation"],
                query.get("machine_ids", [query.get("machine_id")]),
                query["from"],
                query["to"],
            ]
            if last is not None:
                statement += sql.SQL(
                    " AND (received_at,topic,partition,source_offset)<(%s,%s,%s,%s)"
                )
                params.extend(last)
            statement += sql.SQL(
                " ORDER BY received_at DESC,topic DESC,partition DESC,source_offset DESC LIMIT 32"
            )
            with self.meta.factory() as connection:
                rows = connection.execute(statement, params).fetchall()
            if not rows:
                return
            for row in rows:
                # No DB transaction/connection is held during exact object reads.
                yield self.receipt(strict_json(bytes(row["payload"])))
            row = rows[-1]
            last = [row["received_at"], row["topic"], row["partition"], row["source_offset"]]

    def receipt(self, entry):
        manifest = strict_json(self.objects.read(entry["raw_manifest_ref"]))
        default_contracts().validator("archive/raw-manifest.schema.json").validate(manifest)
        if any(
            manifest[key] != entry[source]
            for key, source in (
                ("topic", "topic"),
                ("partition", "partition"),
                ("first_offset", "offset"),
                ("last_offset", "offset"),
            )
        ):
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        reference = entry["raw_object_ref"]
        if manifest["object"] != {
            k: reference[k] for k in ("bucket", "key", "version_id", "sha256")
        }:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        with gzip.GzipFile(fileobj=io.BytesIO(self.objects.read(reference))) as stream:
            raw = stream.read(2097153)
        if len(raw) > 2097152:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        frame = strict_json(raw)
        if any(frame[k] != entry[k] for k in ("topic", "partition", "offset")):
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        event = default_contracts().decode(
            "sensor.readings", base64.b64decode(frame["value_base64"], validate=True)
        )
        value = event.payload
        if (
            value["event_id"] != entry["source_event_id"]
            or value["machine_id"] != entry["machine_id"]
            or timestamp(value["received_at"]) != timestamp(entry["received_at"])
        ):
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        link = self.processing_link(event) if self.processing_link else None
        return {
            "event_id": value["event_id"],
            "machine_id": value["machine_id"],
            "stream_epoch": value["stream_epoch"],
            "sequence_number": value["sequence_number"],
            "received_at": value["received_at"],
            "server_accepted_at": value["server_accepted_at"],
            "features_raw": value["features"],
            "validation_status": "UNVERIFIED" if link is None else link["validation_status"],
            "result_id": None if link is None else link["result_id"],
            "validation_errors": [] if link is None else link["validation_errors"],
            "_markers": [] if link is None else link["_markers"],
            "receipt_id": entry["receipt_id"],
            "kafka": {
                "topic": entry["topic"],
                "partition": entry["partition"],
                "offset": entry["offset"],
            },
        }
