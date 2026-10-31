"""Native Storage catalog reads with a durable Processing-owned delta index."""

import base64
import gzip
import io
from datetime import UTC, datetime

from jsonschema import ValidationError
from maintai_messaging.contracts import default_contracts, strict_json
from maintai_messaging.errors import ConfigurationError, IdentityConflict
from maintai_storage.catalog import MAX_FRAME_BYTES, CatalogReader, CatalogStore
from psycopg import sql

from .native_catalog import NativeIndex
from .security import HistoryError, digest
from .snapshots import timestamp


class Catalog:
    def __init__(self, metadata, objects, schemas, *, processing_link=None):
        self.meta, self.objects, self.schemas = metadata, objects, schemas
        self.processing_link = processing_link
        self.store = None
        self.index = None
        if objects is not None and hasattr(objects, "client"):
            self.store = CatalogStore(objects.client, objects.raw_bucket)
            self.reader = CatalogReader(self.store)
            source_key = digest(
                {
                    "bucket": objects.raw_bucket,
                    "prefix": self.store.prefix,
                    "raw_prefix": self.store.raw_prefix,
                }
            )
            self.index = NativeIndex(metadata, self.store, self.reader, source_key)

    def sync(self):
        if self.index is None or self.meta is None:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        try:
            return self.index.sync()
        except (ConfigurationError, IdentityConflict, ValidationError, ValueError):
            raise HistoryError(503, "HISTORY_UNAVAILABLE") from None
        except HistoryError:
            raise
        except Exception:
            raise HistoryError(503, "HISTORY_UNAVAILABLE") from None

    def coverage(self, query):
        if self.index is None or self.meta is None:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        row = self.index.active()
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
            "partition_coverage": [
                {k: v for k, v in p.items() if k != "topic_id"} for p in partitions
            ],
            "available_from": None,
            "available_through": None,
        }

    def records(self, query):
        if self.index is None:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        with self.meta.factory() as connection:
            generation = connection.execute(
                sql.SQL(
                    "SELECT import_sequence FROM {} WHERE source_key=%s "
                    "AND generation_id=%s AND state='READY'"
                ).format(self.meta.table("native_catalog_imports")),
                (self.index.key, query["catalog_generation"]),
            ).fetchone()
        if generation is None:
            raise HistoryError(503, "HISTORY_UNAVAILABLE")
        last = None
        while True:
            statement = sql.SQL("""SELECT payload,received_at,topic,partition,source_offset FROM {}
                WHERE source_key=%s AND first_sequence<=%s AND machine_id=ANY(%s)
                AND received_at>=%s AND received_at<%s AND topic='sensor.readings'""").format(
                self.meta.table("native_catalog_receipts")
            )
            params = [
                self.index.key,
                generation["import_sequence"],
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
                yield self.receipt(strict_json(bytes(row["payload"])))
            row = rows[-1]
            last = [row["received_at"], row["topic"], row["partition"], row["source_offset"]]

    def receipt(self, entry):
        try:
            packed = self.reader.raw(entry)
            with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
                raw = stream.read(MAX_FRAME_BYTES + 1)
            if len(raw) > MAX_FRAME_BYTES:
                raise ValueError("Raw frame size exceeded")
            frame = strict_json(raw)
            if any(frame[k] != entry[k] for k in ("topic", "partition", "offset")):
                raise ValueError("Raw locator differs from catalog")
            return self._decoded(entry, frame)
        except HistoryError:
            raise
        except Exception:
            raise HistoryError(503, "HISTORY_UNAVAILABLE") from None

    def _decoded(self, entry, frame):
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
