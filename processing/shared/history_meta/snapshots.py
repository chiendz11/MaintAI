"""Freeze owner-local committed history into bounded durable metadata snapshots."""

import hashlib
import tempfile
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from maintai_messaging.contracts import canonical_json, strict_json
from psycopg import sql

from processing.shared.persistence.evidence import history_record

from .security import PROFILE, HistoryError, digest


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(parsed, datetime) or parsed.utcoffset() is None:
            raise ValueError("Timezone required")
        return parsed.astimezone(UTC)
    except (ValueError, TypeError):
        raise HistoryError(422, "VALIDATION_FAILED") from None


def normalize(machine, query, now, *, export=False):
    if set(query) - {"from", "to", "model_version", "result_kind"}:
        raise HistoryError(422, "VALIDATION_FAILED")
    end = timestamp(query.get("to", now))
    start = timestamp(query.get("from", end - timedelta(hours=1)))
    if start >= end or end - start > timedelta(days=365 if export else 31):
        raise HistoryError(422, "QUERY_TOO_LARGE")
    kind = query.get("result_kind")
    if kind is not None and kind not in {
        "PREDICTION_COMPLETED",
        "DATA_INVALID",
        "INFERENCE_UNAVAILABLE",
    }:
        raise HistoryError(422, "VALIDATION_FAILED")
    return {
        "machine_id": machine,
        "from": start.isoformat(),
        "to": end.isoformat(),
        **{k: query[k] for k in ("model_version", "result_kind") if query.get(k) is not None},
    }


class History:
    def __init__(self, source_factory, metadata, cursors, *, clock=None, catalog=None):
        self.source_factory, self.meta, self.cursors = source_factory, metadata, cursors
        self.clock = clock or (lambda: datetime.now(UTC))
        self.catalog = catalog

    def records(self, query, collection):
        if collection == "RAW":
            if self.catalog is None:
                raise HistoryError(503, "HISTORY_UNAVAILABLE")
            yield from self.catalog.records(query)
            return
        with self.source_factory() as connection:
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            with connection.cursor(name="history_" + uuid4().hex) as cursor:
                cursor.execute(
                    """SELECT e.*,r.result_id,r.kind,r.outcome,r.outcome_sha256,
                    r.completed_at AS processed_at,
                    p.prediction_id,p.label,p.failure_probability,p.model_threshold,p.model_identity,
                    d.risk_decision_id,d.risk_score,d.policy_version,d.policy_sha256,d.machine_state
                    FROM processing_results r JOIN sensor_events e USING(event_id)
                    LEFT JOIN predictions p USING(event_id)
                    LEFT JOIN risk_decisions d USING(event_id)
                    WHERE e.machine_id=ANY(%s)
                    AND r.completed_at>=%s AND r.completed_at<%s
                    AND (%s::text IS NULL OR r.kind=%s)
                    AND (%s::text IS NULL OR p.model_version=%s)
                    ORDER BY r.completed_at DESC,r.result_id DESC""",
                    (
                        query.get("machine_ids", [query.get("machine_id")]),
                        query["from"],
                        query["to"],
                        query.get("result_kind"),
                        query.get("result_kind"),
                        query.get("model_version"),
                        query.get("model_version"),
                    ),
                )
                while rows := cursor.fetchmany(128):
                    for row in rows:
                        value = history_record(row)
                        markers = []
                        if value["outcome"]["kind"] == "PREDICTION_COMPLETED":
                            markers = [
                                {
                                    "kind": "PREDICTION",
                                    "reference_id": value["outcome"]["prediction_id"],
                                    "source_event_id": value["source_event_id"],
                                    "position_at": row["processed_at"].isoformat(),
                                    "source_received_at": row["event_time"].isoformat(),
                                    "source_server_accepted_at": value["source_server_accepted_at"],
                                    "processed_at": row["processed_at"].isoformat(),
                                }
                            ]
                        yield {
                            **value,
                            "schema_version": "2.0.0",
                            "processed_at": row["processed_at"].isoformat(),
                            "_markers": markers,
                        }

    def create_snapshot(self, actor, machine, query, *, collection="PROCESSING", export=False):
        machines = [machine] if isinstance(machine, str) else machine
        if not isinstance(machines, list) or not machines or len(machines) > 100:
            raise HistoryError(422, "VALIDATION_FAILED")
        if not set(machines) <= actor.machines:
            raise HistoryError(403, "FORBIDDEN")
        now = self.clock()
        frozen = normalize(machines[0], query, now, export=export)
        if not isinstance(machine, str):
            frozen.pop("machine_id")
            frozen["machine_ids"] = sorted(set(machines))
        if collection == "RAW" and any(k in frozen for k in ("model_version", "result_kind")):
            raise HistoryError(422, "VALIDATION_FAILED")
        coverage = {"available_from": None, "available_through": None}
        if collection == "RAW":
            if self.catalog is None:
                raise HistoryError(503, "HISTORY_UNAVAILABLE")
            coverage.update(self.catalog.coverage(frozen))
            frozen["catalog_generation"] = coverage["catalog_generation"]
        identifier = "hs_" + uuid4().hex
        expires = now + timedelta(minutes=10)
        row_count, total_bytes = 0, 0
        seen_markers = set()
        minimum = maximum = None
        started = time.monotonic()
        # Stage immutable input before the metadata transaction. Raw S3 reads and
        # source cursors never hold a metadata connection/publication lock.
        with tempfile.SpooledTemporaryFile(max_size=1024 * 1024) as staged:
            for record in self.records(frozen, collection):
                markers = []
                for marker in record.pop("_markers", []):
                    marker_id = (marker["kind"], marker["reference_id"])
                    if marker_id not in seen_markers:
                        markers.append(marker)
                        seen_markers.add(marker_id)
                encoded = canonical_json({"record": record, "markers": markers}) + b"\n"
                total_bytes += len(encoded)
                if (
                    row_count >= (1000000 if export else 100000)
                    or total_bytes > (512 if export else 64) * 1024 * 1024
                ):
                    raise HistoryError(422, "QUERY_TOO_LARGE")
                if time.monotonic() - started > 10:
                    raise HistoryError(503, "QUERY_TIMEOUT")
                date = (
                    record["processed_at"] if collection == "PROCESSING" else record["received_at"]
                )
                parsed = timestamp(date)
                minimum = parsed if minimum is None else min(minimum, parsed)
                maximum = parsed if maximum is None else max(maximum, parsed)
                staged.write(encoded)
                row_count += 1
            if collection == "PROCESSING":
                coverage.update(
                    available_from=None if minimum is None else minimum.isoformat(),
                    available_through=None if maximum is None else maximum.isoformat(),
                )
            staged.seek(0)
            with self.meta.factory() as connection, connection.transaction():
                connection.execute(
                    sql.SQL("""INSERT INTO {}(snapshot_id,actor,scope_sha256,
                    query_sha256,query,collection,state,catalog_generation,coverage,as_of,expires_at,row_count)
                    VALUES(%s,%s,%s,%s,%s,%s,'BUILDING',%s,%s,%s,%s,%s)""").format(
                        self.meta.table("snapshots")
                    ),
                    (
                        identifier,
                        actor.subject,
                        actor.scope,
                        digest(frozen),
                        canonical_json(frozen),
                        collection,
                        frozen.get("catalog_generation"),
                        canonical_json(coverage),
                        now,
                        expires,
                        row_count,
                    ),
                )
                with connection.cursor().copy(
                    sql.SQL(
                        "COPY {}(snapshot_id,ordinal,entry_id,payload,payload_sha256,markers) "
                        "FROM STDIN"
                    ).format(self.meta.table("snapshot_members"))
                ) as copier:
                    for ordinal, line in enumerate(staged):
                        value = strict_json(line)
                        record = value["record"]
                        raw = canonical_json(record)
                        key = (
                            record["result_id"]
                            if collection == "PROCESSING"
                            else record["receipt_id"]
                        )
                        copier.write_row(
                            (
                                identifier,
                                ordinal,
                                key,
                                raw,
                                hashlib.sha256(raw).hexdigest(),
                                canonical_json({"items": value["markers"]}),
                            )
                        )
                connection.execute(
                    sql.SQL("UPDATE {} SET state='READY' WHERE snapshot_id=%s").format(
                        self.meta.table("snapshots")
                    ),
                    (identifier,),
                )
        return self.snapshot(actor, identifier)

    def snapshot(self, actor, identifier, *, allow_job_hold=False):
        with self.meta.factory() as connection:
            row = connection.execute(
                sql.SQL("SELECT * FROM {} WHERE snapshot_id=%s AND state='READY'").format(
                    self.meta.table("snapshots")
                ),
                (identifier,),
            ).fetchone()
            if row is None:
                raise HistoryError(404, "SNAPSHOT_NOT_FOUND")
            if row["actor"] != actor.subject or row["scope_sha256"] != actor.scope:
                raise HistoryError(403, "FORBIDDEN")
            if row["expires_at"] <= self.clock():
                hold = (
                    allow_job_hold
                    and connection.execute(
                        sql.SQL("SELECT 1 FROM {} WHERE snapshot_id=%s AND holds_input").format(
                            self.meta.table("export_jobs")
                        ),
                        (identifier,),
                    ).fetchone()
                )
                if not hold:
                    raise HistoryError(410, "SNAPSHOT_EXPIRED")
        return {
            **row,
            "_allow_job_hold": allow_job_hold,
            "query": strict_json(bytes(row["query"])),
            "coverage": strict_json(bytes(row["coverage"])),
        }

    def guard_members(self, connection, snapshot):
        row = connection.execute(
            sql.SQL(
                "SELECT expires_at FROM {} WHERE snapshot_id=%s AND state='READY' FOR SHARE"
            ).format(self.meta.table("snapshots")),
            (snapshot["snapshot_id"],),
        ).fetchone()
        if row is None:
            raise HistoryError(404, "SNAPSHOT_NOT_FOUND")
        if row["expires_at"] <= self.clock():
            hold = (
                snapshot.get("_allow_job_hold")
                and connection.execute(
                    sql.SQL("SELECT 1 FROM {} WHERE snapshot_id=%s AND holds_input LIMIT 1").format(
                        self.meta.table("export_jobs")
                    ),
                    (snapshot["snapshot_id"],),
                ).fetchone()
            )
            if not hold:
                raise HistoryError(410, "SNAPSHOT_EXPIRED")

    def members(self, snapshot, *, after=-1, limit=None):
        statement = sql.SQL(
            "SELECT ordinal,payload,payload_sha256 FROM {} WHERE snapshot_id=%s "
            "AND ordinal>%s ORDER BY ordinal"
        ).format(self.meta.table("snapshot_members"))
        params = [snapshot["snapshot_id"], after]
        if limit is not None:
            statement += sql.SQL(" LIMIT %s")
            params.append(limit)
        with self.meta.factory() as connection:
            self.guard_members(connection, snapshot)
            with connection.cursor(name="members_" + uuid4().hex) as cursor:
                cursor.execute(statement, params)
                while rows := cursor.fetchmany(64):
                    for row in rows:
                        raw = bytes(row["payload"])
                        if hashlib.sha256(raw).hexdigest() != row["payload_sha256"]:
                            raise HistoryError(503, "HISTORY_UNAVAILABLE")
                        yield row["ordinal"], strict_json(raw)

    def page(self, actor, machine, query, *, collection="PROCESSING", limit=100, cursor=None):
        if type(limit) is not int or not 1 <= limit <= 500:
            raise HistoryError(422, "VALIDATION_FAILED")
        last = -1
        if cursor:
            token = self.cursors.decode(cursor, actor=actor.subject, scope=actor.scope)
            snapshot = self.snapshot(actor, token.get("snapshot"))
            if (
                token.get("kind") != "page"
                or snapshot["collection"] != collection
                or snapshot["query"]["machine_id"] != machine
                or token.get("query") != snapshot["query_sha256"]
            ):
                raise HistoryError(400, "INVALID_CURSOR")
            for key, value in query.items():
                normalized = timestamp(value).isoformat() if key in {"from", "to"} else value
                if snapshot["query"].get(key) != normalized:
                    raise HistoryError(400, "INVALID_CURSOR")
            last = token["last"]
        else:
            snapshot = self.create_snapshot(actor, machine, query, collection=collection)
        items, count_bytes, ordinal = [], 0, last
        for index, value in self.members(snapshot, after=last, limit=limit + 1):
            size = len(canonical_json(value))
            if len(items) >= limit or (items and count_bytes + size > 1500000):
                break
            items.append(value)
            count_bytes += size
            ordinal = index
        next_cursor = None
        if ordinal + 1 < snapshot["row_count"]:
            next_cursor = self.cursors.encode(
                {
                    "kind": "page",
                    "actor": actor.subject,
                    "scope": actor.scope,
                    "snapshot": snapshot["snapshot_id"],
                    "query": snapshot["query_sha256"],
                    "last": ordinal,
                    "exp": snapshot["expires_at"].timestamp(),
                }
            )
        return {
            "schema_version": "2.0.0",
            "profile": PROFILE,
            "snapshot_id": snapshot["snapshot_id"],
            "as_of": snapshot["as_of"].isoformat(),
            "expires_at": snapshot["expires_at"].isoformat(),
            "coverage": "COMPLETE",
            "collection": collection,
            "time_basis": "processed_at" if collection == "PROCESSING" else "received_at",
            "tiers_used": ["HOT"] if collection == "PROCESSING" else ["ARCHIVE"],
            **snapshot["coverage"],
            "next_cursor": next_cursor,
            "items": items,
        }
