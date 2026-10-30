"""Bounded CSV preflight and durable, leased CSV/Parquet export execution."""

import csv
import hashlib
import io
import tempfile
import threading
import time
from datetime import timedelta
from uuid import uuid4

from maintai_messaging.contracts import canonical_json, strict_json
from psycopg import sql

from .security import PROFILE, HistoryError, digest
from .snapshots import timestamp


class NeedsJob(Exception):
    pass


def safe_cell(value):
    if (
        isinstance(value, str)
        and value
        and (value.lstrip().startswith(("=", "+", "-", "@")) or value[0] in "\t\r\n")
    ):
        return "'" + value
    return value


def csv_output(records, *, small=False, heartbeat=lambda: None):
    file = tempfile.SpooledTemporaryFile(max_size=1024 * 1024)
    columns = [
        "schema_version",
        "result_id",
        "source_event_id",
        "machine_id",
        "processed_at",
        "received_at",
        "source_server_accepted_at",
        "kind",
        "prediction",
        "failure_probability",
        "risk_score",
        "model_version",
        "policy_version",
        "record_json",
        "features_json",
        "outcome_json",
        "risk_json",
    ]
    line = io.StringIO(newline="")
    writer = csv.writer(line, lineterminator="\r\n")
    started = time.monotonic()
    count = 0
    try:
        writer.writerow(columns)
        file.write(line.getvalue().encode("utf-8"))
        for record in records:
            heartbeat()
            line.seek(0)
            line.truncate(0)
            outcome = record.get("outcome", {})
            values = {
                **outcome,
                **record,
                "source_event_id": record.get("source_event_id", record.get("event_id")),
                "record_json": canonical_json(record).decode(),
                "features_json": canonical_json(record["features_raw"]).decode(),
                "outcome_json": canonical_json(outcome).decode(),
                "risk_json": canonical_json(record.get("risk_snapshot")).decode(),
            }
            writer.writerow([safe_cell(values.get(key, "")) for key in columns])
            file.write(line.getvalue().encode("utf-8"))
            count += 1
            if small and (
                count > 10000 or file.tell() > 10 * 1024 * 1024 or time.monotonic() - started > 10
            ):
                raise NeedsJob()
            if file.tell() > 512 * 1024 * 1024:
                raise HistoryError(422, "QUERY_TOO_LARGE")
        file.seek(0)
        return file, count
    except Exception:
        file.close()
        raise


def parquet_output(records, *, heartbeat=lambda: None):
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Logical JSON preserves INVALID feature types and arbitrary source integers.
    # No ML wire fields are coerced into lossy fixed-width Arrow integers.
    schema = pa.schema(
        [
            ("schema_version", pa.string()),
            ("result_id", pa.string()),
            ("source_event_id", pa.string()),
            ("machine_id", pa.string()),
            ("processed_at", pa.string()),
            ("received_at", pa.string()),
            ("outcome", pa.json_()),
            ("features_raw", pa.json_()),
            ("record", pa.json_()),
        ],
        metadata={b"maintai.history.profile": PROFILE.encode(), b"maintai.record.schema": b"2.0.0"},
    )
    file = tempfile.SpooledTemporaryFile(max_size=1024 * 1024)
    count, batch = 0, []
    try:
        with pq.ParquetWriter(file, schema, compression="zstd") as writer:
            for record in records:
                heartbeat()
                batch.append(
                    {
                        "schema_version": "2.0.0",
                        "result_id": record.get("result_id"),
                        "source_event_id": record.get("source_event_id", record.get("event_id")),
                        "machine_id": record["machine_id"],
                        "processed_at": record.get("processed_at"),
                        "received_at": record.get("received_at"),
                        "outcome": None
                        if "outcome" not in record
                        else canonical_json(record["outcome"]).decode(),
                        "features_raw": canonical_json(record["features_raw"]).decode(),
                        "record": canonical_json(record).decode(),
                    }
                )
                count += 1
                if len(batch) >= 256:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch.clear()
                    if file.tell() > 512 * 1024 * 1024:
                        raise HistoryError(422, "QUERY_TOO_LARGE")
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
        file.seek(0)
        return file, count
    except Exception:
        file.close()
        raise


class Exports:
    def __init__(self, history, objects):
        self.history, self.meta, self.objects = history, history.meta, objects

    def normalize(self, actor, request):
        allowed = {
            "profile",
            "machine_ids",
            "from",
            "to",
            "format",
            "resource",
            "delivery",
            "model_version",
            "result_kind",
            "snapshot_id",
        }
        machines = request.get("machine_ids")
        if (
            set(request) - allowed
            or request.get("profile") != PROFILE
            or not isinstance(machines, list)
            or not 1 <= len(machines) <= 100
            or not all(isinstance(m, str) and m for m in machines)
            or len(set(machines)) != len(machines)
        ):
            raise HistoryError(422, "VALIDATION_FAILED")
        if not set(machines) <= actor.machines:
            raise HistoryError(403, "FORBIDDEN")
        if (
            request.get("format") not in {"CSV", "PARQUET"}
            or request.get("resource") not in {"PROCESSING", "RAW"}
            or request.get("delivery", "AUTO") not in {"AUTO", "JOB"}
        ):
            raise HistoryError(422, "VALIDATION_FAILED")
        if "from" not in request or "to" not in request:
            raise HistoryError(422, "VALIDATION_FAILED")
        start, end = timestamp(request["from"]), timestamp(request["to"])
        if start >= end or end - start > timedelta(days=365):
            raise HistoryError(422, "QUERY_TOO_LARGE")
        if request["resource"] == "RAW" and any(
            k in request for k in ("model_version", "result_kind")
        ):
            raise HistoryError(422, "VALIDATION_FAILED")
        return {
            **request,
            "from": start.isoformat(),
            "to": end.isoformat(),
            "machine_ids": sorted(machines),
            "delivery": request.get("delivery", "AUTO"),
        }

    def _row(self, actor, job_id=None, key=None):
        with self.meta.factory() as connection:
            where = (
                sql.SQL("job_id=%s")
                if job_id is not None
                else sql.SQL("actor=%s AND idempotency_key=%s")
            )
            params = (job_id,) if job_id is not None else (actor.subject, key)
            row = connection.execute(
                sql.SQL("SELECT * FROM {} WHERE ").format(self.meta.table("export_jobs")) + where,
                params,
            ).fetchone()
        if row is not None and (
            row["actor"] != actor.subject or row["scope_sha256"] != actor.scope
        ):
            raise HistoryError(403, "FORBIDDEN")
        return row

    def accept(self, actor, request, key):
        body = self.normalize(actor, request)
        if not isinstance(key, str) or not 1 <= len(key.encode()) <= 256 or "\x00" in key:
            raise HistoryError(422, "VALIDATION_FAILED")
        existing = self._row(actor, key=key)
        if existing:
            if existing["request_sha256"] != digest(body):
                raise HistoryError(409, "IDEMPOTENCY_CONFLICT")
            return self._accepted(actor, existing)
        job_mode = body["format"] == "PARQUET" or body["delivery"] == "JOB"
        if job_mode and self.objects is None:
            raise HistoryError(503, "DEPENDENCY_UNAVAILABLE")
        query = {k: body[k] for k in ("from", "to", "model_version", "result_kind") if k in body}
        if body.get("snapshot_id"):
            snapshot = self.history.snapshot(actor, body["snapshot_id"])
            effective = snapshot["query"]
            if (
                snapshot["collection"] != body["resource"]
                or sorted(effective.get("machine_ids", [effective.get("machine_id")]))
                != body["machine_ids"]
                or any(
                    effective.get(k) != query.get(k)
                    for k in ("from", "to", "model_version", "result_kind")
                )
            ):
                raise HistoryError(409, "SNAPSHOT_QUERY_CONFLICT")
        else:
            snapshot = self.history.create_snapshot(
                actor, body["machine_ids"], query, collection=body["resource"], export=True
            )
        stream = None
        if not job_mode:
            try:
                stream, _ = csv_output(
                    (record for _, record in self.history.members(snapshot)), small=True
                )
            except NeedsJob:
                job_mode = True
        if job_mode and self.objects is None:
            raise HistoryError(503, "DEPENDENCY_UNAVAILABLE")
        now, identifier = self.history.clock(), "he_" + uuid4().hex
        with self.meta.factory() as connection, connection.transaction():
            locked = connection.execute(
                sql.SQL("SELECT expires_at FROM {} WHERE snapshot_id=%s FOR UPDATE").format(
                    self.meta.table("snapshots")
                ),
                (snapshot["snapshot_id"],),
            ).fetchone()
            if locked is None or locked["expires_at"] <= self.history.clock():
                if stream is not None:
                    stream.close()
                raise HistoryError(410, "SNAPSHOT_EXPIRED")
            row = connection.execute(
                sql.SQL("""INSERT INTO {}(job_id,actor,scope_sha256,
                idempotency_key,request_sha256,request,snapshot_id,status,mode,next_attempt_at,
                deadline,created_at,updated_at,holds_input)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(actor,idempotency_key) DO NOTHING RETURNING *""").format(
                    self.meta.table("export_jobs")
                ),
                (
                    identifier,
                    actor.subject,
                    actor.scope,
                    key,
                    digest(body),
                    canonical_json(body),
                    snapshot["snapshot_id"],
                    "QUEUED" if job_mode else "SUCCEEDED",
                    "JOB" if job_mode else "STREAM",
                    now,
                    now + timedelta(minutes=30),
                    now,
                    now,
                    job_mode,
                ),
            ).fetchone()
        if row is None:
            if stream is not None:
                stream.close()
            return self.accept(actor, body, key)
        if stream is not None:
            return 200, stream
        return 202, self.get(actor, identifier)

    def _accepted(self, actor, row):
        if row["mode"] == "STREAM":
            snapshot = self.history.snapshot(actor, row["snapshot_id"])
            stream, _ = csv_output(
                (record for _, record in self.history.members(snapshot)), small=True
            )
            return 200, stream
        return 202, self.get(actor, row["job_id"])

    def get(self, actor, identifier):
        row = self._row(actor, identifier)
        if row is None or row["mode"] != "JOB":
            raise HistoryError(404, "EXPORT_NOT_FOUND")
        request = strict_json(bytes(row["request"]))
        status = (
            "EXPIRED"
            if row["expires_at"] is not None and row["expires_at"] <= self.history.clock()
            else row["status"]
        )
        ready = status == "SUCCEEDED"
        return {
            "job_id": identifier,
            "status": status,
            "created_at": row["created_at"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
            "attempts": row["attempts"],
            "error_code": row["error_code"],
            "status_url": "/api/v1/internal/history-exports/" + identifier,
            "artifact_ref": strict_json(bytes(row["artifact_ref"])) if ready else None,
            "expires_at": None if row["expires_at"] is None else row["expires_at"].isoformat(),
            "download_url": "/api/v1/internal/history-exports/" + identifier + "/download"
            if ready
            else None,
            "profile": PROFILE,
            "snapshot_id": row["snapshot_id"],
            "resource": request["resource"],
            "time_basis": "processed_at" if request["resource"] == "PROCESSING" else "received_at",
        }

    def download(self, actor, identifier):
        value = self.get(actor, identifier)
        if value["status"] == "EXPIRED":
            raise HistoryError(410, "EXPORT_EXPIRED")
        if value["status"] != "SUCCEEDED":
            raise HistoryError(409, "EXPORT_NOT_READY")
        if self.objects is None:
            raise HistoryError(503, "DEPENDENCY_UNAVAILABLE")
        self.objects.verify(value["artifact_ref"])
        return value["artifact_ref"]

    def run_once(self):
        if self.objects is None:
            return False
        now = self.history.clock()
        worker = uuid4().hex
        with self.meta.factory() as connection, connection.transaction():
            connection.execute(
                sql.SQL(
                    "UPDATE {} SET status='FAILED',error_code='EXPORT_TIMEOUT',"
                    "fencing_token=fencing_token+1,updated_at=%s "
                    "WHERE status IN ('QUEUED','RUNNING') AND deadline<=%s AND mode='JOB'"
                ).format(self.meta.table("export_jobs")),
                (now, now),
            )
            row = connection.execute(
                sql.SQL("""SELECT * FROM {} WHERE mode='JOB' AND deadline>%s
                AND ((status='QUEUED' AND next_attempt_at<=%s) OR
                (status='RUNNING' AND lease_until<=%s)) ORDER BY created_at,job_id
                LIMIT 1 FOR UPDATE SKIP LOCKED""").format(self.meta.table("export_jobs")),
                (now, now, now),
            ).fetchone()
            if row is None:
                return False
            row = connection.execute(
                sql.SQL("""UPDATE {} SET status='RUNNING',attempts=attempts+1,
                fencing_token=fencing_token+1,lease_owner=%s,lease_until=%s,updated_at=%s
                WHERE job_id=%s RETURNING *""").format(self.meta.table("export_jobs")),
                (worker, now + timedelta(seconds=30), now, row["job_id"]),
            ).fetchone()
        request = strict_json(bytes(row["request"]))

        # Stored authorization scope is immutable; request machines may be a subset.
        class JobActor:
            subject = row["actor"]
            scope = row["scope_sha256"]

        stopped, lost = threading.Event(), threading.Event()

        def heartbeat():
            current = self.history.clock()
            if lost.is_set() or current >= row["deadline"]:
                raise HistoryError(503, "EXPORT_LEASE_LOST")

        def renew():
            while not stopped.wait(5):
                try:
                    with self.meta.factory() as connection:
                        result = connection.execute(
                            sql.SQL(
                                "UPDATE {} SET lease_until=%s WHERE job_id=%s "
                                "AND fencing_token=%s AND lease_owner=%s AND status='RUNNING'"
                            ).format(self.meta.table("export_jobs")),
                            (
                                self.history.clock() + timedelta(seconds=30),
                                row["job_id"],
                                row["fencing_token"],
                                worker,
                            ),
                        )
                        if result.rowcount != 1:
                            lost.set()
                            return
                except Exception:
                    lost.set()
                    return

        thread = threading.Thread(target=renew, daemon=True, name="history-export-lease")
        thread.start()
        file = None
        try:
            snapshot = self.history.snapshot(JobActor(), row["snapshot_id"], allow_job_hold=True)
            records = (record for _, record in self.history.members(snapshot))
            file, count = (
                parquet_output(records, heartbeat=heartbeat)
                if request["format"] == "PARQUET"
                else csv_output(records, heartbeat=heartbeat)
            )
            checksum = hashlib.sha256()
            size = 0
            while chunk := file.read(65536):
                checksum.update(chunk)
                size += len(chunk)
                heartbeat()
            extension = "parquet" if request["format"] == "PARQUET" else "csv"
            reference = self.objects.put_export(
                "history-exports/" + row["job_id"] + "/data." + extension,
                file,
                size,
                checksum.hexdigest(),
                "application/vnd.apache.parquet" if extension == "parquet" else "text/csv",
            )
            manifest = canonical_json(
                {
                    "schema_version": "2.0.0",
                    "profile": PROFILE,
                    "job_id": row["job_id"],
                    "snapshot_id": row["snapshot_id"],
                    "query_sha256": row["request_sha256"],
                    "row_count": count,
                    "format": request["format"],
                    "parts": [reference],
                }
            )
            with io.BytesIO(manifest) as body:
                self.objects.put_export(
                    "history-exports/" + row["job_id"] + "/manifest.json",
                    body,
                    len(manifest),
                    hashlib.sha256(manifest).hexdigest(),
                    "application/json",
                )
            heartbeat()
            done = self.history.clock()
            with self.meta.factory() as connection:
                connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status='SUCCEEDED',artifact_ref=%s,expires_at=%s,"
                        "updated_at=%s,error_code=NULL WHERE job_id=%s AND fencing_token=%s "
                        "AND lease_owner=%s AND status='RUNNING' AND lease_until>%s"
                    ).format(self.meta.table("export_jobs")),
                    (
                        canonical_json(reference),
                        done + timedelta(hours=24),
                        done,
                        row["job_id"],
                        row["fencing_token"],
                        worker,
                        done,
                    ),
                )
        except Exception as error:
            current = self.history.clock()
            retry = (
                row["attempts"] < 3
                and current < row["deadline"]
                and (not isinstance(error, HistoryError) or error.status >= 500)
            )
            with self.meta.factory() as connection:
                connection.execute(
                    sql.SQL(
                        "UPDATE {} SET status=%s,error_code=%s,next_attempt_at=%s,updated_at=%s "
                        "WHERE job_id=%s AND fencing_token=%s "
                        "AND lease_owner=%s AND status='RUNNING'"
                    ).format(self.meta.table("export_jobs")),
                    (
                        "QUEUED" if retry else "FAILED",
                        error.code if isinstance(error, HistoryError) else "EXPORT_FAILED",
                        current + timedelta(seconds=2 ** row["attempts"]),
                        current,
                        row["job_id"],
                        row["fencing_token"],
                        worker,
                    ),
                )
        finally:
            stopped.set()
            thread.join(timeout=4)
            if file is not None:
                file.close()
        return True
