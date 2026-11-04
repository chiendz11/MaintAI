"""Verified S3 publication catalog with actor-bound immutable pagination."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
from maintai_messaging.contracts import canonical_json, strict_json

from backend.shared.alerts.domain import AlertError
from backend.shared.copilot.repository import emit
from backend.shared.copilot.schemas import valid

STATUSES = {"STABLE", "WARNING", "CRITICAL", "ERROR", "INSUFFICIENT_DATA"}


class DriftCatalog:
    def __init__(
        self,
        factory,
        store,
        cursor_key,
        *,
        model_version=None,
        services=None,
        heartbeat=None,
        stale_after=900,
    ):
        if len(cursor_key) < 32:
            raise ValueError("Drift cursor key requires at least 32 bytes")
        self.factory, self.store, self.key = factory, store, cursor_key
        self.model_version, self.services, self.heartbeat = model_version, services, heartbeat
        self.stale_after = stale_after

    def _report(self, manifest):
        try:
            if not valid("runtime", manifest, "DriftManifest"):
                raise ValueError("Invalid manifest")
            ref = manifest["report_ref"]
            raw = self.store.read(ref)
            if len(raw) != ref["size_bytes"] or hashlib.sha256(raw).hexdigest() != ref["sha256"]:
                raise ValueError("Report identity differs")
            report = strict_json(raw)
            if (
                not valid("drift-report", report)
                or report["report_id"] != manifest["report_id"]
                or report["computed_at"] != manifest["computed_at"]
                or report["overall_status"] != manifest["overall_status"]
                or any(
                    report["reference"][k] != manifest[k]
                    for k in ("model_version", "dataset_version")
                )
            ):
                raise ValueError("Report and manifest differ")
            return report
        except Exception:
            raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE") from None

    def _ingest(self, manifest):
        if not valid("runtime", manifest, "DriftManifest"):
            raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE")
        body = canonical_json(manifest)
        digest = hashlib.sha256(body).hexdigest()
        with self.factory() as connection:
            existing = connection.execute(
                "SELECT manifest_sha256 FROM drift_catalog WHERE report_id=%s",
                (manifest["report_id"],),
            ).fetchone()
        if existing:
            if existing["manifest_sha256"] != digest:
                raise AlertError(503, "DRIFT_CATALOG_CONFLICT")
            return
        report = self._report(manifest)
        with self.factory() as connection, connection.transaction():
            inserted = connection.execute(
                """INSERT INTO drift_catalog
                (report_id,computed_at,model_version,overall_status,manifest,manifest_sha256,window_end_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(report_id) DO NOTHING
                RETURNING report_id""",
                (
                    manifest["report_id"],
                    manifest["computed_at"],
                    manifest["model_version"],
                    manifest["overall_status"],
                    body,
                    digest,
                    report["current_window"]["end_at"],
                ),
            ).fetchone()
            if inserted:
                emit(connection, "drift.updated", None, {"report_id": manifest["report_id"]})
            else:
                existing = connection.execute(
                    "SELECT manifest_sha256 FROM drift_catalog WHERE report_id=%s",
                    (manifest["report_id"],),
                ).fetchone()
                if existing["manifest_sha256"] != digest:
                    raise AlertError(503, "DRIFT_CATALOG_CONFLICT")

    def _entries(self, entries):
        failed = False
        for key, manifest in entries:
            try:
                if manifest is None:
                    raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE")
                self._ingest(manifest)
                with self.factory() as connection:
                    connection.execute(
                        "DELETE FROM drift_catalog_errors WHERE object_key=%s", (key,)
                    )
            except Exception:
                failed = True
                with self.factory() as connection:
                    connection.execute(
                        """INSERT INTO drift_catalog_errors(object_key,error_code)
                        VALUES(%s,'DRIFT_REPORT_UNAVAILABLE') ON CONFLICT(object_key) DO UPDATE
                        SET updated_at=clock_timestamp()""",
                        (key,),
                    )
        return failed

    def sync(self, *, full=False, max_pages=5):
        today = datetime.now(UTC).date()
        prefix = "drift-report-manifests/published/"
        prefixes = (
            [prefix] if full else [prefix + str(today - timedelta(days=i)) + "/" for i in (0, 1)]
        )
        failed, complete = False, True
        try:
            for selected in prefixes:
                with self.factory() as connection:
                    connection.execute(
                        """INSERT INTO drift_scan_progress(prefix,scan_id)
                        VALUES(%s,%s) ON CONFLICT(prefix) DO UPDATE SET scan_id=excluded.scan_id,
                        continuation=NULL,done=FALSE,has_errors=FALSE
                        WHERE drift_scan_progress.done""",
                        (selected, uuid4().hex),
                    )
                    state = connection.execute(
                        "SELECT * FROM drift_scan_progress WHERE prefix=%s", (selected,)
                    ).fetchone()
                for _ in range(max_pages):
                    entries, token = self.store.page(selected, state["continuation"])
                    errors = self._entries(entries)
                    with self.factory() as connection:
                        updated = connection.execute(
                            """UPDATE drift_scan_progress SET continuation=%s,
                            done=%s,has_errors=has_errors OR %s WHERE prefix=%s AND scan_id=%s
                            AND continuation IS NOT DISTINCT FROM %s AND NOT done RETURNING *""",
                            (
                                token,
                                token is None,
                                errors,
                                selected,
                                state["scan_id"],
                                state["continuation"],
                            ),
                        ).fetchone()
                    if updated is None:
                        complete = False
                        break
                    state = updated
                    if token is None:
                        break
                complete = complete and state["done"]
                failed = failed or state["has_errors"]
            with self.factory() as connection:
                quarantined = connection.execute(
                    "SELECT EXISTS(SELECT 1 FROM drift_catalog_errors) AS value"
                ).fetchone()["value"]
                connection.execute(
                    """UPDATE drift_catalog_state SET
                    last_scan_at=CASE WHEN %s AND NOT %s THEN clock_timestamp()
                        ELSE last_scan_at END,
                    complete_scan_at=CASE WHEN %s AND %s THEN clock_timestamp()
                        ELSE complete_scan_at END,
                    error_code=CASE WHEN %s THEN 'DRIFT_REPORT_UNAVAILABLE' ELSE NULL END""",
                    (complete, failed, full, complete, quarantined),
                )
            if failed:
                raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE")
            return complete
        except Exception:
            with self.factory() as connection:
                connection.execute(
                    "UPDATE drift_catalog_state SET error_code='DRIFT_REPORT_UNAVAILABLE'"
                )
            raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE") from None

    def cleanup(self, *, batch=1000):
        if not 1 <= batch <= 10000:
            raise ValueError("Invalid metadata cleanup batch")
        with self.factory() as connection, connection.transaction():
            connection.execute(
                """DELETE FROM drift_list_members WHERE (snapshot_id,position) IN (
                SELECT m.snapshot_id,m.position FROM drift_list_members m
                JOIN drift_list_snapshots s
                USING(snapshot_id) WHERE s.expires_at<=clock_timestamp() LIMIT %s)""",
                (batch,),
            )
            connection.execute(
                """DELETE FROM drift_list_snapshots WHERE snapshot_id IN (
                SELECT s.snapshot_id FROM drift_list_snapshots s WHERE expires_at<=clock_timestamp()
                AND NOT EXISTS(SELECT 1 FROM drift_list_members m WHERE m.snapshot_id=s.snapshot_id)
                LIMIT %s)""",
                (batch,),
            )

    def list(self, actor, *, limit=20, cursor=None, model_version=None, overall_status=None):
        if not 1 <= limit <= 100 or (overall_status is not None and overall_status not in STATUSES):
            raise AlertError(422, "VALIDATION_FAILED")
        binding = hashlib.sha256(
            canonical_json(
                {
                    "actor": actor.subject,
                    "machines": sorted(actor.machines),
                    "roles": sorted(actor.roles),
                    "limit": limit,
                    "model": model_version,
                    "status": overall_status,
                }
            )
        ).hexdigest()
        with self.factory() as connection, connection.transaction():
            if cursor:
                try:
                    data = jwt.decode(
                        cursor,
                        self.key,
                        algorithms=["HS256"],
                        audience="drift-cursor",
                        options={"require": ["exp", "aud", "snapshot", "position", "binding"]},
                    )
                    if (
                        data["binding"] != binding
                        or type(data["position"]) is not int
                        or data["position"] < 0
                    ):
                        raise ValueError("Invalid binding")
                    identifier, position = data["snapshot"], data["position"]
                except jwt.ExpiredSignatureError:
                    raise AlertError(410, "CURSOR_EXPIRED") from None
                except (jwt.PyJWTError, ValueError, TypeError, KeyError):
                    raise AlertError(400, "INVALID_CURSOR") from None
            else:
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                state = connection.execute(
                    "SELECT *, clock_timestamp() AS now FROM drift_catalog_state"
                ).fetchone()
                if state["last_scan_at"] is None:
                    raise AlertError(503, "DRIFT_REPORT_UNAVAILABLE")
                identifier, position = uuid4().hex, 0
                stale = (
                    bool(state["error_code"])
                    or (state["now"] - state["last_scan_at"]).total_seconds() > 120
                )
                connection.execute(
                    "INSERT INTO drift_list_snapshots(snapshot_id,binding,catalog_as_of,stale) "
                    "VALUES(%s,%s,%s,%s)",
                    (identifier, binding, state["last_scan_at"], stale),
                )
                connection.execute(
                    """INSERT INTO drift_list_members(snapshot_id,position,manifest)
                    SELECT %s,row_number() OVER(ORDER BY computed_at DESC,report_id DESC),manifest
                    FROM drift_catalog WHERE (%s::text IS NULL OR model_version=%s)
                    AND (%s::text IS NULL OR overall_status=%s)""",
                    (identifier, model_version, model_version, overall_status, overall_status),
                )
            snapshot = connection.execute(
                "SELECT * FROM drift_list_snapshots WHERE snapshot_id=%s "
                "AND binding=%s AND expires_at>clock_timestamp()",
                (identifier, binding),
            ).fetchone()
            if snapshot is None:
                raise AlertError(410, "CURSOR_EXPIRED")
            rows = connection.execute(
                "SELECT * FROM drift_list_members WHERE snapshot_id=%s "
                "AND position>%s ORDER BY position LIMIT %s",
                (identifier, position, limit + 1),
            ).fetchall()
            next_cursor = None
            if len(rows) > limit:
                next_cursor = jwt.encode(
                    {
                        "aud": "drift-cursor",
                        "exp": snapshot["expires_at"],
                        "snapshot": identifier,
                        "binding": binding,
                        "position": rows[limit - 1]["position"],
                    },
                    self.key,
                    algorithm="HS256",
                )
            return {
                "items": [json.loads(bytes(row["manifest"])) for row in rows[:limit]],
                "next_cursor": next_cursor,
                "catalog_as_of": snapshot["catalog_as_of"].isoformat(),
                "stale": snapshot["stale"],
            }

    def detail(self, report_id):
        with self.factory() as connection:
            row = connection.execute(
                "SELECT manifest FROM drift_catalog WHERE report_id=%s", (report_id,)
            ).fetchone()
        if row is None:
            raise AlertError(404, "DRIFT_REPORT_NOT_FOUND")
        return self._report(json.loads(bytes(row["manifest"])))

    def summary(self):
        now = datetime.now(UTC)
        services = {
            name: {"status": "UNAVAILABLE", "message": "Chưa cấu hình nguồn kiểm tra."}
            for name in ("backend", "ml", "rag")
        }
        with self.factory() as connection:
            connection.execute("SELECT 1")
            latest = None
            if self.model_version:
                row = connection.execute(
                    "SELECT * FROM drift_catalog WHERE model_version=%s "
                    "ORDER BY computed_at DESC,report_id DESC LIMIT 1",
                    (self.model_version,),
                ).fetchone()
                if row:
                    latest = {
                        k: row[k].isoformat() if k in {"computed_at", "window_end_at"} else row[k]
                        for k in (
                            "report_id",
                            "computed_at",
                            "window_end_at",
                            "overall_status",
                            "model_version",
                        )
                    }
        services["backend"] = {"status": "AVAILABLE", "message": "Business database khả dụng."}
        if self.services:
            services.update(self.services())
        last = None
        if self.heartbeat and self.model_version:
            last = self.heartbeat(self.model_version)
        worker = (
            "NOT_AVAILABLE"
            if last is None
            else "FRESH"
            if (now - last).total_seconds() <= self.stale_after
            else "STALE"
        )
        return {
            "status": "AVAILABLE"
            if all(s["status"] == "AVAILABLE" for s in services.values())
            and worker == "FRESH"
            and latest is not None
            else "DEGRADED",
            "observed_at": now.isoformat(),
            "services": services,
            "drift": {
                "worker_status": worker,
                "last_success_at": last.isoformat() if last else None,
                "stale_after_seconds": self.stale_after,
                "latest_report": latest,
            },
        }
