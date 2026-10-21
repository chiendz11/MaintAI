import json
import time
from collections.abc import Callable
from datetime import datetime
from importlib.resources import files
from typing import Protocol

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from drift_monitor.models import Checkpoint, DriftReport, Features, RuntimeSample


class Repository(Protocol):
    def load_checkpoint(self, worker_name: str) -> Checkpoint | None: ...
    def load_window(self, size: int, model_version: str) -> list[RuntimeSample]: ...
    def find_report(self, report_id: str) -> DriftReport | None: ...
    def save_report(self, report: DriftReport) -> DriftReport: ...
    def counts(self) -> dict[str, int]: ...
    def checkpoint(
        self, worker_name: str, report: DriftReport, latest: RuntimeSample | None, now: datetime
    ) -> None: ...


class PostgresRepository:
    def __init__(
        self,
        database_url: str,
        *,
        max_retries: int = 2,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.database_url, self.max_retries, self.sleep = database_url, max_retries, sleep

    def _transaction(self, operation):
        for attempt in range(self.max_retries + 1):
            try:
                with psycopg.connect(
                    self.database_url,
                    connect_timeout=5,
                    row_factory=dict_row,
                    application_name="maintai-drift-worker",
                ) as conn:
                    conn.execute("SET LOCAL statement_timeout=10000")
                    return operation(conn)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                if attempt == self.max_retries:
                    raise
                self.sleep(0.1 * 2**attempt)
        raise AssertionError("DB retry loop must return")

    def init_db(self) -> None:
        # Explicit command only; the worker never creates backend-owned tables.
        sql = files("drift_monitor").joinpath("sql/001_drift.sql").read_text()
        self._transaction(lambda conn: conn.execute(sql))

    def load_checkpoint(self, worker_name: str) -> Checkpoint | None:
        row = self._transaction(
            lambda conn: conn.execute(
                "SELECT worker_name, last_processed_event_at, last_processed_event_id, "
                "last_success_at, last_report_id FROM drift_checkpoints WHERE worker_name=%s",
                (worker_name,),
            ).fetchone()
        )
        return Checkpoint.model_validate(row) if row else None

    def load_window(self, size: int, model_version: str) -> list[RuntimeSample]:
        rows = self._transaction(
            lambda conn: conn.execute(
                "SELECT s.event_id, s.received_at, s.features_json, p.label "
                "FROM sensor_events s LEFT JOIN predictions p "
                "ON p.event_id=s.event_id AND p.model_version=%s "
                "WHERE s.validation_status='VALID' "
                "ORDER BY s.received_at DESC, s.event_id DESC LIMIT %s",
                (model_version, size),
            ).fetchall()
        )
        return sorted(
            [
                RuntimeSample(
                    event_id=row["event_id"],
                    received_at=row["received_at"],
                    features=Features.model_validate(row["features_json"]),
                    prediction_label=row["label"],
                )
                for row in rows
            ],
            key=lambda row: (row.received_at, row.event_id),
        )

    def find_report(self, report_id: str) -> DriftReport | None:
        row = self._transaction(
            lambda conn: conn.execute(
                "SELECT report_json FROM drift_reports WHERE report_id=%s",
                (report_id,),
            ).fetchone()
        )
        return DriftReport.model_validate_json(json.dumps(row["report_json"])) if row else None

    def save_report(self, report: DriftReport) -> DriftReport:
        def save(conn):
            conn.execute(
                "INSERT INTO drift_reports (report_id,computed_at,detector_version,"
                "reference_dataset_version,model_version,window_start,window_end,sample_count,"
                "overall_status,drifted_feature_count,warning_feature_count,critical_feature_count,"
                "report_json) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (report_id) DO NOTHING",
                (
                    report.report_id,
                    report.computed_at,
                    report.detector_version,
                    report.reference.dataset_version,
                    report.reference.model_version,
                    report.current_window.start_at,
                    report.current_window.end_at,
                    report.current_window.sample_count,
                    report.overall_status.value,
                    report.summary.warning_features + report.summary.critical_features,
                    report.summary.warning_features,
                    report.summary.critical_features,
                    Jsonb(report.model_dump(mode="json")),
                ),
            )
            return conn.execute(
                "SELECT report_json FROM drift_reports WHERE report_id=%s", (report.report_id,)
            ).fetchone()["report_json"]

        return DriftReport.model_validate_json(json.dumps(self._transaction(save)))

    def counts(self) -> dict[str, int]:
        rows = self._transaction(
            lambda conn: conn.execute(
                "SELECT overall_status, count(*) AS count "
                "FROM drift_reports GROUP BY overall_status",
            ).fetchall()
        )
        return {row["overall_status"]: row["count"] for row in rows}

    def checkpoint(
        self, worker_name: str, report: DriftReport, latest: RuntimeSample | None, now: datetime
    ) -> None:
        self._transaction(
            lambda conn: conn.execute(
                "INSERT INTO drift_checkpoints (worker_name,last_processed_event_at,"
                "last_processed_event_id,last_success_at,last_report_id) VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (worker_name) DO UPDATE SET "
                "last_processed_event_at=EXCLUDED.last_processed_event_at,"
                "last_processed_event_id=EXCLUDED.last_processed_event_id,"
                "last_success_at=EXCLUDED.last_success_at,last_report_id=EXCLUDED.last_report_id,"
                "updated_at=now() WHERE drift_checkpoints.last_processed_event_at IS NULL OR "
                "(EXCLUDED.last_processed_event_at,EXCLUDED.last_processed_event_id) >= "
                "(drift_checkpoints.last_processed_event_at,drift_checkpoints.last_processed_event_id)",
                (
                    worker_name,
                    latest.received_at if latest else None,
                    latest.event_id if latest else None,
                    now,
                    report.report_id,
                ),
            )
        )
