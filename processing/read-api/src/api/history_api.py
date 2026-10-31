"""M6 HTTP adapters; heavy work runs outside the ASGI event loop."""

import asyncio
import threading
import time
from dataclasses import dataclass, field

from fastapi import Query, Request
from fastapi.responses import StreamingResponse
from maintai_messaging.contracts import strict_json
from processing.shared.history_meta.security import PROFILE, HistoryError
from starlette.background import BackgroundTask

from .auth import ReadFailure


@dataclass
class HistoryRuntime:
    history: object
    delegation: object
    exports: object
    charts: object
    schemas: object
    stop: threading.Event = field(default_factory=threading.Event)
    threads: list = field(default_factory=list)
    resources: list = field(default_factory=list)

    def start(self):
        from processing.shared.history_meta.expiry import expire_metadata

        def execute_jobs():
            self.exports.run_once()
            expire_metadata(self.exports.meta, now=self.exports.history.clock())

        def loop(operation, interval):
            while not self.stop.is_set():
                try:
                    result = operation()
                except Exception:
                    # Durable job state and the previous verified generation survive.
                    self.stop.wait(min(interval, 10))
                else:
                    self.stop.wait(1 if interval == 60 and result is None else interval)

        operations = [("history-exports", execute_jobs, 1)]
        if self.history.catalog is not None:
            operations.append(("history-catalog", self.history.catalog.sync, 60))
        for name, operation, interval in operations:
            thread = threading.Thread(
                target=loop, args=(operation, interval), name=name, daemon=True
            )
            thread.start()
            self.threads.append(thread)

    def close(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=8)
        for resource in self.resources:
            resource.close()

    def check(self):
        return self.history.meta.check() and self.delegation.keys.check()


def stream_file(file):
    try:
        while chunk := file.read(65536):
            yield chunk
    finally:
        file.close()


def install(app, verifier, runtime):
    def actor(request):
        service = verifier.verify(request.headers.get("Authorization"))
        if request.headers.get("X-MaintAI-History-Profile") != PROFILE:
            raise ReadFailure(400, "INVALID_HISTORY_PROFILE")
        if runtime is None:
            raise ReadFailure(503, "HISTORY_UNAVAILABLE")
        try:
            return runtime.delegation.verify(
                request.headers.get("X-MaintAI-History-Delegation"), service
            )
        except HistoryError as exc:
            raise ReadFailure(exc.status, exc.code) from None

    def call(request, operation):
        try:
            return operation()
        except HistoryError as exc:
            if exc.status >= 500:
                request.state.failed_dependency = "history"
            raise ReadFailure(exc.status, exc.code) from None
        except Exception:
            request.state.failed_dependency = "history"
            raise ReadFailure(503, "HISTORY_UNAVAILABLE") from None

    def query(start, end, model_version=None, result_kind=None):
        return {
            k: v
            for k, v in {
                "from": start,
                "to": end,
                "model_version": model_version,
                "result_kind": result_kind,
            }.items()
            if v is not None
        }

    @app.get("/api/v1/internal/machines/{machine_id:path}/predictions")
    def predictions(
        machine_id: str,
        request: Request,
        start: str | None = Query(None, alias="from"),
        end: str | None = Query(None, alias="to"),
        model_version: str | None = None,
        result_kind: str | None = None,
        cursor: str | None = None,
        limit: int = Query(100, ge=1, le=500),
    ):
        principal = actor(request)
        request.state.history_source, request.state.history_started = "HOT", time.perf_counter()
        return call(
            request,
            lambda: runtime.schemas.validate(
                "HistoryPage",
                runtime.history.page(
                    principal,
                    machine_id,
                    query(start, end, model_version, result_kind),
                    limit=limit,
                    cursor=cursor,
                ),
            ),
        )

    @app.get("/api/v1/internal/machines/{machine_id:path}/sensor-readings")
    def readings(
        machine_id: str,
        request: Request,
        start: str | None = Query(None, alias="from"),
        end: str | None = Query(None, alias="to"),
        cursor: str | None = None,
        limit: int = Query(100, ge=1, le=500),
    ):
        principal = actor(request)
        request.state.history_source, request.state.history_started = "ARCHIVE", time.perf_counter()
        return call(
            request,
            lambda: runtime.schemas.validate(
                "RawHistoryPage",
                runtime.history.page(
                    principal,
                    machine_id,
                    query(start, end),
                    collection="RAW",
                    limit=limit,
                    cursor=cursor,
                ),
            ),
        )

    @app.get("/api/v1/internal/machines/{machine_id:path}/timeseries")
    def timeseries(
        machine_id: str,
        request: Request,
        metric: str,
        start: str | None = Query(None, alias="from"),
        end: str | None = Query(None, alias="to"),
        resolution: str = "auto",
        max_points: int = Query(1000, ge=1, le=2000),
        markers_cursor: str | None = None,
    ):
        principal = actor(request)
        request.state.history_source = (
            "HOT" if metric in {"risk_score", "failure_probability"} else "ARCHIVE"
        )
        request.state.history_started = time.perf_counter()
        return call(
            request,
            lambda: runtime.schemas.validate(
                "TimeSeriesPage",
                runtime.charts.get(
                    principal,
                    machine_id,
                    query(start, end),
                    metric=metric,
                    resolution=resolution,
                    max_points=max_points,
                    markers_cursor=markers_cursor,
                ),
            ),
        )

    @app.post("/api/v1/internal/history-exports")
    async def create_export(request: Request):
        principal = await asyncio.to_thread(actor, request)
        if request.headers.get("Content-Type", "").split(";", 1)[0].lower() != "application/json":
            raise ReadFailure(415, "UNSUPPORTED_MEDIA_TYPE")
        if request.headers.get("Content-Encoding", "identity").lower() != "identity":
            raise ReadFailure(415, "UNSUPPORTED_MEDIA_TYPE")
        try:
            async with asyncio.timeout(10):
                body = bytearray()
                async for chunk in request.stream():
                    body.extend(chunk)
                    if len(body) > 262144:
                        raise ReadFailure(413, "PAYLOAD_TOO_LARGE")
            value = strict_json(bytes(body))
        except ReadFailure:
            raise
        except TimeoutError:
            raise ReadFailure(408, "REQUEST_TIMEOUT") from None
        except Exception:
            raise ReadFailure(422, "VALIDATION_FAILED") from None
        call(request, lambda: runtime.schemas.validate("ExportRequest", value, status=422))
        status, value = await asyncio.to_thread(
            call,
            request,
            lambda: runtime.exports.accept(
                principal, value, request.headers.get("Idempotency-Key")
            ),
        )
        if status == 200:
            return StreamingResponse(
                stream_file(value),
                media_type="text/csv; charset=utf-8",
                headers={
                    "Content-Disposition": 'attachment; filename="maintai-history.csv"',
                    "Cache-Control": "no-store",
                },
                background=BackgroundTask(value.close),
            )
        from fastapi.responses import JSONResponse

        return JSONResponse(
            call(request, lambda: runtime.schemas.validate("ExportJob", value)), status_code=202
        )

    @app.get("/api/v1/internal/history-exports/{job_id}/download")
    def download(job_id: str, request: Request):
        principal = actor(request)
        reference = call(request, lambda: runtime.exports.download(principal, job_id))
        store = runtime.exports.objects
        response = call(
            request,
            lambda: store.client.get_object(
                Bucket=reference["bucket"], Key=reference["key"], VersionId=reference["version_id"]
            ),
        )
        body = response["Body"]

        def chunks():
            try:
                while chunk := body.read(65536):
                    yield chunk
            finally:
                body.close()

        parquet = reference["key"].endswith(".parquet")
        return StreamingResponse(
            chunks(),
            media_type="application/vnd.apache.parquet" if parquet else "text/csv; charset=utf-8",
            headers={
                "Content-Length": str(reference["size_bytes"]),
                "Content-Disposition": 'attachment; filename="maintai-history.'
                + ("parquet" if parquet else "csv")
                + '"',
                "Cache-Control": "no-store",
            },
            background=BackgroundTask(body.close),
        )

    @app.get("/api/v1/internal/history-exports/{job_id}")
    def export_status(job_id: str, request: Request):
        principal = actor(request)
        return call(
            request,
            lambda: runtime.schemas.validate("ExportJob", runtime.exports.get(principal, job_id)),
        )
