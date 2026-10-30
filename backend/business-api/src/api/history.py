"""Public M6 history facade; all history inputs and results cross owner HTTP."""

import asyncio

from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from maintai_messaging.contracts import strict_json
from starlette.background import BackgroundTask

from .auth import RegistryFailure

PROFILE = "M6-HISTORY-01/1.0.0"


def stream_file(file):
    try:
        while chunk := file.read(65536):
            yield chunk
    finally:
        file.close()


class HistoryFacade:
    def __init__(self, client, schemas, *, markers=None):
        self.client, self.schemas, self.markers = client, schemas, markers

    async def aclose(self):
        await self.client.aclose()

    async def machine(self, actor, machine, resource, request):
        from business_processing_client.history import opaque

        allowed = {"from", "to"}
        if resource == "predictions":
            allowed |= {"model_version", "result_kind", "limit", "cursor"}
        elif resource == "sensor-readings":
            allowed |= {"limit", "cursor"}
        else:
            allowed |= {"metric", "resolution", "max_points", "markers_cursor"}
        params = dict(request.query_params)
        if set(params) - allowed or len(request.query_params.multi_items()) != len(params):
            raise RegistryFailure(422, "VALIDATION_FAILED")
        if resource == "timeseries" and self.markers is not None:
            try:
                return await self.markers.get(actor, machine, request)
            except RegistryFailure:
                raise
            except Exception:
                raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None
        if resource == "timeseries":
            raise RegistryFailure(503, "HISTORY_MARKERS_UNAVAILABLE")
        response = await self.client.request(
            actor,
            "GET",
            "machines/" + opaque(machine) + "/" + resource,
            query=params,
            correlation_id=request.state.correlation_id,
        )
        definition = {
            "predictions": "HistoryPage",
            "sensor-readings": "RawHistoryPage",
            "timeseries": "TimeSeriesPage",
        }[resource]
        value = self.schemas.validate(definition, response.value)
        if any(item["machine_id"] != machine for item in value["items"]):
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        return value

    async def create_export(self, actor, request):
        if (
            request.headers.get("Content-Type", "").split(";", 1)[0].lower() != "application/json"
            or request.headers.get("Content-Encoding", "identity").lower() != "identity"
        ):
            raise RegistryFailure(415, "UNSUPPORTED_MEDIA_TYPE")
        try:
            async with asyncio.timeout(10):
                raw = bytearray()
                async for chunk in request.stream():
                    if len(raw) + len(chunk) > 262144:
                        raise RegistryFailure(413, "PAYLOAD_TOO_LARGE")
                    raw.extend(chunk)
            value = strict_json(bytes(raw))
        except RegistryFailure:
            raise
        except TimeoutError:
            raise RegistryFailure(408, "REQUEST_TIMEOUT") from None
        except Exception:
            raise RegistryFailure(422, "VALIDATION_FAILED") from None
        self.schemas.validate("ExportRequest", value, request=True)
        if not set(value["machine_ids"]) <= actor.machines:
            raise RegistryFailure(403, "FORBIDDEN")
        key = request.headers.get("Idempotency-Key")
        if not isinstance(key, str) or not 1 <= len(key) <= 256 or not key.isprintable():
            raise RegistryFailure(422, "VALIDATION_FAILED")
        response = await self.client.request(
            actor,
            "POST",
            "history-exports",
            value=value,
            idempotency_key=key,
            correlation_id=request.state.correlation_id,
        )
        return self.render(response, "ExportJob")

    def render(self, response, definition):
        if response.file is not None:
            parquet = response.media_type == "application/vnd.apache.parquet"
            return StreamingResponse(
                stream_file(response.file),
                media_type=response.media_type,
                headers={
                    "Cache-Control": "no-store",
                    "Content-Disposition": 'attachment; filename="maintai-history.'
                    + ("parquet" if parquet else "csv")
                    + '"',
                },
                background=BackgroundTask(response.file.close),
            )
        return JSONResponse(
            self.schemas.validate(definition, response.value),
            status_code=response.status,
            headers={"Cache-Control": "no-store"},
        )

    async def job(self, actor, job_id, request, *, download=False):
        from business_processing_client.history import opaque

        resource = "history-exports/" + opaque(job_id) + ("/download" if download else "")
        response = await self.client.request(
            actor, "GET", resource, correlation_id=request.state.correlation_id
        )
        return self.render(response, "ExportJob")

    async def prediction(self, actor, result_id, request):
        from business_processing_client.history import opaque

        response = await self.client.request(
            actor,
            "GET",
            "predictions/" + opaque(result_id),
            correlation_id=request.state.correlation_id,
        )
        value = self.schemas.validate("HistoryRecord", response.value)
        actor.authorize(value["machine_id"])
        if value["result_id"] != result_id:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        return value


def install(app, verifier, history):
    async def actor(request):
        identity = await asyncio.to_thread(verifier.verify, request.headers.get("Authorization"))
        if request.headers.get("X-MaintAI-History-Profile") != PROFILE:
            raise RegistryFailure(400, "INVALID_HISTORY_PROFILE")
        if history is None:
            raise RegistryFailure(503, "HISTORY_UNAVAILABLE")
        return identity

    @app.post("/api/v1/history-exports")
    async def create_export(request: Request):
        identity = await actor(request)
        return await history.create_export(identity, request)

    @app.get("/api/v1/predictions/{result_id:path}")
    async def prediction(result_id: str, request: Request):
        identity = await asyncio.to_thread(verifier.verify, request.headers.get("Authorization"))
        if history is None:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        return await history.prediction(identity, result_id, request)

    @app.get("/api/v1/history-exports/{job_id}/download")
    async def download(job_id: str, request: Request):
        identity = await actor(request)
        return await history.job(identity, job_id, request, download=True)

    @app.get("/api/v1/history-exports/{job_id}")
    async def status(job_id: str, request: Request):
        identity = await actor(request)
        return await history.job(identity, job_id, request)
