"""Business registry/live reads and the Processing status HTTP facade."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Literal
from uuid import uuid4

import psycopg
from backend.shared.projections.views import MachineViewUnavailable
from business_sse.response import SSEResponse
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import generate_latest

from .auth import RegistryFailure
from .observability import Metrics


def create_app(
    repository,
    verifier,
    *,
    status_client=None,
    projections=None,
    notifications=None,
    alerts=None,
    history=None,
):
    @asynccontextmanager
    async def lifespan(app):
        try:
            if notifications is not None:
                await notifications.start()
            yield
        finally:
            if notifications is not None:
                await notifications.stop()
            if status_client is not None:
                await status_client.aclose()
            if history is not None:
                await history.aclose()

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    metrics = Metrics()
    if notifications is not None:
        notifications.metrics = metrics
    app.state.metrics = metrics
    from .history import install

    install(app, verifier, history)
    app.state.implementation_scope = {
        "registry": True,
        "fleet_detail": projections is not None,
        "live": projections is not None,
        "status": status_client is not None,
        "sse": notifications is not None,
    }

    @app.middleware("http")
    async def tracing(request, call_next):
        started, status_code = time.perf_counter(), "UNHANDLED"

        def trace(name, prefix):
            value = request.headers.get(name, "")
            return (
                value
                if 0 < len(value) <= 256 and value.isascii() and value.isprintable()
                else prefix + uuid4().hex
            )

        request.state.request_id = trace("X-Request-ID", "req_")
        request.state.correlation_id = trace("X-Correlation-ID", "cor_")
        try:
            response = await call_next(request)
            status_code = response.status_code
        finally:
            if request.url.path not in {"/metrics", "/health/live", "/health/ready"}:
                route = request.scope.get("route")
                try:
                    template = getattr(
                        request.state, "metric_route", getattr(route, "path", "__unmatched__")
                    )
                    metrics.http(
                        template, request.method, status_code, time.perf_counter() - started
                    )
                except Exception:
                    pass
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["X-Correlation-ID"] = request.state.correlation_id
        try:
            logging.getLogger("maintai.business.registry").info(
                "registry response status=%s request_id=%s correlation_id=%s",
                response.status_code,
                request.state.request_id,
                request.state.correlation_id,
            )
        except Exception:
            pass
        return response

    @app.exception_handler(RegistryFailure)
    async def failure(request: Request, exc: RegistryFailure):
        dependency = getattr(request.state, "failed_dependency", None)
        if dependency is not None and exc.status in {503, 504}:
            try:
                metrics.dependency(dependency)
            except Exception:
                pass
        return JSONResponse(
            {
                "error": {
                    "code": exc.code,
                    "message": exc.code,
                    "retryable": exc.status in {429, 503, 504}
                    or exc.code == "EVENT_NOT_MATERIALIZED",
                    "details": {},
                    "request_id": request.state.request_id,
                    "correlation_id": request.state.correlation_id,
                }
            },
            status_code=exc.status,
            headers={"WWW-Authenticate": "Bearer"} if exc.status == 401 else None,
        )

    @app.exception_handler(RequestValidationError)
    async def invalid_query(request: Request, exc: RequestValidationError):
        return await failure(request, RegistryFailure(422, "VALIDATION_FAILED"))

    def read_identity(request):
        try:
            return verifier.verify(request.headers.get("Authorization"))
        except RegistryFailure as exc:
            if exc.status == 503:
                request.state.failed_dependency = "oidc"
            raise

    @app.get("/api/v1/machines")
    def fleet(
        request: Request,
        state: Literal["NORMAL", "WATCH", "HIGH", "CRITICAL", "INVALID", "UNKNOWN"] | None = None,
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ):
        identity = read_identity(request)
        if projections is None:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        try:
            return projections.fleet(identity.machines, state=state, limit=limit, offset=offset)
        except MachineViewUnavailable:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None
        except (psycopg.Error, ValueError):
            request.state.failed_dependency = "business_db"
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None

    @app.get("/api/v1/machines/{machine_id:path}")
    async def machine(machine_id: str, request: Request):
        raw_path = request.scope.get("raw_path", request.url.path.encode()).split(b"?", 1)[0]
        for resource in ("predictions", "sensor-readings", "timeseries"):
            suffix = "/" + resource
            if machine_id.endswith(suffix) and raw_path.endswith(suffix.encode()):
                machine = machine_id[: -len(suffix)]
                identity = await asyncio.to_thread(read_identity, request)
                identity.authorize(machine)
                request.state.metric_route = "/api/v1/machines/{machine_id}/" + resource
                if request.headers.get("X-MaintAI-History-Profile") != "M6-HISTORY-01/1.0.0":
                    raise RegistryFailure(400, "INVALID_HISTORY_PROFILE")
                if history is None:
                    raise RegistryFailure(503, "HISTORY_UNAVAILABLE")
                return await history.machine(identity, machine, resource, request)
        return await asyncio.to_thread(machine_detail, machine_id, request)

    def machine_detail(machine_id: str, request: Request):
        # Inspect the raw fixed suffix before decoded opaque IDs. A detail ID ending
        # in /live must encode that slash; an actual live resource appends /live literally.
        raw_path = request.scope.get("raw_path", request.url.path.encode()).split(b"?", 1)[0]
        is_live = (
            projections is not None and machine_id.endswith("/live") and raw_path.endswith(b"/live")
        )
        if is_live:
            machine_id = machine_id[:-5]
            request.state.metric_route = "/api/v1/machines/{machine_id}/live"
        identity = read_identity(request)
        identity.authorize(machine_id)
        try:
            if projections is not None:
                row = projections.live(machine_id) if is_live else projections.detail(machine_id)
            else:
                row = repository.get(machine_id)
        except MachineViewUnavailable:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None
        except (psycopg.Error, ValueError):
            request.state.failed_dependency = "business_db"
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None
        if row is None:
            raise RegistryFailure(404, "MACHINE_NOT_FOUND")
        if projections is not None:
            return row
        return {
            "machine_id": row["machine_id"],
            "display_name": row["display_name"],
            "state": row["state"],
            "last_seen_at": row["last_seen_at"],
            "data_status": None,
            "latest_sensor": None,
            "latest_prediction": None,
            "active_alerts": [],
        }

    @app.get("/api/v1/events/stream")
    async def events(request: Request, machine_id: str | None = None):
        identity = await asyncio.to_thread(read_identity, request)
        machines = identity.machines
        if machine_id is not None:
            identity.authorize(machine_id)
            machines = frozenset({machine_id})
        if notifications is None or not notifications.ready:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        if machine_id is not None:
            try:
                existing = await asyncio.to_thread(repository.get, machine_id)
            except psycopg.Error:
                request.state.failed_dependency = "business_db"
                raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None
            if existing is None:
                raise RegistryFailure(404, "MACHINE_NOT_FOUND")
        subscription = notifications.subscribe(machines, expires_at=identity.expires_at)
        return SSEResponse(notifications, subscription)

    @app.get("/api/v1/sensor-events/{event_id:path}/status")
    async def status(event_id: str, request: Request):
        try:
            identity = await asyncio.to_thread(
                verifier.verify,
                request.headers.get("Authorization"),
                allowed_roles={"VIEWER", "TECHNICIAN", "ADMIN", "SENSOR_SERVICE"},
            )
        except RegistryFailure as exc:
            if exc.status == 503:
                request.state.failed_dependency = "oidc"
            raise
        if status_client is None:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        value = await status_client.get(event_id, correlation_id=request.state.correlation_id)
        identity.authorize(value["machine_id"])
        return value

    @app.get("/metrics")
    def scrape():
        return Response(generate_latest(metrics.registry), media_type="text/plain; version=0.0.4")

    @app.get("/health/live")
    def live():
        return {"status": "UP", "service": "backend", "version": version("maintai-business")}

    @app.get("/health/ready")
    def ready():
        try:
            database = projections.check() if projections is not None else repository.check()
            if alerts is not None:
                database = database and alerts.check()
        except psycopg.Error:
            database = False
        oidc = verifier.check()
        listener = notifications is None or notifications.ready
        healthy = database and oidc and listener
        return JSONResponse(
            {
                "status": "UP" if healthy else "DEGRADED",
                "checks": {
                    "database": database,
                    "oidc_keys": oidc,
                    **({"notifications": listener} if notifications is not None else {}),
                },
            },
            status_code=200 if healthy else 503,
        )

    if alerts is not None:
        alerts.metrics = metrics.alerts
        from .alerts import install

        install(app, verifier, alerts)

    return app
