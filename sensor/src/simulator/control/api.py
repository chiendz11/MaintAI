"""Five frozen Control operations; no telemetry/rate/SSE endpoints are invented."""

import json
import sqlite3
import time
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException

from simulator import __version__
from simulator.control.auth import AuthError, TokenVerifier
from simulator.logging import log
from simulator.ordering.store import ControlError, utc
from simulator.scheduling.engine import SensorEngine
from simulator.settings import RuntimeSettings, loopback


def create_app(settings: RuntimeSettings, *, engine=None, verifier=None, background=True):
    verifier = verifier or TokenVerifier(settings)

    @asynccontextmanager
    async def lifespan(app):
        owned = engine or SensorEngine(settings)
        app.state.engine = owned
        if background:
            owned.start()
        try:
            yield
        finally:
            owned.close()

    app = FastAPI(
        title="MaintAI Sensor Control",
        version="1.3.2",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=False,
            allow_methods=["GET", "POST", "PUT"],
            allow_headers=[
                "Authorization",
                "Content-Type",
                "Idempotency-Key",
                "X-Request-ID",
                "X-Correlation-ID",
            ],
            expose_headers=["X-Request-ID", "X-Correlation-ID"],
            max_age=600,
        )

    def failure(request, status, code):
        return JSONResponse(
            {
                "error": {
                    "code": code,
                    "message": "Sensor Control request could not be completed",
                    "retryable": status in (429, 503, 504),
                    "request_id": getattr(request.state, "request_id", f"req_{uuid4()}"),
                    "correlation_id": getattr(request.state, "correlation_id", f"cor_{uuid4()}"),
                }
            },
            status_code=status,
            headers={"WWW-Authenticate": "Bearer"} if status == 401 else None,
        )

    @app.exception_handler(AuthError)
    @app.exception_handler(ControlError)
    async def control_failure(request, exc):
        return failure(request, exc.status, exc.code)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        return failure(request, 422, "VALIDATION_FAILED")

    @app.exception_handler(sqlite3.Error)
    async def state_failure(request, exc):
        log("sensor_state_unavailable", error_type=type(exc).__name__)
        return failure(request, 503, "STATE_UNAVAILABLE")

    @app.exception_handler(HTTPException)
    async def http_failure(request, exc):
        return failure(
            request, exc.status_code, "NOT_FOUND" if exc.status_code == 404 else "INVALID_REQUEST"
        )

    @app.middleware("http")
    async def instrumentation(request, call_next):
        def identity(value, prefix):
            return (
                value
                if value and len(value) <= 128 and value.isascii() and value.isprintable()
                else f"{prefix}_{uuid4()}"
            )

        request.state.request_id = identity(request.headers.get("X-Request-ID"), "req")
        request.state.correlation_id = identity(request.headers.get("X-Correlation-ID"), "cor")
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            log("sensor_http_failure", error_type=type(exc).__name__)
            response = failure(request, 500, "INTERNAL_ERROR")
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["X-Correlation-ID"] = request.state.correlation_id
        if request.url.path not in ("/metrics", "/health/live", "/health/ready"):
            route = getattr(request.scope.get("route"), "path", "__unmatched__")
            method = (
                request.method if request.method in ("GET", "POST", "PUT", "OPTIONS") else "OTHER"
            )
            metrics = request.app.state.engine.metrics
            metrics.http.labels(route, method, str(response.status_code)).inc()
            metrics.duration.labels(route, method).observe(time.perf_counter() - started)
        return response

    def principal(request):
        if settings.auth_mode == "local" and not loopback(request.url.hostname):
            raise AuthError(403, "FORBIDDEN")
        return verifier.verify(
            request.headers.get("Authorization"), request.client.host if request.client else None
        )

    def authorize(request, machine_id):
        user = principal(request)
        if not user.allows(machine_id):
            raise AuthError(403, "FORBIDDEN")
        return user

    @app.get("/api/v1/simulator/machines")
    def list_machines(request: Request):
        user = principal(request)
        return {
            "items": [
                m for m in request.app.state.engine.store.list() if user.allows(m["machine_id"])
            ],
            "as_of": utc(),
        }

    @app.get("/api/v1/simulator/machines/{machine_id}/control")
    def read_control(machine_id: str, request: Request):
        authorize(request, machine_id)
        return request.app.state.engine.store.get(machine_id)

    def command_handler(action):
        async def command(machine_id: str, request: Request):
            user = await run_in_threadpool(authorize, request, machine_id)
            key = request.headers.get("Idempotency-Key", "")
            if not key or len(key) > 128 or not key.isascii() or not key.isprintable():
                raise ControlError(422, "VALIDATION_FAILED")
            if (
                request.headers.get("content-type", "").split(";")[0].strip().lower()
                != "application/json"
            ):
                raise ControlError(422, "VALIDATION_FAILED")
            raw = bytearray()
            async for chunk in request.stream():
                raw.extend(chunk)
                if len(raw) > settings.max_control_body_bytes:
                    raise ControlError(422, "PAYLOAD_TOO_LARGE")
            try:

                def pairs(items):
                    result = {}
                    for key, value in items:
                        if key in result:
                            raise ValueError("Duplicate key")
                        result[key] = value
                    return result

                body = json.loads(raw, object_pairs_hook=pairs)
            except (ValueError, UnicodeError):
                raise ControlError(400, "MALFORMED_JSON") from None
            keys = (
                {"expected_control_version", "scenario"}
                if action == "scenario"
                else {"expected_control_version"}
            )
            if not (
                isinstance(body, dict)
                and set(body) == keys
                and type(body.get("expected_control_version")) is int
                and body["expected_control_version"] >= 0
            ):
                raise ControlError(422, "VALIDATION_FAILED")
            if action == "scenario" and not (
                isinstance(body["scenario"], str) and 0 < len(body["scenario"]) <= 128
            ):
                raise ControlError(422, "VALIDATION_FAILED")
            runtime = request.app.state.engine
            result = await run_in_threadpool(
                runtime.store.command,
                user.subject,
                key,
                machine_id,
                action,
                body,
                set(runtime.scenarios),
                settings.receipt_max_count,
            )
            log(
                "sensor_control_ack",
                actor=user.subject,
                machine_id=machine_id,
                action=action,
                control_version=result["control_version"],
                request_id=request.state.request_id,
                correlation_id=request.state.correlation_id,
            )
            return result

        return command

    for action in ("pause", "resume", "scenario"):
        app.add_api_route(
            f"/api/v1/simulator/machines/{{machine_id}}/{action}",
            command_handler(action),
            methods=["PUT" if action == "scenario" else "POST"],
        )

    @app.get("/health/live")
    def live(request: Request):
        healthy = request.app.state.engine.ready()
        return JSONResponse(
            {
                "status": "UP" if healthy else "DOWN",
                "service": "sensor-simulator",
                "version": __version__,
            },
            status_code=200 if healthy else 503,
        )

    @app.get("/health/ready")
    def ready(request: Request):
        runtime = request.app.state.engine
        runtime.store.summary()  # Failure is503, never a fabricated empty queue.
        return JSONResponse(
            {"status": "UP" if runtime.ready() else "DOWN"},
            status_code=200 if runtime.ready() else 503,
        )

    @app.get("/metrics")
    def metrics(request: Request):
        runtime = request.app.state.engine
        runtime.metrics.refresh(runtime.store)
        return Response(
            generate_latest(runtime.metrics.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    return app
