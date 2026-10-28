"""FastAPI wrapper: bounded raw body → verified SDK gateway → HTTP projection."""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from prometheus_client import CONTENT_TYPE_LATEST
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect
from starlette.responses import JSONResponse, Response

from . import __version__
from .errors import EdgeFailure
from .observability import RequestObservation, request_observation
from .request_guards import check_media, read_bounded_body
from .responses import edge_error, fresh_trace, resolve_request_identity, to_http
from .runtime import build_runtime, run_blocking


def create_app(settings, *, publisher_factory=None, verifier=None, probe=None):
    @asynccontextmanager
    async def lifespan(app):
        runtime = build_runtime(
            settings, publisher_factory=publisher_factory, verifier=verifier, probe=probe
        )
        app.state.runtime = runtime
        stop = asyncio.Event()
        monitor = None
        try:
            await runtime.refresh_health()
            monitor = asyncio.create_task(runtime.monitor(stop))
            yield
        finally:
            runtime.gate.begin_shutdown()
            stop.set()
            if monitor is not None:
                await monitor
            await runtime.close()

    # The checked-in OpenAPI is authoritative. Avoid a second generated contract.
    app = FastAPI(
        title="MaintAI Ingestion API",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.middleware("http")
    async def request_context(request, call_next):
        runtime = request.app.state.runtime
        started = runtime.obs.clock()
        trace = fresh_trace()
        context = RequestObservation()
        token = request_observation.set(context)
        response = None
        try:
            try:
                trace = resolve_request_identity(request.headers)
                request.state.trace = trace
                response = await call_next(request)
            except EdgeFailure as failure:
                context.error_code = failure.code
                response = edge_error(
                    failure.status, failure.code, trace, retryable=failure.retryable
                )
            except Exception:
                response = edge_error(500, "INTERNAL_ERROR", trace)
        finally:
            runtime.obs.record_request(request, response, trace, started, context)
            request_observation.reset(token)
        response.headers.update(
            {"X-Request-ID": trace.request_id, "X-Correlation-ID": trace.correlation_id}
        )
        return response

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return edge_error(
            exc.status_code,
            "NOT_FOUND" if exc.status_code == 404 else "METHOD_NOT_ALLOWED",
            request.state.trace,
        )

    @app.post("/api/v1/sensor-readings")
    async def ingest(request: Request):
        rt = request.app.state.runtime
        trace = request.state.trace
        admitted = False
        try:
            if not rt.gate.try_acquire():
                raise EdgeFailure(
                    503 if rt.gate.draining else 429,
                    "DEPENDENCY_UNAVAILABLE" if rt.gate.draining else "RATE_LIMITED",
                    retryable=True,
                )
            admitted = True
            context = request_observation.get()
            context.admitted_at = rt.obs.clock()
            identity = await run_blocking(rt.verifier.verify, request.headers.get("authorization"))
            check_media(request.headers)
            raw = await read_bounded_body(
                request,
                limit=settings.kafka.max_payload_bytes,
                timeout_seconds=settings.body_timeout,
            )
            result = await run_blocking(
                rt.gateway.handle,
                raw,
                identity=identity,
                request_id=trace.request_id,
                correlation_id=trace.correlation_id,
            )
            context.error_code = result.body.get("error", {}).get("code")
            return to_http(result, trace)
        except EdgeFailure as failure:
            return edge_error(failure.status, failure.code, trace, retryable=failure.retryable)
        except ClientDisconnect:
            # No body was published if disconnect occurred during streaming.
            return Response(status_code=499)
        except Exception:
            return edge_error(500, "INTERNAL_ERROR", trace)
        finally:
            if admitted:
                rt.gate.release()

    @app.get("/health/live")
    async def live():
        return {"status": "UP", "service": "ingestion-api", "version": __version__}

    @app.get("/health/ready")
    async def ready():
        rt = app.state.runtime
        status, body = rt.health.response(draining=rt.gate.draining)
        return JSONResponse(body, status_code=status)

    @app.get("/metrics")
    async def metrics():
        return Response(
            app.state.runtime.obs.render(), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    return app
