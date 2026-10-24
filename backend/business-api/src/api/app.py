"""M2.R GET machine boundary; no projections, mutations, Alert, SSE or ML."""

import logging
from importlib.metadata import version
from uuid import uuid4

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .auth import RegistryFailure


def create_app(repository, verifier):
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.state.implementation_scope = "registry_lookup_only"

    @app.middleware("http")
    async def tracing(request, call_next):
        def trace(name, prefix):
            value = request.headers.get(name, "")
            return (
                value
                if 0 < len(value) <= 256 and value.isascii() and value.isprintable()
                else prefix + uuid4().hex
            )

        request.state.request_id = trace("X-Request-ID", "req_")
        request.state.correlation_id = trace("X-Correlation-ID", "cor_")
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["X-Correlation-ID"] = request.state.correlation_id
        logging.getLogger("maintai.business.registry").info(
            "registry response status=%s request_id=%s correlation_id=%s",
            response.status_code,
            request.state.request_id,
            request.state.correlation_id,
        )
        return response

    @app.exception_handler(RegistryFailure)
    async def failure(request: Request, exc: RegistryFailure):
        return JSONResponse(
            {
                "error": {
                    "code": exc.code,
                    "message": exc.code,
                    "retryable": exc.status == 503,
                    "details": {},
                    "request_id": request.state.request_id,
                    "correlation_id": request.state.correlation_id,
                }
            },
            status_code=exc.status,
            headers={"WWW-Authenticate": "Bearer"} if exc.status == 401 else None,
        )

    @app.get("/api/v1/machines/{machine_id:path}")
    def machine(machine_id: str, request: Request):
        identity = verifier.verify(request.headers.get("Authorization"))
        identity.authorize(machine_id)
        try:
            row = repository.get(machine_id)
        except psycopg.Error:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None
        if row is None:
            raise RegistryFailure(404, "MACHINE_NOT_FOUND")
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

    @app.get("/health/live")
    def live():
        return {"status": "UP", "service": "backend", "version": version("maintai-business")}

    @app.get("/health/ready")
    def ready():
        try:
            database = repository.check()
        except psycopg.Error:
            database = False
        oidc = verifier.check()
        healthy = database and oidc
        return JSONResponse(
            {
                "status": "UP" if healthy else "DEGRADED",
                "checks": {"database": database, "oidc_keys": oidc},
            },
            status_code=200 if healthy else 503,
        )

    return app
