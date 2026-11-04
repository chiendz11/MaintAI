"""Read-only job and Monitoring facades with current authenticated permissions."""

import psycopg
from backend.shared.alerts.domain import AlertError
from fastapi import Query, Request

from .auth import RegistryFailure


def install(app, verifier, briefs, monitoring):
    def call(request, repository, operation):
        principal = verifier.verify(request.headers.get("Authorization"))
        if repository is None:
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
        try:
            return operation(principal)
        except AlertError as exc:
            raise RegistryFailure(exc.status, exc.code) from None
        except (psycopg.Error, ValueError):
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None

    @app.get("/api/v1/brief-jobs/{job_id:path}")
    def job(job_id: str, request: Request):
        return call(request, briefs, lambda actor: briefs.job(job_id, actor.machines))

    @app.get("/api/v1/monitoring/summary")
    def summary(request: Request):
        return call(request, monitoring, lambda actor: monitoring.summary())

    @app.get("/api/v1/drift-reports")
    def reports(
        request: Request,
        limit: int = Query(20, ge=1, le=100),
        cursor: str | None = None,
        model_version: str | None = None,
        overall_status: str | None = None,
    ):
        return call(
            request,
            monitoring,
            lambda actor: monitoring.list(
                actor,
                limit=limit,
                cursor=cursor,
                model_version=model_version,
                overall_status=overall_status,
            ),
        )

    @app.get("/api/v1/drift-reports/{report_id:path}")
    def report(report_id: str, request: Request):
        return call(request, monitoring, lambda actor: monitoring.detail(report_id))
