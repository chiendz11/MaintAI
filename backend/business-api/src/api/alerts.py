"""Alert REST routes use authenticated principals and commit before success."""

from typing import Literal

import psycopg
from fastapi import Query, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .auth import RegistryFailure


class ActorNote(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    actor_id: str = Field(min_length=1)
    note: str = ""


class Resolve(ActorNote):
    resolution: str = Field(min_length=1)


class FalseAlert(ActorNote):
    reason: str = Field(min_length=1)


class Feedback(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    technician_verdict: Literal["CONFIRMED_ISSUE", "FALSE_ALERT", "INCONCLUSIVE"]
    recommendation_feedback: Literal["USEFUL", "PARTIALLY_USEFUL", "NOT_USEFUL", "NOT_AVAILABLE"]
    note: str = ""


def install(app, verifier, repository):
    from backend.shared.alerts.domain import AlertError

    def identity(request, mutate=False):
        return verifier.verify(
            request.headers.get("Authorization"),
            allowed_roles={"TECHNICIAN", "ADMIN"} if mutate else None,
        )

    def call(request, operation):
        try:
            return operation()
        except AlertError as exc:
            raise RegistryFailure(exc.status, exc.code) from None
        except (psycopg.Error, ValueError):
            request.state.failed_dependency = "business_db"
            raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE") from None

    @app.get("/api/v1/alerts")
    def alerts(
        request: Request,
        status: Literal[
            "OPEN", "ACKNOWLEDGED", "IN_PROGRESS", "ESCALATED", "RESOLVED", "FALSE_ALERT"
        ]
        | None = None,
        severity: Literal["WATCH", "HIGH", "CRITICAL"] | None = None,
        machine_id: str | None = None,
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ):
        principal = identity(request)
        return call(
            request,
            lambda: repository.list(
                principal.machines,
                status=status,
                severity=severity,
                machine_id=machine_id,
                limit=limit,
                offset=offset,
            ),
        )

    @app.get("/api/v1/alerts/{alert_id:path}")
    def detail(alert_id: str, request: Request):
        principal = identity(request)
        return call(request, lambda: repository.detail(alert_id, principal.machines))

    @app.post("/api/v1/alerts/{alert_id:path}/{action}")
    def action(alert_id: str, action: str, request: Request, body: dict):
        principal = identity(request, True)
        if action not in {"ack", "start", "escalate", "resolve", "false-alert", "feedback"}:
            raise RegistryFailure(404, "NOT_FOUND")
        model = (
            Feedback
            if action == "feedback"
            else Resolve
            if action == "resolve"
            else FalseAlert
            if action == "false-alert"
            else ActorNote
        )
        try:
            value = model.model_validate(body).model_dump()
        except ValidationError:
            raise RegistryFailure(422, "VALIDATION_FAILED") from None
        if action == "feedback":
            from fastapi.responses import JSONResponse

            result = call(
                request,
                lambda: repository.feedback(alert_id, principal.machines, principal.subject, value),
            )
            return JSONResponse(result, status_code=201)
        if value.pop("actor_id") != principal.subject:
            raise RegistryFailure(403, "FORBIDDEN")
        return call(
            request,
            lambda: repository.action(
                alert_id, action, principal.machines, principal.subject, value
            ),
        )
