"""HTTP boundary for backend sensor ingestion."""

from collections.abc import Awaitable
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.schemas.errors import ApiError, ErrorCode, ErrorEnvelope
from app.schemas.sensor import (
    SensorCompletedResponse,
    SensorDependencyFailure,
    SensorInvalidResponse,
    SensorReadingRequest,
    ValidationIssue,
)
from app.services.sensor_ingestion import (
    EventConflict,
    HttpOutcome,
    MachineNotFound,
    PendingEvent,
    SensorIngestionService,
)

SENSOR_PATH = "/api/v1/sensor-readings"


def _error_response(
    request: Request,
    status_code: int,
    code: ErrorCode,
    message: str,
    *,
    retryable: bool = False,
) -> JSONResponse:
    body = ErrorEnvelope(
        error=ApiError(
            code=code,
            message=message,
            retryable=retryable,
            details={},
            request_id=request.state.request_id,
            correlation_id=request.state.correlation_id,
        )
    )
    return JSONResponse(status_code=status_code, content=body.model_dump(exclude_none=True))


def _validation_issue(error: dict[str, Any]) -> ValidationIssue:
    location = error.get("loc", ())
    path = location[1:] if location and location[0] == "body" else location
    field = ".".join(str(part) for part in path) or "body"
    error_type = error.get("type", "invalid_value")
    if error_type in {"float_type", "int_type", "string_type", "datetime_type"}:
        code = "INVALID_TYPE"
    elif error_type == "literal_error":
        code = "UNSUPPORTED_VALUE"
    elif error_type in {"greater_than", "greater_than_equal", "less_than", "less_than_equal"}:
        code = "OUT_OF_RANGE"
    elif error_type == "missing":
        code = "REQUIRED_FIELD"
    elif error_type == "extra_forbidden":
        code = "UNEXPECTED_FIELD"
    else:
        code = "INVALID_VALUE"
    return ValidationIssue(
        field=field, code=code, message=error.get("msg") or "Invalid value"
    )


def create_app(ingestion_service: SensorIngestionService | None = None) -> FastAPI:
    app = FastAPI(title="MaintAI Backend")

    @app.middleware("http")
    async def trace_headers(request: Request, call_next):
        request.state.request_id = request.headers.get("X-Request-ID") or f"req_{uuid4()}"
        request.state.correlation_id = (
            request.headers.get("X-Correlation-ID") or f"cor_{uuid4()}"
        )
        content_type = request.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if (
            request.method == "POST"
            and request.url.path == SENSOR_PATH
            and content_type != "application/json"
        ):
            response = _error_response(
                request, 400, "INVALID_REQUEST", "Content-Type must be application/json"
            )
        else:
            response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["X-Correlation-ID"] = request.state.correlation_id
        return response

    async def execute(request: Request, operation: Awaitable[HttpOutcome]) -> JSONResponse:
        try:
            outcome = await operation
        except EventConflict:
            return _error_response(request, 409, "EVENT_CONFLICT", "Event ID has a different payload")
        except MachineNotFound:
            return _error_response(request, 404, "MACHINE_NOT_FOUND", "Machine not found")
        except PendingEvent:
            return _error_response(request, 500, "INTERNAL_ERROR", "Event outcome is pending")
        except Exception:
            # No stack trace or connection strings in the HTTP response.
            return _error_response(request, 500, "INTERNAL_ERROR", "Sensor ingestion failed")
        return JSONResponse(status_code=outcome.status_code, content=outcome.body)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError):
        if request.url.path != SENSOR_PATH:
            return await request_validation_exception_handler(request, exc)

        if any(error.get("type") == "json_invalid" for error in exc.errors()):
            return _error_response(request, 400, "INVALID_REQUEST", "Malformed JSON request")

        raw = exc.body
        identifiable = (
            isinstance(raw, dict)
            and isinstance(raw.get("event_id"), str)
            and bool(raw["event_id"])
            and isinstance(raw.get("machine_id"), str)
            and bool(raw["machine_id"])
        )
        if not identifiable:
            return _error_response(request, 422, "VALIDATION_FAILED", "Validation failed")

        if ingestion_service is None:
            return _error_response(request, 500, "INTERNAL_ERROR", "Sensor ingestion is not configured")
        issues = [_validation_issue(error) for error in exc.errors()]
        return await execute(
            request,
            ingestion_service.reject_invalid(
                raw, issues, request.state.correlation_id, request.state.request_id
            ),
        )

    @app.get("/health/live")
    async def live():
        return {"status": "UP", "service": "backend", "version": "0.1.0"}

    @app.post(
        SENSOR_PATH,
        response_model=SensorCompletedResponse,
        responses={
            400: {"model": ErrorEnvelope},
            404: {"model": ErrorEnvelope},
            409: {"model": ErrorEnvelope},
            422: {"model": SensorInvalidResponse | ErrorEnvelope},
            500: {"model": ErrorEnvelope},
            503: {"model": SensorDependencyFailure},
            504: {"model": SensorDependencyFailure},
        },
    )
    async def ingest_sensor_reading(request: Request, payload: SensorReadingRequest):
        if ingestion_service is None:
            return _error_response(request, 500, "INTERNAL_ERROR", "Sensor ingestion is not configured")
        return await execute(
            request,
            ingestion_service.ingest(
                payload, request.state.correlation_id, request.state.request_id
            ),
        )

    return app


app = create_app()
