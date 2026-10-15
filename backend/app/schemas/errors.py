from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

type ErrorCode = Literal[
    "INVALID_REQUEST",
    "VALIDATION_FAILED",
    "EVENT_CONFLICT",
    "MACHINE_NOT_FOUND",
    "ALERT_NOT_FOUND",
    "INVALID_ALERT_TRANSITION",
    "MODEL_NOT_READY",
    "ML_UNAVAILABLE",
    "ML_TIMEOUT",
    "RAG_UNAVAILABLE",
    "INTERNAL_ERROR",
    "UNAUTHORIZED",
    "FORBIDDEN",
    "DRIFT_REPORT_NOT_FOUND",
    "DEPENDENCY_UNAVAILABLE",
]


class ApiError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: ErrorCode
    message: str = Field(min_length=1)
    retryable: bool
    details: dict[str, Any]
    request_id: str | None = Field(default=None, min_length=1)
    correlation_id: str | None = Field(default=None, min_length=1)


class ErrorEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ApiError

