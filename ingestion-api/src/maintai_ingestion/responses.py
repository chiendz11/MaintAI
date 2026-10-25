"""Project SDK results onto the frozen HTTP response shape."""

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import uuid4

from starlette.responses import JSONResponse

from .errors import EdgeFailure


@dataclass(frozen=True)
class RequestIdentity:
    request_id: str
    correlation_id: str


def fresh_trace():
    return RequestIdentity("req_" + uuid4().hex, "cor_" + uuid4().hex)


def resolve_request_identity(headers: Mapping[str, str]) -> RequestIdentity:
    values = []
    for key, prefix in [("x-request-id", "req_"), ("x-correlation-id", "cor_")]:
        value = headers.get(key)
        if value is None:
            value = prefix + uuid4().hex
        if not 1 <= len(value) <= 256 or any(ord(c) < 32 or ord(c) > 126 for c in value):
            raise EdgeFailure(400, "INVALID_REQUEST_IDENTITY")
        values.append(value)
    return RequestIdentity(*values)


def trace_headers(trace):
    return {"X-Request-ID": trace.request_id, "X-Correlation-ID": trace.correlation_id}


def edge_error(status, code, trace, *, retryable=False, details=None):
    messages = {
        "MALFORMED_JSON": "Request body is not valid JSON.",
        "VALIDATION_FAILED": "Sensor envelope is invalid.",
        "UNAUTHORIZED": "Valid Bearer credentials are required.",
        "FORBIDDEN": "Source is not authorized for this operation.",
        "KAFKA_UNAVAILABLE": "Kafka acknowledgement is unavailable.",
        "KAFKA_PUBLISH_UNCERTAIN": "Kafka acknowledgement is uncertain; retain event identity.",
        "PAYLOAD_TOO_LARGE": "Request exceeds the payload limit.",
        "QUARANTINE_METADATA_TOO_LARGE": "Rejection metadata exceeds the payload limit.",
        "UNSUPPORTED_MEDIA_TYPE": "Uncompressed application/json is required.",
        "REQUEST_TIMEOUT": "Request body did not arrive within the time limit.",
        "RATE_LIMITED": "Request admission capacity is full.",
        "DEPENDENCY_UNAVAILABLE": "A required dependency is unavailable.",
        "INVALID_REQUEST_IDENTITY": "Request identity headers are invalid.",
        "INVALID_REQUEST": "Request transport is invalid.",
        "NOT_FOUND": "Resource was not found.",
        "METHOD_NOT_ALLOWED": "Method is not allowed.",
        "INTERNAL_ERROR": "An internal error occurred.",
    }
    headers = trace_headers(trace)
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    if status == 429:
        headers["Retry-After"] = "1"
    return JSONResponse(
        {
            "error": {
                "code": code,
                "message": messages.get(code, "Request failed."),
                "retryable": retryable,
                "details": details or {},
                "request_id": trace.request_id,
                "correlation_id": trace.correlation_id,
            }
        },
        status_code=status,
        headers=headers,
    )


def to_http(response, trace):
    if response.status == 202:
        keys = (
            "event_id",
            "machine_id",
            "ingestion_status",
            "processing_status",
            "server_accepted_at",
            "status_url",
            "correlation_id",
        )
        body = {key: response.body[key] for key in keys}
        return JSONResponse(
            body, status_code=202, headers={**response.headers, **trace_headers(trace)}
        )
    error = response.body.get("error", {})
    details = {}
    if "rejection_id" in response.body:
        details["rejection_id"] = response.body["rejection_id"]
    return edge_error(
        response.status,
        error.get("code", "INTERNAL_ERROR"),
        trace,
        retryable=bool(error.get("retryable", False)),
        details=details,
    )
