"""Bounded delivery retries preserve payload/idempotency and correlation identity."""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal
from uuid import uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from simulator.fault_injection import FaultMode
from simulator.logging import log


class ProcessedResponse(BaseModel):
    model_config = ConfigDict(strict=True)
    event_id: str
    machine_id: str
    validation_status: Literal["VALID"]
    processing_status: Literal["COMPLETED"]
    machine_state: Literal["NORMAL", "WATCH", "HIGH", "CRITICAL"]
    prediction_id: str = Field(min_length=1)
    alert_id: str | None
    correlation_id: str


class ValidationIssue(BaseModel):
    model_config = ConfigDict(strict=True)
    field: str
    code: str
    message: str


class InvalidResponse(BaseModel):
    model_config = ConfigDict(strict=True)
    event_id: str
    machine_id: str
    validation_status: Literal["INVALID"]
    machine_state: Literal["INVALID"]
    errors: list[ValidationIssue] = Field(min_length=1)
    prediction_id: None
    alert_id: None
    correlation_id: str


class ValidationFailure(BaseModel):
    model_config = ConfigDict(strict=True)
    code: Literal["VALIDATION_FAILED"]
    message: str
    retryable: Literal[False]
    details: dict[str, Any]
    request_id: str
    correlation_id: str


@dataclass(frozen=True)
class SendResult:
    outcome: Literal["accepted", "rejected", "failed"]
    status_code: int | None
    attempts: int
    latency_ms: float
    error: str | None = None
    response: dict[str, Any] | None = None


class BackendClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 5,
        max_retries: int = 2,
        retry_backoff_ms: float = 100,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.endpoint = f"{base_url.rstrip('/')}/api/v1/sensor-readings"
        self.client = httpx.Client(
            timeout=timeout_seconds,
            transport=transport,
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.sleep = sleep

    def __enter__(self) -> "BackendClient":
        return self

    def __exit__(self, *args) -> None:
        self.client.close()

    def send(
        self,
        payload: dict[str, Any],
        *,
        correlation_id: str | None = None,
        fault: FaultMode = FaultMode.NONE,
    ) -> SendResult:
        correlation_id = correlation_id or f"cor_{uuid4()}"
        started = time.perf_counter()
        for attempt in range(self.max_retries + 1):
            request_id = f"req_{uuid4()}"
            attempt_started = time.perf_counter()
            status_code = None
            body = None
            error = None
            outcome = "failed"
            retryable = False
            try:
                # Inject a first-attempt delivery failure without modifying the event.
                if attempt == 0 and fault == FaultMode.NETWORK_TIMEOUT:
                    raise httpx.ReadTimeout("Injected first-attempt timeout")
                if attempt == 0 and fault == FaultMode.NETWORK_DISCONNECT:
                    raise httpx.ConnectError("Injected first-attempt disconnect")
                response = self.client.post(
                    self.endpoint,
                    json=payload,
                    headers={"X-Request-ID": request_id, "X-Correlation-ID": correlation_id},
                )
                status_code = response.status_code
                try:
                    parsed = response.json()
                    body = parsed if isinstance(parsed, dict) else None
                except ValueError:
                    pass
                if 200 <= status_code < 300:
                    ack = ProcessedResponse.model_validate(body)
                    self._check_identity(ack, payload, correlation_id)
                    outcome = "accepted"
                elif status_code == 422:
                    if body and body.get("validation_status") == "INVALID":
                        ack = InvalidResponse.model_validate(body)
                        self._check_identity(ack, payload, correlation_id)
                        outcome, error = "rejected", "VALIDATION_FAILED"
                    elif (
                        body
                        and isinstance(body.get("error"), dict)
                        and body["error"].get("code") == "VALIDATION_FAILED"
                    ):
                        error_body = ValidationFailure.model_validate(body["error"])
                        if error_body.correlation_id != correlation_id:
                            raise ValueError("Error response has mismatched correlation identity")
                        outcome, error = "rejected", "VALIDATION_FAILED"
                    else:
                        error = "INVALID_BACKEND_RESPONSE"
                else:
                    envelope = body.get("error", {}) if body else {}
                    if not isinstance(envelope, dict):
                        envelope = {}
                    error = str(envelope.get("code", f"HTTP_{status_code}"))
                    retryable = status_code in {429, 500, 502, 503, 504}
                    if envelope.get("retryable") is False:
                        retryable = False
            except httpx.RequestError as exc:
                error = (
                    "NETWORK_TIMEOUT"
                    if isinstance(exc, httpx.TimeoutException)
                    else "NETWORK_ERROR"
                )
                retryable = True
            except (ValueError, ValidationError):
                error = "INVALID_BACKEND_RESPONSE"
            log(
                "sensor_delivery_attempt",
                level=logging.INFO if outcome == "accepted" else logging.WARNING,
                event_id=payload.get("event_id"),
                machine_id=payload.get("machine_id"),
                request_id=request_id,
                correlation_id=correlation_id,
                attempt=attempt + 1,
                status_code=status_code,
                latency_ms=round((time.perf_counter() - attempt_started) * 1000, 3),
                outcome=outcome,
                error=error,
            )
            if retryable and attempt < self.max_retries:
                self.sleep(min(self.retry_backoff_ms / 1000 * 2**attempt, 5.0))
                continue
            return SendResult(
                outcome,
                status_code,
                attempt + 1,
                (time.perf_counter() - started) * 1000,
                error,
                body,
            )
        raise AssertionError("Delivery loop must return a result")

    @staticmethod
    def _check_identity(
        ack: ProcessedResponse | InvalidResponse,
        payload: dict,
        correlation_id: str,
    ) -> None:
        if (
            ack.event_id != payload["event_id"]
            or ack.machine_id != payload["machine_id"]
            or ack.correlation_id != correlation_id
        ):
            raise ValueError("Backend response has mismatched event/trace identity")
