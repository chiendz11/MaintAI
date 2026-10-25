"""Ingestion ACK transport; no prediction, risk or alert is awaited here."""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Literal
from uuid import uuid4

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, HttpUrl, ValidationError

from simulator.control.auth import AuthError
from simulator.fault_injection import FaultMode
from simulator.logging import log


class IngestAck(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    event_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    ingestion_status: Literal["ACCEPTED"]
    processing_status: Literal["PENDING"]
    server_accepted_at: AwareDatetime
    status_url: HttpUrl
    correlation_id: str = Field(min_length=1)


@dataclass(frozen=True)
class SendResult:
    outcome: Literal["accepted", "rejected", "failed"]
    status_code: int | None
    attempts: int
    latency_ms: float
    error: str | None = None
    response: dict[str, Any] | None = None
    retryable: bool = False
    retry_after: float | None = None


def retry_after(value: str | None, now: float | None = None) -> float | None:
    try:
        if value is None:
            return None
        delay = (
            float(value)
            if value.isdigit()
            else parsedate_to_datetime(value).timestamp() - (time.time() if now is None else now)
        )
        return max(0, min(delay, 300))
    except (ValueError, TypeError, OverflowError):
        return None


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
        credentials: Callable[[], str | None] = lambda: None,
        observe: Callable[[str], None] = lambda outcome: None,
    ):
        self.endpoint = f"{base_url.rstrip('/')}/api/v1/sensor-readings"
        self.client = httpx.Client(
            timeout=timeout_seconds,
            transport=transport,
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )
        self.max_retries, self.retry_backoff_ms, self.sleep = max_retries, retry_backoff_ms, sleep
        self.credentials, self.observe = credentials, observe

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()

    def send(
        self,
        payload: dict[str, Any],
        *,
        correlation_id: str | None = None,
        fault: FaultMode = FaultMode.NONE,
    ) -> SendResult:
        correlation_id = correlation_id or payload.get("correlation_id") or f"cor_{uuid4()}"
        started = time.perf_counter()
        for attempt in range(self.max_retries + 1):
            request_id = f"req_{uuid4()}"
            outcome, error, body, status, retryable, wait = "failed", None, None, None, False, None
            try:
                token = self.credentials()
                headers = {"X-Request-ID": request_id, "X-Correlation-ID": correlation_id}
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                if attempt == 0 and fault == FaultMode.NETWORK_TIMEOUT:
                    raise httpx.ReadTimeout("Injected timeout")
                if attempt == 0 and fault == FaultMode.NETWORK_DISCONNECT:
                    raise httpx.ConnectError("Injected disconnect")
                response = self.client.post(self.endpoint, json=payload, headers=headers)
                status = response.status_code
                wait = retry_after(response.headers.get("Retry-After"))
                try:
                    parsed = response.json()
                    body = parsed if isinstance(parsed, dict) else None
                except ValueError:
                    pass
                if status == 202:
                    ack = IngestAck.model_validate_json(response.content)
                    if (
                        ack.event_id != payload.get("event_id")
                        or ack.machine_id != payload.get("machine_id")
                        or ack.correlation_id != correlation_id
                    ):
                        raise ValueError("Mismatched ACK identity")
                    outcome = "accepted"
                elif 200 <= status < 300:
                    error, retryable = "INVALID_INGESTION_RESPONSE", True
                else:
                    envelope = body.get("error", {}) if body else {}
                    envelope = envelope if isinstance(envelope, dict) else {}
                    error = str(envelope.get("code", f"HTTP_{status}"))
                    if status in (400, 422):
                        if envelope.get("correlation_id") not in (
                            None,
                            correlation_id,
                        ) or envelope.get("request_id") not in (None, request_id):
                            error = "INVALID_INGESTION_RESPONSE"
                        elif (
                            envelope.get("retryable") is False
                            and isinstance(envelope.get("code"), str)
                            and isinstance(envelope.get("message"), str)
                        ):
                            outcome = "rejected"
                        else:
                            error = "INVALID_INGESTION_RESPONSE"
                    retryable = (
                        status in (408, 429, 500, 502, 503, 504)
                        and envelope.get("retryable") is not False
                    )
            except AuthError:
                error = "INGESTION_CREDENTIAL_UNAVAILABLE"
            except httpx.RequestError as exc:
                error = (
                    "NETWORK_TIMEOUT"
                    if isinstance(exc, httpx.TimeoutException)
                    else "NETWORK_ERROR"
                )
                retryable = True
            except (ValueError, ValidationError):
                error, retryable = "INVALID_INGESTION_RESPONSE", True
            label = (
                "ACCEPTED"
                if outcome == "accepted"
                else "REJECTED"
                if outcome == "rejected"
                else "AUTH_ERROR"
                if status in (401, 403) or error == "INGESTION_CREDENTIAL_UNAVAILABLE"
                else "RETRY"
                if retryable
                else "OTHER"
            )
            self.observe(label)
            log(
                "sensor_delivery_attempt",
                level=logging.INFO if outcome == "accepted" else logging.WARNING,
                event_id=payload.get("event_id"),
                machine_id=payload.get("machine_id"),
                request_id=request_id,
                correlation_id=correlation_id,
                attempt=attempt + 1,
                status_code=status,
                latency_ms=round((time.perf_counter() - started) * 1000, 3),
                outcome=outcome,
                error=error,
            )
            if retryable and attempt < self.max_retries:
                self.sleep(
                    wait if wait is not None else min(self.retry_backoff_ms / 1000 * 2**attempt, 5)
                )
                continue
            return SendResult(
                outcome,
                status,
                attempt + 1,
                (time.perf_counter() - started) * 1000,
                error,
                body,
                retryable,
                wait,
            )
        raise AssertionError("Unreachable")
