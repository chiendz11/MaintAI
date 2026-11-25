"""Ingestion ACK transport; no prediction, risk or alert is awaited here."""

import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Literal, Protocol
from uuid import uuid4

import httpx
import paho.mqtt.client as mqtt
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


class DeliveryTransport(Protocol):
    def send(
        self,
        payload: dict[str, Any],
        *,
        correlation_id: str | None = None,
        fault: FaultMode = FaultMode.NONE,
    ) -> SendResult: ...

    def __enter__(self): ...

    def __exit__(self, *args): ...


class MqttTransport:
    """Durable QoS 1 MQTT delivery transport backed by paho-mqtt v2."""

    def __init__(
        self,
        host: str,
        *,
        port: int = 1883,
        topic_prefix: str = "maintai/v1/site/local/machines",
        qos: Literal[1] = 1,
        timeout_seconds: float = 5.0,
        observe: Callable[[str], None] = lambda outcome: None,
        client=None,
    ):
        self.host = host
        self.port = port
        self.topic_prefix = topic_prefix.rstrip("/")
        self.status_topic = f"{self.topic_prefix}/controller/status"
        self.qos = qos
        self.timeout_seconds = timeout_seconds
        self.observe = observe
        self._lock = threading.Lock()
        self._connected = threading.Event()
        self._started = False
        self._closed = False
        if client is None:
            cli = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"maintai-sensor-{uuid4().hex[:8]}",
                protocol=mqtt.MQTTv311,
            )
            cli.will_set(self.status_topic, payload=b"OFFLINE", qos=self.qos, retain=True)
            cli.reconnect_delay_set(min_delay=1, max_delay=10)
            cli.on_connect = self._on_connect
            cli.on_disconnect = self._on_disconnect
            self.client = cli
        else:
            self.client = client

    def _on_connect(self, client, _userdata, _flags, reason_code, _properties=None):
        if reason_code == 0 or getattr(reason_code, "value", reason_code) == 0:
            self._connected.set()
            try:
                client.publish(self.status_topic, payload=b"ONLINE", qos=self.qos, retain=True)
            except Exception:
                pass

    def _on_disconnect(self, _client, _userdata, _disconnect_flags, _reason_code, _properties=None):
        self._connected.clear()

    def _ensure_connected(self):
        if self._started:
            return
        with self._lock:
            if self._started or self._closed:
                return
            self.client.connect_async(self.host, self.port, keepalive=30)
            self.client.loop_start()
            self._started = True
        self._connected.wait(timeout=self.timeout_seconds)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        with self._lock:
            self._closed = True
            if self._started:
                try:
                    info = self.client.publish(
                        self.status_topic, payload=b"OFFLINE", qos=self.qos, retain=True
                    )
                    info.wait_for_publish(timeout=1.0)
                except Exception:
                    pass
                try:
                    self.client.disconnect()
                finally:
                    self.client.loop_stop()

    def send(
        self,
        payload: dict[str, Any],
        *,
        correlation_id: str | None = None,
        fault: FaultMode = FaultMode.NONE,
    ) -> SendResult:
        correlation_id = correlation_id or payload.get("correlation_id") or f"cor_{uuid4()}"
        started = time.perf_counter()
        request_id = f"req_{uuid4()}"
        wire = {**payload, "correlation_id": correlation_id}
        encoded = json.dumps(wire, separators=(",", ":"), allow_nan=False).encode("utf-8")
        topic = f"{self.topic_prefix}/{payload['machine_id']}/telemetry"
        outcome, error, retryable = "failed", None, True
        try:
            if fault == FaultMode.NETWORK_TIMEOUT:
                raise TimeoutError("Injected MQTT timeout")
            if fault == FaultMode.NETWORK_DISCONNECT:
                raise ConnectionError("Injected MQTT disconnect")
            self._ensure_connected()
            info = self.client.publish(topic, payload=encoded, qos=self.qos)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise ConnectionError(f"MQTT publish failed rc={info.rc}")
            info.wait_for_publish(timeout=self.timeout_seconds)
            if not info.is_published():
                raise TimeoutError("MQTT PUBACK not received before timeout")
            outcome, retryable = "accepted", False
        except TimeoutError:
            error = "NETWORK_TIMEOUT"
        except Exception as exc:
            error = "MQTT_PUBLISH_FAILED" if "rc=" in str(exc) else "NETWORK_ERROR"
        label = "ACCEPTED" if outcome == "accepted" else "RETRY"
        self.observe(label)
        latency_ms = (time.perf_counter() - started) * 1000
        log(
            "sensor_delivery_attempt",
            level=logging.INFO if outcome == "accepted" else logging.WARNING,
            transport="mqtt",
            topic=topic,
            event_id=payload.get("event_id"),
            machine_id=payload.get("machine_id"),
            request_id=request_id,
            correlation_id=correlation_id,
            attempt=1,
            latency_ms=round(latency_ms, 3),
            outcome=outcome,
            error=error,
        )
        return SendResult(
            outcome,
            202 if outcome == "accepted" else None,
            1,
            latency_ms,
            error,
            None,
            retryable,
            None,
        )


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
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
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
