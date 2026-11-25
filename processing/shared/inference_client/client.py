"""Bounded ML HTTP client for the existing PredictionResponse baseline.

This module does not invent missing model lineage. The worker must validate the
machine registry before calling it and resolve verified identity before emitting
a completed Kafka outcome. The caller owns the AsyncClient lifecycle.
"""

from __future__ import annotations

import asyncio
import ipaddress
import math
from dataclasses import dataclass
from typing import Literal

import httpx
from maintai_messaging.contracts import canonical_json, strict_json
from maintai_messaging.errors import ContractViolation
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator

from processing.shared.data_quality.validator import FeatureValidator


class PredictionResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, allow_inf_nan=False)

    prediction_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    prediction: Literal["NORMAL", "FAILURE"]
    failure_probability: float = Field(ge=0, le=1)
    model_version: str = Field(min_length=1)
    preprocessor_version: str = Field(min_length=1)
    model_threshold: float = Field(ge=0, le=1)
    inference_ms: float = Field(ge=0)
    predicted_at: AwareDatetime

    @model_validator(mode="after")
    def consistent_label(self):
        label = "FAILURE" if self.failure_probability >= self.model_threshold else "NORMAL"
        if self.prediction != label:
            raise ValueError("Prediction label does not match the model threshold")
        return self


@dataclass(frozen=True)
class PredictionAttempt:
    response: PredictionResponse | None
    failure_code: str | None
    attempts: int


INTERNAL_HOSTS = {
    "localhost",
    "127.0.0.1",
    "::1",
    "kafka",
    "postgres",
    "keycloak",
    "business-api",
    "business-consumer",
    "processing-worker",
    "processing-read-api",
    "processing-outbox-relay",
    "ingestion-api",
    "sensor",
    "storage-consumer",
    "ml-service",
    "ml-service-shadow",
    "ml-service-mirror",
    "mlflow",
    "rag-service",
    "frontend",
    "simulator-ui",
    "idp-gateway",
}


def is_loopback(host: str) -> bool:
    if not host:
        return False
    lowered = host.strip().lower()
    if (
        lowered in INTERNAL_HOSTS
        or lowered.endswith(".svc.cluster.local")
        or lowered.endswith(".local")
        or lowered.endswith(".localhost")
        or lowered.endswith(".internal")
    ):
        return True
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class InferenceClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str,
        feature_validator: FeatureValidator,
        bearer_token: str | None = None,
        local_fixture: bool = False,
        timeout_seconds: float = 2,
        max_response_bytes: int = 65536,
    ):
        url = httpx.URL(base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.host
            or url.userinfo
            or url.query
            or url.fragment
        ):
            raise ValueError("ML URL must be an explicit HTTP(S) origin without credentials")
        if url.path not in {"", "/"}:
            raise ValueError("ML base URL must be an origin")
        loopback = is_loopback(url.host)
        if local_fixture and not loopback:
            raise ValueError("Anonymous ML fixture is loopback-only")
        if not bearer_token and not local_fixture:
            raise ValueError("ML service credentials are required")
        if bearer_token is not None and (
            not isinstance(bearer_token, str)
            or not bearer_token.strip()
            or "\r" in bearer_token
            or "\n" in bearer_token
        ):
            raise ValueError("Invalid ML service credential")
        if (
            type(timeout_seconds) not in (int, float)
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 2
        ):
            raise ValueError("ML attempt deadline must be positive and at most two seconds")
        if type(max_response_bytes) is not int or max_response_bytes <= 0:
            raise ValueError("ML response byte budget must be positive")
        self.http, self.validator = http, feature_validator
        base = str(url).rstrip("/")
        self.url = base + "/api/v1/predict"
        self.batch_url = base + "/api/v1/predict/batch"
        self.timeout_seconds, self.max_response_bytes = timeout_seconds, max_response_bytes
        self._headers = {"Authorization": f"Bearer {bearer_token}"} if bearer_token else {}

    @staticmethod
    def _error_code_for_status(status: int) -> str:
        if status in {401, 403}:
            return "ML_AUTH_FAILED"
        if status in {400, 422}:
            return "ML_REQUEST_REJECTED"
        return "ML_HTTP_ERROR"

    async def _read_bounded_body(self, response, max_bytes: int) -> bytes | None:
        encoding = response.headers.get("Content-Encoding", "identity").strip().lower()
        if encoding not in {"", "identity"}:
            return None
        raw = bytearray()
        if response.is_stream_consumed:
            if len(response.content) > max_bytes:
                return None
            raw.extend(response.content)
            return bytes(raw)
        async for chunk in response.aiter_raw(chunk_size=16384):
            if len(raw) + len(chunk) > max_bytes:
                return None
            raw.extend(chunk)
        return bytes(raw)

    def _validate_single_input(
        self, event_id: str, machine_id: str, features: dict, correlation_id: str
    ) -> dict:
        if any(
            not isinstance(value, str) or not value
            for value in (event_id, machine_id, correlation_id)
        ):
            raise ValueError("Event, machine and correlation identities are required")
        if "\r" in correlation_id or "\n" in correlation_id:
            raise ValueError("Invalid correlation header")
        dq = self.validator.validate(features)
        if dq.status != "VALID":
            raise ValueError("Only validated features may be sent to ML")
        return {"event_id": event_id, "machine_id": machine_id, "features": dq.normalized_features}

    async def predict(
        self,
        *,
        event_id: str,
        machine_id: str,
        features: dict,
        correlation_id: str,
    ) -> PredictionAttempt:
        body = self._validate_single_input(event_id, machine_id, features, correlation_id)
        headers = {
            **self._headers,
            "X-Correlation-ID": correlation_id,
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
        for attempt in (1, 2):
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    async with self.http.stream(
                        "POST",
                        self.url,
                        json=body,
                        headers=headers,
                        timeout=self.timeout_seconds,
                        follow_redirects=False,
                    ) as response:
                        status = response.status_code
                        if status != 200:
                            if status in {500, 502, 503, 504}:
                                if attempt == 1:
                                    continue
                                return PredictionAttempt(None, "ML_UNAVAILABLE", attempt)
                            return PredictionAttempt(
                                None, self._error_code_for_status(status), attempt
                            )
                        raw = await self._read_bounded_body(response, self.max_response_bytes)
                        if raw is None:
                            return PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
                        try:
                            payload = strict_json(raw)
                            prediction = PredictionResponse.model_validate_json(
                                canonical_json(payload)
                            )
                        except (ContractViolation, ValidationError):
                            return PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
                        if prediction.event_id != event_id:
                            return PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
                        return PredictionAttempt(prediction, None, attempt)
            except (TimeoutError, httpx.TimeoutException):
                code = "ML_TIMEOUT"
            except httpx.DecodingError:
                return PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
            except httpx.TransportError:
                code = "ML_CONNECTION_FAILED"
            if attempt == 2:
                return PredictionAttempt(None, code, attempt)
        raise AssertionError("Unreachable ML attempt state")

    async def predict_batch(self, items: list[dict]) -> list[PredictionAttempt]:
        if not items:
            return []
        if len(items) == 1:
            item = items[0]
            return [
                await self.predict(
                    event_id=item["event_id"],
                    machine_id=item["machine_id"],
                    features=item["features"],
                    correlation_id=item["correlation_id"],
                )
            ]
        results: list[PredictionAttempt] = []
        for offset in range(0, len(items), 64):
            chunk = items[offset : offset + 64]
            results.extend(await self._predict_batch_chunk(chunk))
        return results

    async def _predict_batch_chunk(self, chunk: list[dict]) -> list[PredictionAttempt]:
        bodies = [
            self._validate_single_input(
                item["event_id"], item["machine_id"], item["features"], item["correlation_id"]
            )
            for item in chunk
        ]
        headers = {
            **self._headers,
            "X-Correlation-ID": chunk[0]["correlation_id"],
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
        max_bytes = self.max_response_bytes * len(chunk)
        for attempt in (1, 2):
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    async with self.http.stream(
                        "POST",
                        self.batch_url,
                        json={"items": bodies},
                        headers=headers,
                        timeout=self.timeout_seconds,
                        follow_redirects=False,
                    ) as response:
                        status = response.status_code
                        if status in {404, 405}:
                            return [
                                await self.predict(
                                    event_id=item["event_id"],
                                    machine_id=item["machine_id"],
                                    features=item["features"],
                                    correlation_id=item["correlation_id"],
                                )
                                for item in chunk
                            ]
                        if status != 200:
                            if status in {500, 502, 503, 504}:
                                if attempt == 1:
                                    continue
                                return [PredictionAttempt(None, "ML_UNAVAILABLE", attempt)] * len(
                                    chunk
                                )
                            code = self._error_code_for_status(status)
                            return [PredictionAttempt(None, code, attempt)] * len(chunk)
                        raw = await self._read_bounded_body(response, max_bytes)
                        if raw is None:
                            return [PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)] * len(
                                chunk
                            )
                        try:
                            payload = strict_json(raw)
                            raw_results = (
                                payload.get("results") if isinstance(payload, dict) else None
                            )
                            if not isinstance(raw_results, list) or len(raw_results) != len(chunk):
                                return [
                                    PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
                                ] * len(chunk)
                            attempts: list[PredictionAttempt] = []
                            for req_item, res_item in zip(chunk, raw_results, strict=True):
                                prediction = PredictionResponse.model_validate_json(
                                    canonical_json(res_item)
                                )
                                if prediction.event_id != req_item["event_id"]:
                                    return [
                                        PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
                                    ] * len(chunk)
                                attempts.append(PredictionAttempt(prediction, None, attempt))
                            return attempts
                        except (ContractViolation, ValidationError):
                            return [PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)] * len(
                                chunk
                            )
            except (TimeoutError, httpx.TimeoutException):
                code = "ML_TIMEOUT"
            except httpx.DecodingError:
                return [PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)] * len(chunk)
            except httpx.TransportError:
                code = "ML_CONNECTION_FAILED"
            if attempt == 2:
                return [PredictionAttempt(None, code, attempt)] * len(chunk)
        raise AssertionError("Unreachable ML batch attempt state")
