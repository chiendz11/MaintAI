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
        try:
            loopback = url.host == "localhost" or ipaddress.ip_address(url.host).is_loopback
        except ValueError:
            loopback = False
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
        self.url = str(url).rstrip("/") + "/api/v1/predict"
        self.timeout_seconds, self.max_response_bytes = timeout_seconds, max_response_bytes
        self._headers = {"Authorization": f"Bearer {bearer_token}"} if bearer_token else {}

    async def predict(
        self,
        *,
        event_id: str,
        machine_id: str,
        features: dict,
        correlation_id: str,
    ) -> PredictionAttempt:
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
        body = {"event_id": event_id, "machine_id": machine_id, "features": dq.normalized_features}
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
                            code = (
                                "ML_AUTH_FAILED"
                                if status in {401, 403}
                                else "ML_REQUEST_REJECTED"
                                if status in {400, 422}
                                else "ML_HTTP_ERROR"
                            )
                            return PredictionAttempt(None, code, attempt)
                        # HTTPX's decoded iterator can allocate the entire expanded
                        # transport chunk before yielding. Refuse compression and
                        # consume raw identity bytes instead of decompressing them.
                        encoding = (
                            response.headers.get("Content-Encoding", "identity").strip().lower()
                        )
                        if encoding not in {"", "identity"}:
                            return PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
                        raw = bytearray()
                        if response.is_stream_consumed:
                            if len(response.content) > self.max_response_bytes:
                                return PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
                            raw.extend(response.content)
                        else:
                            async for chunk in response.aiter_raw(chunk_size=16384):
                                if len(raw) + len(chunk) > self.max_response_bytes:
                                    return PredictionAttempt(None, "ML_INVALID_RESPONSE", attempt)
                                raw.extend(chunk)
                        try:
                            payload = strict_json(bytes(raw))
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
