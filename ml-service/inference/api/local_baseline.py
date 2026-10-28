"""API baseline chỉ dành cho thử nghiệm local trên loopback."""

import hashlib
import json
import logging
import math
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Literal
from uuid import uuid4

import joblib
import pandas as pd
import sklearn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field


LOGGER = logging.getLogger("uvicorn.error")

FEATURES = [
    "product_type",
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min",
]

STATE = {"pipeline": None, "metadata": None}


class StrictBody(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        allow_inf_nan=False,
    )


class Features(StrictBody):
    product_type: Literal["L", "M", "H"]
    air_temperature_k: float
    process_temperature_k: float
    rotational_speed_rpm: float = Field(gt=0)
    torque_nm: float
    tool_wear_min: float = Field(ge=0)


class PredictionRequest(StrictBody):
    event_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    features: Features


@asynccontextmanager
async def lifespan(app):
    if os.environ.get("MAINTAI_LOCAL_ONLY") != "1":
        raise RuntimeError(
            "Local prototype requires MAINTAI_LOCAL_ONLY=1"
        )

    try:
        model_dir = Path(os.environ["MODEL_DIR"]).resolve()
        metadata = json.loads(
            (model_dir / "metadata.json").read_text(encoding="utf-8")
        )
        artifact = model_dir / "pipeline.joblib"

        actual_hash = hashlib.sha256(artifact.read_bytes()).hexdigest()
        if actual_hash != metadata["pipeline_sha256"]:
            raise ValueError("Pipeline checksum mismatch")

        if metadata["features"] != FEATURES:
            raise ValueError("Feature list mismatch")

        if metadata["sklearn_version"] != sklearn.__version__:
            raise ValueError("Scikit-learn version mismatch")

        threshold = metadata["threshold"]
        if (
            type(threshold) not in (int, float)
            or not math.isfinite(threshold)
            or not 0 <= threshold <= 1
        ):
            raise ValueError("Invalid model threshold")

        for field in ["model_version", "preprocessor_version"]:
            if not isinstance(metadata[field], str) or not metadata[field]:
                raise ValueError(f"Invalid {field}")

        # Chỉ load artifact tin cậy do chính ta tạo.
        pipeline = joblib.load(artifact)

        if list(pipeline.feature_names_in_) != FEATURES:
            raise ValueError("Loaded pipeline feature order mismatch")

        if list(pipeline.classes_) != [0, 1]:
            raise ValueError("Unexpected model classes")

        STATE["pipeline"] = pipeline
        STATE["metadata"] = metadata
        LOGGER.info("Local baseline loaded: %s", metadata["model_version"])

    except Exception:
        # Giữ model unavailable; không fallback sang artifact khác.
        LOGGER.exception("Baseline could not be loaded")

    yield

    STATE["pipeline"] = None
    STATE["metadata"] = None


app = FastAPI(
    title="MaintAI local baseline",
    description="Local integration prototype; not an approved release.",
    lifespan=lifespan,
)


@app.middleware("http")
async def tracing(request: Request, call_next):
    request.state.request_id = (
        request.headers.get("X-Request-ID") or f"req_{uuid4().hex}"
    )
    request.state.correlation_id = (
        request.headers.get("X-Correlation-ID") or f"cor_{uuid4().hex}"
    )

    response = await call_next(request)
    response.headers["X-Request-ID"] = request.state.request_id
    response.headers["X-Correlation-ID"] = request.state.correlation_id
    return response


def error_response(request, status, code, message, retryable):
    return JSONResponse(
        status_code=status,
        content={
            "error": {
                "code": code,
                "message": message,
                "retryable": retryable,
                "details": {},
                "request_id": request.state.request_id,
                "correlation_id": request.state.correlation_id,
            }
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError):
    malformed_json = any(
        item["type"] == "json_invalid" for item in exc.errors()
    )
    return error_response(
        request,
        400 if malformed_json else 422,
        "INVALID_REQUEST" if malformed_json else "VALIDATION_FAILED",
        "Malformed JSON" if malformed_json else "Invalid prediction request",
        False,
    )


@app.post("/api/v1/predict")
def predict(body: PredictionRequest, request: Request):
    pipeline = STATE["pipeline"]
    metadata = STATE["metadata"]

    if pipeline is None:
        return error_response(
            request, 503, "MODEL_NOT_READY",
            "Model artifact is not ready", True,
        )

    try:
        start = perf_counter()

        frame = pd.DataFrame(
            [body.features.model_dump()],
            columns=FEATURES,
        )
        probability = float(pipeline.predict_proba(frame)[0, 1])

        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError("Model returned invalid probability")

        threshold = metadata["threshold"]

        return {
            "prediction_id": f"pred_{uuid4().hex}",
            "event_id": body.event_id,
            "prediction": (
                "FAILURE" if probability >= threshold else "NORMAL"
            ),
            "failure_probability": probability,
            "model_version": metadata["model_version"],
            "preprocessor_version": metadata["preprocessor_version"],
            "model_threshold": threshold,
            "inference_ms": (perf_counter() - start) * 1000,
            "predicted_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception:
        LOGGER.exception(
            "Inference failed; request_id=%s",
            request.state.request_id,
        )
        return error_response(
            request, 500, "INTERNAL_ERROR",
            "Inference failed", False,
        )