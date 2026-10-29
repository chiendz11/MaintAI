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
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

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

# Registry riêng để các module test import API độc lập không trùng metrics.
METRICS = CollectorRegistry()

ML_REQUESTS = Counter(
    "maintai_ml_inference_requests_total",
    "Prediction requests by outcome",
    ["status"],
    registry=METRICS,
)

ML_DURATION = Histogram(
    "maintai_ml_inference_duration_seconds",
    "Preprocessing, prediction and result construction time",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
    registry=METRICS,
)

ML_PREDICTIONS = Counter(
    "maintai_ml_predictions_total",
    "Successful predictions by label",
    ["prediction"],
    registry=METRICS,
)

ML_PROBABILITY = Histogram(
    "maintai_ml_failure_probability",
    "Failure probabilities of successful predictions",
    buckets=(0, 0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, 0.95, 0.99, 1),
    registry=METRICS,
)

ML_MODEL = Gauge(
    "maintai_ml_model_info",
    "Currently loaded model",
    ["model_version"],
    registry=METRICS,
)

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
    # Mỗi lần khởi động phải kiểm tra và load lại từ đầu.
    STATE["pipeline"] = None
    STATE["metadata"] = None

    ML_MODEL.clear()

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

        # Kiểm tra toàn bộ preprocessing + model có chạy được.
        # Đây là probe khởi động, không dùng để đánh giá accuracy.
        probe = pd.DataFrame(
            [{
                "product_type": "M",
                "air_temperature_k": 298.1,
                "process_temperature_k": 308.6,
                "rotational_speed_rpm": 1551,
                "torque_nm": 42.8,
                "tool_wear_min": 108,
            }],
            columns=FEATURES,
        )

        probabilities = pipeline.predict_proba(probe)

        if getattr(probabilities, "shape", None) != (1, 2):
            raise ValueError("Unexpected predict_proba output shape")

        values = [float(value) for value in probabilities[0]]

        if any(
            not math.isfinite(value) or not 0 <= value <= 1
            for value in values
        ):
            raise ValueError("Startup probe returned invalid probability")

        if not math.isclose(
            sum(values), 1.0, rel_tol=0.0, abs_tol=1e-6
        ):
            raise ValueError("Startup probabilities do not sum to one")

        # Chỉ công bố ready sau khi tất cả kiểm tra thành công.
        STATE["pipeline"] = pipeline
        STATE["metadata"] = metadata

        ML_MODEL.labels(
            model_version=metadata["model_version"]
        ).set(1)

        LOGGER.info("Local baseline loaded: %s", metadata["model_version"])

    except Exception:
        # Giữ model unavailable; không fallback sang artifact khác.
        LOGGER.exception("Baseline could not be loaded")

    yield

    STATE["pipeline"] = None
    STATE["metadata"] = None
    ML_MODEL.clear()


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

    is_prediction = (
        request.method == "POST"
        and request.url.path == "/api/v1/predict"
    )

    try:
        response = await call_next(request)
    except Exception:
        if is_prediction:
            ML_REQUESTS.labels(status="ERROR").inc()
        raise

    # Đếm đúng một lần khi request dự đoán hoàn tất.
    if is_prediction:
        status = {
            200: "SUCCESS",
            400: "INVALID_INPUT",
            422: "INVALID_INPUT",
            503: "NOT_READY",
        }.get(response.status_code, "ERROR")

        ML_REQUESTS.labels(status=status).inc()

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

@app.get("/health/live")
def health_live():
    """Process đang sống và có thể phản hồi HTTP."""
    return {
        "status": "UP",
        "service": "ml-inference",
        "version": "0.1.0",
    }


@app.get("/health/ready")
def health_ready():
    """Chỉ nhận dự đoán khi model đã qua kiểm tra khởi động."""
    model_loaded = (
        STATE["pipeline"] is not None
        and STATE["metadata"] is not None
    )

    return JSONResponse(
        status_code=200 if model_loaded else 503,
        content={
            "status": "UP" if model_loaded else "DOWN",
            "checks": {
                "model_loaded": model_loaded,
            },
        },
    )

@app.get("/metrics")
def metrics():
    """Xuất metrics để công cụ monitoring thu thập."""
    return Response(
        content=generate_latest(METRICS),
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )

@app.post("/api/v1/predict")
def predict(body: PredictionRequest, request: Request):
    pipeline = STATE["pipeline"]
    metadata = STATE["metadata"]

    if pipeline is None or metadata is None:
        return error_response(
            request, 503, "MODEL_NOT_READY",
            "Model artifact is not ready", True,
        )

    start = perf_counter()

    try:
        frame = pd.DataFrame(
            [body.features.model_dump()],
            columns=FEATURES,
        )
        probability = float(pipeline.predict_proba(frame)[0, 1])

        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError("Model returned invalid probability")

        threshold = metadata["threshold"]
        label = "FAILURE" if probability >= threshold else "NORMAL"

        result = {
            "prediction_id": f"pred_{uuid4().hex}",
            "event_id": body.event_id,
            "prediction": label,
            "failure_probability": probability,
            "model_version": metadata["model_version"],
            "preprocessor_version": metadata["preprocessor_version"],
            "model_threshold": threshold,
            "inference_ms": (perf_counter() - start) * 1000,
            "predicted_at": datetime.now(timezone.utc).isoformat(),
        }

        # Chỉ ghi nhận prediction khi đã tạo kết quả thành công.
        ML_PREDICTIONS.labels(prediction=label).inc()
        ML_PROBABILITY.observe(probability)

        LOGGER.info(
            json.dumps({
                "event": "prediction_completed",
                "request_id": request.state.request_id,
                "correlation_id": request.state.correlation_id,
                "model_version": metadata["model_version"],
                "prediction": label,
                "inference_ms": result["inference_ms"],
            })
        )

        return result

    except Exception:
        LOGGER.exception(
            "Inference failed; request_id=%s",
            request.state.request_id,
        )
        return error_response(
            request, 500, "INTERNAL_ERROR",
            "Inference failed", False,
        )

    finally:
        # Đo cả inference thành công và inference ném lỗi.
        ML_DURATION.observe(perf_counter() - start)