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
import numpy as np
import pandas as pd
import sklearn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field

import importlib.util

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

DERIVED_FEATURES = [
    "temperature_difference_k",
    "mechanical_power_w",
    "wear_torque_interaction",
]

STATE = {"pipeline": None, "metadata": None, "fast_path": None}
TWO_PI_OVER_60 = 2.0 * math.pi / 60.0
ALL_FEATURE_INDEX = {
    "air_temperature_k": 0,
    "process_temperature_k": 1,
    "rotational_speed_rpm": 2,
    "torque_nm": 3,
    "tool_wear_min": 4,
    "temperature_difference_k": 5,
    "mechanical_power_w": 6,
    "wear_torque_interaction": 7,
}


class _FastInferencePath:
    """Thread-safe zero-DataFrame NumPy preprocessing + predict_proba fast path."""

    def __init__(self, pipeline, *, interactions_enabled: bool, total_cols: int, segments: list, estimator):
        self.pipeline = pipeline
        self.interactions_enabled = interactions_enabled
        self.total_cols = total_cols
        self.segments = segments
        self.estimator = estimator

    def predict_proba_single(self, features: "Features") -> float:
        air = float(features.air_temperature_k)
        proc = float(features.process_temperature_k)
        rpm = float(features.rotational_speed_rpm)
        torque = float(features.torque_nm)
        wear = float(features.tool_wear_min)
        if self.interactions_enabled:
            td = proc - air
            mp = torque * rpm * TWO_PI_OVER_60
            wt = wear * torque
            if not (math.isfinite(td) and math.isfinite(mp) and math.isfinite(wt)):
                raise ValueError("Derived features overflowed or became non-finite.")
            vals = (air, proc, rpm, torque, wear, td, mp, wt)
        else:
            vals = (air, proc, rpm, torque, wear)

        x = np.zeros((1, self.total_cols), dtype=np.float64)
        for kind, offset, spec in self.segments:
            if kind == "ohe":
                cat_idx = spec.get(features.product_type)
                if cat_idx is None:
                    raise ValueError("Invalid product_type.")
                x[0, offset + cat_idx] = 1.0
            else:
                col_indices, mean, scale = spec
                for j, vidx in enumerate(col_indices):
                    val = vals[vidx]
                    if mean is not None:
                        val -= mean[j]
                    if scale is not None:
                        val /= scale[j]
                    x[0, offset + j] = val
        probs = self.estimator.predict_proba(x)
        return float(probs[0, 1])


def _build_verified_fast_path(pipeline) -> _FastInferencePath | None:
    steps = getattr(pipeline, "steps", None)
    if not steps or len(steps) < 2:
        return None
    step_idx = 0
    interactions_enabled = False
    first_name, first_obj = steps[0]
    if first_name == "interactions" or type(first_obj).__name__ == "SensorInteractions":
        enabled = getattr(first_obj, "enabled", None)
        if type(enabled) is not bool:
            return None
        interactions_enabled = enabled
        step_idx += 1
    if len(steps) - step_idx != 2:
        return None
    _, ct = steps[step_idx]
    _, estimator = steps[step_idx + 1]
    transformers = getattr(ct, "transformers_", None)
    if not transformers or not hasattr(estimator, "predict_proba"):
        return None
    allowed_num = set(FEATURES[1:]) | (set(DERIVED_FEATURES) if interactions_enabled else set())
    offset = 0
    segments = []
    for t_name, trans, cols in transformers:
        if trans == "drop" or cols is None or len(cols) == 0:
            continue
        cols_list = list(cols)
        if cols_list == ["product_type"] and hasattr(trans, "categories_") and getattr(trans, "drop", None) is None:
            cats = trans.categories_
            if len(cats) != 1:
                return None
            cat_map = {str(c): idx for idx, c in enumerate(cats[0])}
            segments.append(("ohe", offset, cat_map))
            offset += len(cats[0])
        elif all(c in allowed_num for c in cols_list):
            if trans == "passthrough" or (type(trans).__name__ == "FunctionTransformer" and getattr(trans, "func", None) is None and getattr(trans, "inverse_func", None) is None):
                mean = None
                scale = None
            elif hasattr(trans, "mean_") or hasattr(trans, "scale_"):
                raw_mean = getattr(trans, "mean_", None)
                raw_scale = getattr(trans, "scale_", None)
                mean = [float(v) for v in raw_mean] if raw_mean is not None else None
                scale = [float(v) for v in raw_scale] if raw_scale is not None else None
            else:
                return None
            col_indices = [ALL_FEATURE_INDEX[c] for c in cols_list]
            segments.append(("num", offset, (col_indices, mean, scale)))
            offset += len(cols_list)
        else:
            return None

    candidate = _FastInferencePath(
        pipeline,
        interactions_enabled=interactions_enabled,
        total_cols=offset,
        segments=segments,
        estimator=estimator,
    )
    probes = [
        {"product_type": "L", "air_temperature_k": 297.2, "process_temperature_k": 307.9, "rotational_speed_rpm": 1412.0, "torque_nm": 51.4, "tool_wear_min": 42.0},
        {"product_type": "M", "air_temperature_k": 298.1, "process_temperature_k": 308.6, "rotational_speed_rpm": 1551.0, "torque_nm": 42.8, "tool_wear_min": 108.0},
        {"product_type": "H", "air_temperature_k": 301.4, "process_temperature_k": 311.1, "rotational_speed_rpm": 1340.0, "torque_nm": 64.5, "tool_wear_min": 215.0},
    ]
    for p in probes:
        ref = float(pipeline.predict_proba(pd.DataFrame([p], columns=FEATURES))[0, 1])
        fast = candidate.predict_proba_single(Features(**p))
        if not math.isfinite(fast) or abs(ref - fast) > 1e-12:
            LOGGER.warning("Fast inference path parity check failed (diff=%s); falling back to DataFrame pipeline", abs(ref - fast))
            return None
    # Warm up estimator code paths before reporting ready
    probe_f = Features(**probes[1])
    for _ in range(10):
        candidate.predict_proba_single(probe_f)
    return candidate

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

SERVICE_ROLE = os.environ.get("SERVICE_ROLE", "champion")

ML_ROLE = Gauge(
    "maintai_ml_service_role",
    "Service deployment role (champion or shadow)",
    ["role"],
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
    STATE["fast_path"] = None

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

        # Pipeline có tương tác cần đúng source module trong runtime.
        if metadata.get("transformer_module") is not None:
            if metadata["transformer_module"] != "feature_interactions":
                raise ValueError("Unsupported transformer module")

            spec = importlib.util.find_spec("feature_interactions")
            if spec is None or spec.origin is None:
                raise ValueError("Transformer module is unavailable")

            transformer_path = Path(spec.origin)
            actual_transformer_hash = hashlib.sha256(
                transformer_path.read_bytes()
            ).hexdigest()

            if actual_transformer_hash != metadata["transformer_sha256"]:
                raise ValueError("Transformer source checksum mismatch")

        # Chỉ load artifact tin cậy sau khi kiểm tra model và module.
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
        fast_path = _build_verified_fast_path(pipeline)
        STATE["pipeline"] = pipeline
        STATE["metadata"] = metadata
        STATE["fast_path"] = fast_path

        ML_MODEL.labels(
            model_version=metadata["model_version"]
        ).set(1)
        ML_ROLE.labels(role=SERVICE_ROLE).set(1)

        LOGGER.info(
            "Local baseline loaded: %s (role: %s, fast_path: %s)",
            metadata["model_version"],
            SERVICE_ROLE,
            fast_path is not None,
        )

    except Exception:
        # Giữ model unavailable; không fallback sang artifact khác.
        LOGGER.exception("Baseline could not be loaded")

    yield

    STATE["pipeline"] = None
    STATE["metadata"] = None
    STATE["fast_path"] = None
    ML_MODEL.clear()
    ML_ROLE.clear()


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
    response.headers["X-Service-Role"] = SERVICE_ROLE
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
        fast_path = STATE.get("fast_path")
        if fast_path is not None and fast_path.pipeline is pipeline:
            probability = fast_path.predict_proba_single(body.features)
        else:
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