"""API baseline chỉ dành cho thử nghiệm local trên loopback."""

import hashlib
import json
import logging
import math
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
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

try:
    import onnxruntime as ort
except ImportError:  # pragma: no cover - optional runtime optimization
    ort = None

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

STATE = {"pipeline": None, "metadata": None, "fast_predict": None, "fast_pipeline": None}

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


class BatchPredictRequest(StrictBody):
    items: list[PredictionRequest] = Field(min_length=1, max_length=64)


def _features_to_numpy_matrix(items_features: list[dict], *, include_interactions: bool) -> np.ndarray:
    n = len(items_features)
    pt_l = np.empty(n, dtype=np.float64)
    pt_m = np.empty(n, dtype=np.float64)
    pt_h = np.empty(n, dtype=np.float64)
    air = np.empty(n, dtype=np.float64)
    proc = np.empty(n, dtype=np.float64)
    rpm = np.empty(n, dtype=np.float64)
    torque = np.empty(n, dtype=np.float64)
    wear = np.empty(n, dtype=np.float64)
    for i, feat in enumerate(items_features):
        pt = feat["product_type"]
        pt_l[i] = 1.0 if pt == "L" else 0.0
        pt_m[i] = 1.0 if pt == "M" else 0.0
        pt_h[i] = 1.0 if pt == "H" else 0.0
        air[i] = float(feat["air_temperature_k"])
        proc[i] = float(feat["process_temperature_k"])
        rpm[i] = float(feat["rotational_speed_rpm"])
        torque[i] = float(feat["torque_nm"])
        wear[i] = float(feat["tool_wear_min"])
    if include_interactions:
        temp_diff = proc - air
        power = torque * rpm * (2.0 * np.pi / 60.0)
        wear_torque = wear * torque
        return np.column_stack(
            [pt_l, pt_m, pt_h, air, proc, rpm, torque, wear, temp_diff, power, wear_torque]
        )
    return np.column_stack([pt_l, pt_m, pt_h, air, proc, rpm, torque, wear])


def _extract_transformer_stats(preprocessor):
    """Extract linear scaling/one-hot params from a fitted ColumnTransformer."""
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    if not isinstance(preprocessor, ColumnTransformer):
        return None
    cat_cols = []
    num_cols = []
    num_means = []
    num_scales = []
    pt_categories = None
    for _name, trans, cols in preprocessor.transformers_:
        if trans == "drop":
            continue
        leaf = trans.steps[-1][1] if isinstance(trans, Pipeline) else trans
        if isinstance(leaf, OneHotEncoder):
            if list(cols) != ["product_type"]:
                return None
            cats = list(leaf.categories_[0])
            if set(cats) != {"L", "M", "H"} or leaf.drop is not None:
                return None
            pt_categories = cats
            cat_cols.extend(cols)
        elif trans == "passthrough":
            for col in cols:
                num_cols.append(col)
                num_means.append(0.0)
                num_scales.append(1.0)
        elif isinstance(leaf, StandardScaler):
            means = leaf.mean_ if leaf.mean_ is not None else np.zeros(len(cols))
            scales = leaf.scale_ if leaf.scale_ is not None else np.ones(len(cols))
            for col, mean, scale in zip(cols, means, scales, strict=True):
                num_cols.append(col)
                num_means.append(float(mean))
                num_scales.append(float(scale))
        else:
            return None
    if not pt_categories and not num_cols:
        return None
    return pt_categories or [], num_cols, np.asarray(num_means, dtype=np.float64), np.asarray(num_scales, dtype=np.float64)


def _build_fast_predictor(pipeline, model_dir: Path):
    """Build a zero-pandas vectorized scorer after strict < 1e-12 startup parity verification."""
    probe_rows = [
        {
            "product_type": "L",
            "air_temperature_k": 297.2,
            "process_temperature_k": 307.9,
            "rotational_speed_rpm": 1410.0,
            "torque_nm": 51.4,
            "tool_wear_min": 180.0,
        },
        {
            "product_type": "M",
            "air_temperature_k": 298.1,
            "process_temperature_k": 308.6,
            "rotational_speed_rpm": 1551.0,
            "torque_nm": 42.8,
            "tool_wear_min": 108.0,
        },
        {
            "product_type": "H",
            "air_temperature_k": 301.4,
            "process_temperature_k": 311.2,
            "rotational_speed_rpm": 1920.0,
            "torque_nm": 28.5,
            "tool_wear_min": 12.0,
        },
    ]
    probe_df = pd.DataFrame(probe_rows, columns=FEATURES)
    ref_probs = np.asarray(pipeline.predict_proba(probe_df)[:, 1], dtype=np.float64)

    onnx_path = model_dir / "model.onnx"
    if ort is not None and onnx_path.is_file():
        try:
            opts = ort.SessionOptions()
            opts.intra_op_num_threads = 1
            opts.inter_op_num_threads = 1
            session = ort.InferenceSession(
                str(onnx_path), sess_options=opts, providers=["CPUExecutionProvider"]
            )
            input_names = [inp.name for inp in session.get_inputs()]
            if input_names == FEATURES:

                def _onnx_predict(items_features: list[dict]) -> np.ndarray:
                    feeds = {
                        "product_type": np.array([[f["product_type"]] for f in items_features]),
                        **{
                            col: np.array([[float(f[col])] for f in items_features], dtype=np.float64)
                            for col in FEATURES[1:]
                        },
                    }
                    outputs = session.run(None, feeds)
                    probs = outputs[1] if len(outputs) > 1 else outputs[0]
                    if isinstance(probs, list):
                        return np.array([float(p[1]) for p in probs], dtype=np.float64)
                    return np.asarray(probs, dtype=np.float64)[:, 1]

                onnx_probs = _onnx_predict(probe_rows)
                if np.max(np.abs(onnx_probs - ref_probs)) < 1e-12:
                    return _onnx_predict
        except Exception:
            LOGGER.warning("Optional ONNX fast path failed verification; checking NumPy path")

    if not hasattr(pipeline, "steps") or len(pipeline.steps) < 2:
        return None
    classifier = pipeline.steps[-1][1]
    if not hasattr(classifier, "predict_proba"):
        return None
    pre_steps = [step for _, step in pipeline.steps[:-1]]
    interactions_enabled = False
    preprocessor = None
    for step in pre_steps:
        if type(step).__name__ == "SensorInteractions":
            interactions_enabled = bool(getattr(step, "enabled", False))
        elif preprocessor is None:
            preprocessor = step
        else:
            return None
    if preprocessor is None:
        return None
    extracted = _extract_transformer_stats(preprocessor)
    if extracted is None:
        return None
    pt_categories, num_cols, means, scales = extracted
    base_col_map = {
        "pt_L": 0,
        "pt_M": 1,
        "pt_H": 2,
        "air_temperature_k": 3,
        "process_temperature_k": 4,
        "rotational_speed_rpm": 5,
        "torque_nm": 6,
        "tool_wear_min": 7,
        "temperature_difference_k": 8,
        "mechanical_power_w": 9,
        "wear_torque_interaction": 10,
    }
    if any(col not in base_col_map for col in num_cols):
        return None
    cat_indices = [base_col_map[f"pt_{c}"] for c in pt_categories]
    num_indices = [base_col_map[col] for col in num_cols]

    def _numpy_predict(items_features: list[dict]) -> np.ndarray:
        raw = _features_to_numpy_matrix(items_features, include_interactions=interactions_enabled)
        parts = []
        for _name, trans, cols in preprocessor.transformers_:
            if trans == "drop":
                continue
            if list(cols) == ["product_type"]:
                parts.append(raw[:, cat_indices])
            else:
                scaled = (raw[:, num_indices] - means) / scales
                parts.append(scaled)
        X = np.hstack(parts) if len(parts) > 1 else parts[0]
        return np.asarray(classifier.predict_proba(X)[:, 1], dtype=np.float64)

    try:
        fast_probs = _numpy_predict(probe_rows)
        diff = float(np.max(np.abs(fast_probs - ref_probs)))
        if diff < 1e-12:
            LOGGER.info("Enabled zero-pandas vectorized ML fast path (max_abs_diff=%.2e)", diff)
            return _numpy_predict
        LOGGER.warning("Vectorized fast path parity diff %.2e >= 1e-12; falling back to pipeline", diff)
    except Exception:
        LOGGER.warning("Vectorized fast path parity check errored; falling back to pipeline")
    return None


def _score_batch_probabilities(items_features: list[dict], pipeline) -> np.ndarray:
    fast = STATE.get("fast_predict")
    if fast is not None and STATE.get("fast_pipeline") is pipeline:
        return fast(items_features)
    frame = pd.DataFrame.from_records(items_features, columns=FEATURES)
    probs = np.asarray(pipeline.predict_proba(frame)[:, 1], dtype=np.float64)
    return probs


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
        STATE["pipeline"] = pipeline
        STATE["metadata"] = metadata
        STATE["fast_predict"] = _build_fast_predictor(pipeline, model_dir)
        STATE["fast_pipeline"] = pipeline

        ML_MODEL.labels(
            model_version=metadata["model_version"]
        ).set(1)
        ML_ROLE.labels(role=SERVICE_ROLE).set(1)

        LOGGER.info("Local baseline loaded: %s (role: %s)", metadata["model_version"], SERVICE_ROLE)

    except Exception:
        # Giữ model unavailable; không fallback sang artifact khác.
        LOGGER.exception("Baseline could not be loaded")

    yield

    STATE["pipeline"] = None
    STATE["metadata"] = None
    STATE["fast_predict"] = None
    STATE["fast_pipeline"] = None
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
        and request.url.path in {"/api/v1/predict", "/api/v1/predict/batch"}
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

def _build_prediction_item(event_id: str, probability: float, metadata: dict, elapsed_ms: float) -> dict:
    if not math.isfinite(probability) or not 0 <= probability <= 1:
        raise ValueError("Model returned invalid probability")
    threshold = metadata["threshold"]
    label = "FAILURE" if probability >= threshold else "NORMAL"
    ML_PREDICTIONS.labels(prediction=label).inc()
    ML_PROBABILITY.observe(probability)
    return {
        "prediction_id": f"pred_{uuid4().hex}",
        "event_id": event_id,
        "prediction": label,
        "failure_probability": probability,
        "model_version": metadata["model_version"],
        "preprocessor_version": metadata["preprocessor_version"],
        "model_threshold": threshold,
        "inference_ms": elapsed_ms,
        "predicted_at": datetime.now(UTC).isoformat(),
    }


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
        probs = _score_batch_probabilities([body.features.model_dump()], pipeline)
        if getattr(probs, "shape", None) != (1,):
            raise ValueError("Unexpected probability output shape")
        probability = float(probs[0])
        elapsed_ms = (perf_counter() - start) * 1000
        result = _build_prediction_item(body.event_id, probability, metadata, elapsed_ms)

        LOGGER.info(
            json.dumps({
                "event": "prediction_completed",
                "request_id": request.state.request_id,
                "correlation_id": request.state.correlation_id,
                "model_version": metadata["model_version"],
                "prediction": result["prediction"],
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


@app.post("/api/v1/predict/batch")
def predict_batch(body: BatchPredictRequest, request: Request):
    pipeline = STATE["pipeline"]
    metadata = STATE["metadata"]

    if pipeline is None or metadata is None:
        return error_response(
            request, 503, "MODEL_NOT_READY",
            "Model artifact is not ready", True,
        )

    start = perf_counter()

    try:
        features_list = [item.features.model_dump() for item in body.items]
        probs = _score_batch_probabilities(features_list, pipeline)
        if getattr(probs, "shape", None) != (len(body.items),):
            raise ValueError("Unexpected batch probability output shape")
        elapsed_ms = (perf_counter() - start) * 1000
        per_item_ms = elapsed_ms / len(body.items)
        results = [
            _build_prediction_item(item.event_id, float(prob), metadata, per_item_ms)
            for item, prob in zip(body.items, probs, strict=True)
        ]

        LOGGER.info(
            json.dumps({
                "event": "batch_prediction_completed",
                "request_id": request.state.request_id,
                "correlation_id": request.state.correlation_id,
                "model_version": metadata["model_version"],
                "batch_size": len(results),
                "inference_ms": elapsed_ms,
            })
        )

        return {"results": results, "batch_size": len(results)}

    except Exception:
        LOGGER.exception(
            "Batch inference failed; request_id=%s",
            request.state.request_id,
        )
        return error_response(
            request, 500, "INTERNAL_ERROR",
            "Inference failed", False,
        )

    finally:
        ML_DURATION.observe(perf_counter() - start)