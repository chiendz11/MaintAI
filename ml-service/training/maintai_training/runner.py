"""Approved-dataset batch training, evaluation, and immutable bundle export."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}
TARGET_COLUMN = "Machine failure"
FEATURES = list(COLUMN_MAP.values())
APP_ROOT = Path(__file__).resolve().parents[3]


def _ensure_shared_paths() -> Path:
    shared = APP_ROOT / "ml-service" / "shared" / "preprocessing"
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))
    data_prep = APP_ROOT / "ml-service" / "training" / "data_preparation"
    if str(data_prep) not in sys.path:
        sys.path.insert(0, str(data_prep))
    model_dir = APP_ROOT / "ml-service" / "training" / "model"
    if str(model_dir) not in sys.path:
        sys.path.insert(0, str(model_dir))
    return shared


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1048576):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_source_revision() -> str:
    env_rev = os.environ.get("MAINTAI_SOURCE_REVISION")
    if env_rev:
        if not re.fullmatch(r"[0-9a-f]{40}", env_rev):
            raise ValueError("Invalid MAINTAI_SOURCE_REVISION format")
        return env_rev
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=APP_ROOT, text=True).strip()
        if re.fullmatch(r"[0-9a-f]{40}", out):
            return out
    except (OSError, subprocess.CalledProcessError):
        pass
    return "0" * 40


def load_verified_rows(dataset_path: Path, expected_sha256: str) -> list[dict[str, str]]:
    if not dataset_path.is_file():
        raise FileNotFoundError(f"Verified dataset does not exist: {dataset_path}")
    actual_sha = sha256_file(dataset_path)
    if actual_sha != expected_sha256:
        raise ValueError(
            "Dataset checksum mismatch before training: "
            f"expected {expected_sha256}, got {actual_sha}"
        )
    with dataset_path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError("Dataset CSV has no header")
        required = set(COLUMN_MAP.keys()) | {TARGET_COLUMN}
        missing = sorted(required - set(reader.fieldnames))
        if missing:
            raise ValueError(f"Dataset CSV missing required columns: {missing}")
        rows = list(reader)
    if not rows:
        raise ValueError("Dataset CSV is empty")
    return rows


def extract_features_and_targets(rows: list[dict[str, str]]) -> tuple[object, object]:
    import numpy as np
    import pandas as pd

    records = []
    targets = []
    for idx, row in enumerate(rows):
        label_raw = row[TARGET_COLUMN]
        if label_raw not in {"0", "1"}:
            raise ValueError(f"Invalid target value at row {idx}")
        item = {}
        for csv_col, feature_name in COLUMN_MAP.items():
            val = row[csv_col]
            if feature_name == "product_type":
                if val not in {"L", "M", "H"}:
                    raise ValueError(f"Invalid product_type at row {idx}")
                item[feature_name] = val
            else:
                num = float(val)
                if not np.isfinite(num):
                    raise ValueError(f"Non-finite value for {feature_name} at row {idx}")
                item[feature_name] = num
        records.append(item)
        targets.append(int(label_raw))

    frame = pd.DataFrame(records, columns=FEATURES)
    if list(frame.columns) != FEATURES:
        raise ValueError("Allowlisted feature order violated")
    return frame, np.asarray(targets, dtype=int)


def _histogram(values, bins) -> list[float]:
    import numpy as np

    indexes = np.searchsorted(bins, values, side="right") - 1
    indexes = np.clip(indexes, 0, len(bins) - 2)
    counts = np.bincount(indexes, minlength=len(bins) - 1)
    return (counts / len(values)).tolist()


def _check_profile(profile: dict) -> None:
    import numpy as np

    probabilities = np.asarray(profile["probabilities"], dtype=float)
    expected_length = (
        len(profile["categories"]) if profile["type"] == "categorical" else len(profile["bins"]) - 1
    )
    if (
        len(probabilities) != expected_length
        or not np.isfinite(probabilities).all()
        or np.any(probabilities < 0)
        or np.any(probabilities > 1)
        or not np.isclose(probabilities.sum(), 1.0, rtol=0, atol=1e-12)
    ):
        raise ValueError("Invalid reference probabilities")
    if profile["type"] == "numeric":
        bins = np.asarray(profile["bins"], dtype=float)
        if not np.isfinite(bins).all() or not np.all(np.diff(bins) > 0):
            raise ValueError("Reference bins must be finite and increasing")


def run_training(context_path: Path, output_path: Path) -> dict:
    shared_dir = _ensure_shared_paths()

    import joblib
    import numpy as np
    import sklearn
    from benchmark_candidates import choose_operating_point, metrics, threshold_table
    from create_split import build_split, validate_split
    from feature_interactions import DERIVED_FEATURES, RAW_FEATURES, SensorInteractions
    from jsonschema import Draft202012Validator, FormatChecker
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.metrics import average_precision_score
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    context = json.loads(context_path.read_text(encoding="utf-8"))
    dataset_path = Path(context["dataset_path"]).resolve()
    dataset_sha256 = context["dataset_sha256"]
    dataset_version = str(context["dataset_version"])
    model_version = str(context["model_version"])
    seed = int(context.get("split_seed", 42))
    bundle_dir = Path(context["output_dir"]).resolve()
    bundle_dir.mkdir(parents=True, exist_ok=True)

    rows = load_verified_rows(dataset_path, dataset_sha256)
    splits = build_split(rows, seed=seed)
    validate_split(rows, splits)
    split_bytes = json.dumps(
        {"seed": seed, "indices": splits}, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    split_sha256 = sha256_bytes(split_bytes)

    features_df, labels = extract_features_and_targets(rows)
    train_idx = splits["train"]
    val_idx = splits["validation"]
    test_idx = splits["test"]

    x_train = features_df.iloc[train_idx].copy()
    y_train = labels[train_idx]
    x_val = features_df.iloc[val_idx].copy()
    y_val = labels[val_idx]
    x_test = features_df.iloc[test_idx].copy()
    y_test = labels[test_idx]

    numeric_cols = RAW_FEATURES[1:] + DERIVED_FEATURES
    preprocessing = ColumnTransformer(
        transformers=[
            (
                "category",
                OneHotEncoder(
                    categories=[["L", "M", "H"]],
                    handle_unknown="error",
                    sparse_output=False,
                ),
                ["product_type"],
            ),
            ("numeric", "passthrough", numeric_cols),
        ],
        remainder="drop",
    )
    model_params = {
        "max_iter": 200,
        "learning_rate": 0.1,
        "max_leaf_nodes": 15,
        "min_samples_leaf": 20,
        "l2_regularization": 1.0,
        "early_stopping": False,
        "random_state": seed,
    }
    pipeline = Pipeline(
        [
            ("interactions", SensorInteractions(enabled=True)),
            ("preprocessing", preprocessing),
            ("model", HistGradientBoostingClassifier(**model_params)),
        ]
    )
    pipeline.fit(x_train, y_train)
    if list(pipeline.feature_names_in_) != FEATURES or list(pipeline.classes_) != [0, 1]:
        raise ValueError("Fitted pipeline violates feature or class contract")

    val_probs = np.asarray(pipeline.predict_proba(x_val), dtype=float)[:, 1]
    val_table = threshold_table(y_val, val_probs)
    val_point = choose_operating_point(val_table)
    threshold = float(val_point["threshold"])
    val_ap = float(average_precision_score(y_val, val_probs))

    test_probs = np.asarray(pipeline.predict_proba(x_test), dtype=float)[:, 1]
    test_point = metrics(y_test, test_probs >= threshold)
    test_ap = float(average_precision_score(y_test, test_probs))

    pipeline_path = bundle_dir / "pipeline.joblib"
    joblib.dump(pipeline, pipeline_path)
    pipeline_sha256 = sha256_file(pipeline_path)

    transformer_file = shared_dir / "feature_interactions.py"
    transformer_sha256 = sha256_file(transformer_file)
    source_git_sha = resolve_source_revision()

    metadata = {
        "status": "local_candidate_bundle",
        "approved_for_release": False,
        "model_version": model_version,
        "preprocessor_version": f"sensor-interactions-{transformer_sha256[:12]}",
        "dataset_version": dataset_version,
        "algorithm": "HistGradientBoostingClassifier",
        "threshold": threshold,
        "threshold_status": "selected_on_validation_split",
        "features": list(RAW_FEATURES),
        "derived_features": list(DERIVED_FEATURES),
        "transformer_module": "feature_interactions",
        "transformer_sha256": transformer_sha256,
        "pipeline_sha256": pipeline_sha256,
        "source_sha256": dataset_sha256,
        "split_sha256": split_sha256,
        "train_rows": len(train_idx),
        "validation_rows": len(val_idx),
        "test_rows": len(test_idx),
        "sklearn_version": sklearn.__version__,
        "python_version": sys.version.split()[0],
        "source_git_commit": source_git_sha,
        "model_params": model_params,
        "validation_metrics": {
            "precision_failure": float(val_point["precision"]),
            "recall_failure": float(val_point["recall"]),
            "f1": float(val_point["f1"]),
            "f2": float(val_point["f2"]),
            "average_precision": val_ap,
            "tp": int(val_point["tp"]),
            "fp": int(val_point["fp"]),
            "fn": int(val_point["fn"]),
            "tn": int(val_point["tn"]),
        },
    }
    metadata_path = bundle_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    evaluation = {
        "schema_version": "1.0.0",
        "model_version": model_version,
        "dataset_version": dataset_version,
        "threshold": threshold,
        "validation": {
            **metadata["validation_metrics"],
            "gate_pass": bool(val_point["gate_pass"]),
        },
        "test": {
            "precision_failure": float(test_point["precision"]),
            "recall_failure": float(test_point["recall"]),
            "f1": float(test_point["f1"]),
            "f2": float(test_point["f2"]),
            "average_precision": test_ap,
            "tp": int(test_point["tp"]),
            "fp": int(test_point["fp"]),
            "fn": int(test_point["fn"]),
            "tn": int(test_point["tn"]),
            "gate_pass": bool(test_point["gate_pass"]),
        },
        "source_git_commit": source_git_sha,
    }
    evaluation_path = bundle_dir / "evaluation.json"
    evaluation_path.write_text(json.dumps(evaluation, indent=2) + "\n", encoding="utf-8")

    categories = ["L", "M", "H"]
    profiles = {
        "product_type": {
            "type": "categorical",
            "metric": "PSI",
            "categories": categories,
            "probabilities": [float((x_train["product_type"] == cat).mean()) for cat in categories],
        }
    }
    for name in FEATURES[1:]:
        values = x_train[name].to_numpy(dtype=float)
        bins = np.unique(np.quantile(values, np.linspace(0, 1, 11)))
        if len(bins) < 3:
            lo, hi = float(values.min()), float(values.max())
            padding = max(1.0, abs(lo) * 0.01)
            bins = np.linspace(lo - padding, hi + padding, 11)
        profiles[name] = {
            "type": "numeric",
            "metric": "PSI",
            "bins": bins.tolist(),
            "probabilities": _histogram(values, bins),
        }
    for profile in profiles.values():
        _check_profile(profile)

    train_probs = np.asarray(pipeline.predict_proba(x_train), dtype=float)[:, 1]
    drift_reference = {
        "schema_version": "1.0.0",
        "dataset_version": dataset_version,
        "model_version": model_version,
        "created_at": datetime.now(UTC).isoformat(),
        "sample_count": len(x_train),
        "features": profiles,
        "prediction_reference": {"failure_rate": float((train_probs >= threshold).mean())},
        "reference_source": "training",
    }
    schema_path = APP_ROOT / "contracts" / "v1.3" / "schemas" / "drift-reference.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(drift_reference)
    drift_path = bundle_dir / "drift_reference.json"
    drift_path.write_text(json.dumps(drift_reference, indent=2) + "\n", encoding="utf-8")

    files = {}
    for file_path in (pipeline_path, metadata_path, evaluation_path, drift_path):
        files[file_path.name] = sha256_file(file_path)

    model_result = {
        "schema_version": "1.0.0",
        "model_version": model_version,
        "preprocessor_version": metadata["preprocessor_version"],
        "dataset_version": dataset_version,
        "manifest_key": context.get("manifest_key"),
        "manifest_version_id": context.get("manifest_version_id"),
        "manifest_sha256": context.get("manifest_sha256"),
        "dataset_version_id": context.get("dataset_version_id"),
        "dataset_sha256": dataset_sha256,
        "source_git_sha": source_git_sha,
        "bundle_dir": str(bundle_dir),
        "files": files,
        "threshold": threshold,
        "validation_metrics": metadata["validation_metrics"],
        "test_metrics": evaluation["test"],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(model_result, indent=2) + "\n", encoding="utf-8")
    return model_result
