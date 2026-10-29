"""Xuất drift reference từ frozen train split cho baseline đã train."""

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[3]

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}
FEATURES = list(COLUMN_MAP.values())


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def histogram(values, bins):
    """Cùng quy tắc với Drift Worker: gom under/overflow vào bin biên."""
    indexes = np.searchsorted(bins, values, side="right") - 1
    indexes = np.clip(indexes, 0, len(bins) - 2)
    counts = np.bincount(indexes, minlength=len(bins) - 1)
    return (counts / len(values)).tolist()


def check_profile(profile):
    probabilities = np.asarray(profile["probabilities"], dtype=float)
    expected_length = (
        len(profile["categories"])
        if profile["type"] == "categorical"
        else len(profile["bins"]) - 1
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--dataset-version", required=True)
    args = parser.parse_args()

    if not args.dataset_version.strip():
        raise ValueError("dataset-version must not be empty")

    model_dir = args.model_dir.resolve()
    metadata_path = model_dir / "metadata.json"
    artifact_path = model_dir / "pipeline.joblib"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    data_path = ROOT / "sensor/data/ai4i2020.csv"

    # Xác minh frozen split bằng verifier đã kiểm thử.
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "ml-service/training/data_preparation/create_split.py"),
            "--verify-only",
        ],
        cwd=ROOT,
        check=True,
    )

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    split = json.loads(split_path.read_text(encoding="utf-8"))

    for path, expected in [
        (artifact_path, metadata["pipeline_sha256"]),
        (split_path, metadata["split_sha256"]),
        (data_path, metadata["source_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Checksum mismatch: {path.name}")

    if metadata["features"] != FEATURES:
        raise ValueError("Metadata feature list mismatch")
    if metadata["sklearn_version"] != sklearn.__version__:
        raise ValueError("Scikit-learn version mismatch")

    threshold = metadata["threshold"]
    if (
        type(threshold) not in (int, float)
        or not np.isfinite(threshold)
        or not 0 <= threshold <= 1
    ):
        raise ValueError("Invalid model threshold")

    # Chỉ chọn train; không tính profile trên validation/test.
    raw = pd.read_csv(data_path)
    train = (
        raw.iloc[split["indices"]["train"]][list(COLUMN_MAP)]
        .rename(columns=COLUMN_MAP)
        .copy()
    )

    if len(train) != metadata["train_rows"] or len(train) == 0:
        raise ValueError("Train sample count mismatch")
    if train.isna().any().any():
        raise ValueError("Train contains missing values")

    categories = ["L", "M", "H"]
    if not train["product_type"].isin(categories).all():
        raise ValueError("Unknown product type")

    profiles = {
        "product_type": {
            "type": "categorical",
            "metric": "PSI",
            "categories": categories,
            "probabilities": [
                float((train["product_type"] == category).mean())
                for category in categories
            ],
        },
    }

    for name in FEATURES[1:]:
        values = train[name].to_numpy(dtype=float)
        if not np.isfinite(values).all():
            raise ValueError(f"Non-finite feature: {name}")

        bins = np.unique(np.quantile(values, np.linspace(0, 1, 11)))

        # Giữ cùng cách xử lý constant/near-discrete của worker.
        if len(bins) < 3:
            lo, hi = float(values.min()), float(values.max())
            padding = max(1.0, abs(lo) * 0.01)
            bins = np.linspace(lo - padding, hi + padding, 11)

        profiles[name] = {
            "type": "numeric",
            "metric": "PSI",
            "bins": bins.tolist(),
            "probabilities": histogram(values, bins),
        }

    for profile in profiles.values():
        check_profile(profile)

    # Chỉ load artifact tin cậy của mình sau khi kiểm tra checksum.
    pipeline = joblib.load(artifact_path)
    if list(pipeline.feature_names_in_) != FEATURES:
        raise ValueError("Loaded pipeline feature mismatch")
    if list(pipeline.classes_) != [0, 1]:
        raise ValueError("Unexpected model classes")

    probabilities = np.asarray(pipeline.predict_proba(train), dtype=float)
    if (
        probabilities.shape != (len(train), 2)
        or not np.isfinite(probabilities).all()
        or np.any(probabilities < 0)
        or np.any(probabilities > 1)
        or not np.allclose(
            probabilities.sum(axis=1), 1.0, rtol=0, atol=1e-6
        )
    ):
        raise ValueError("Invalid model probabilities")

    # Worker so sánh tỉ lệ prediction label FAILURE ở runtime.
    failure_rate = float((probabilities[:, 1] >= threshold).mean())

    reference = {
        "schema_version": "1.0.0",
        "dataset_version": args.dataset_version,
        "model_version": metadata["model_version"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "sample_count": len(train),
        "features": profiles,
        "prediction_reference": {"failure_rate": failure_rate},
        "reference_source": "training",
    }

    schema_path = ROOT / "contracts/v1.3/schemas/drift-reference.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(
        schema, format_checker=FormatChecker()
    )
    validator.validate(reference)

    output = model_dir / "drift_reference.json"

    if output.exists():
        existing = json.loads(output.read_text(encoding="utf-8"))
        validator.validate(existing)

        # Giữ thời điểm tạo ban đầu để chạy lại không thay file/hash.
        reference["created_at"] = existing["created_at"]
        if existing != reference:
            raise ValueError(
                "Existing reference differs; inspect before replacing"
            )
        result = "UNCHANGED"
    else:
        with output.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(
                json.dumps(
                    reference, indent=2, ensure_ascii=False, allow_nan=False
                ) + "\n"
            )
        result = "CREATED"

    print("Result:", result)
    print("Output:", output)
    print("Model version:", reference["model_version"])
    print("Dataset version:", reference["dataset_version"])
    print("Reference source:", reference["reference_source"])
    print("Sample count:", reference["sample_count"])
    print("Predicted failure rate:", failure_rate)
    print("Schema and profile checks: PASS")
    print("Reference SHA-256:", sha256(output))


if __name__ == "__main__":
    main()