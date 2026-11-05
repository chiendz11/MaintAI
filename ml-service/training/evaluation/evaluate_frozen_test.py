"""Đánh giá test của candidate đã khóa; không fit hoặc tune."""

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    fbeta_score,
    precision_score,
    recall_score,
)


ROOT = Path(__file__).resolve().parents[3]

COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(
            value, stream, indent=2,
            ensure_ascii=False, allow_nan=False,
        )
        stream.write("\n")


def wilson_interval(successes, total):
    """Khoảng Wilson 95% cho một tỷ lệ nhị thức."""
    if total == 0:
        return None

    z = 1.959963984540054
    proportion = successes / total
    denominator = 1 + z * z / total
    center = (proportion + z * z / (2 * total)) / denominator
    half_width = (
        z
        * math.sqrt(
            proportion * (1 - proportion) / total
            + z * z / (4 * total * total)
        )
        / denominator
    )

    return {
        "low": max(0.0, center - half_width),
        "high": min(1.0, center + half_width),
        "successes": successes,
        "total": total,
        "method": "Wilson 95%",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    parser.add_argument("--selection-sha256", required=True)
    args = parser.parse_args()

    directory = args.benchmark_dir.resolve()
    selection_path = directory / "frozen_selection.json"
    selection_hash = sha256(selection_path)

    if selection_hash != args.selection_sha256.lower():
        raise ValueError("Frozen selection checksum mismatch.")

    selection = read_json(selection_path)
    if selection["status"] != "frozen_for_test_not_approved_release":
        raise ValueError("Unexpected selection status.")
    if selection["test_evaluated"] is not False:
        raise ValueError("Unexpected frozen selection scope.")
    if selection["refit_before_test"] is not False:
        raise ValueError("This evaluation requires the original fitted model.")

    benchmark_path = directory / "benchmark_metadata.json"
    if sha256(benchmark_path) != selection["benchmark_metadata_sha256"]:
        raise ValueError("Benchmark metadata changed.")

    benchmark = read_json(benchmark_path)
    if sklearn.__version__ != benchmark["sklearn_version"]:
        raise ValueError("Scikit-learn version differs from training.")

    source_path = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    protocol_path = (
        ROOT / "ml-service/training/evaluation/BENCHMARK_PROTOCOL.md"
    )

    for path, expected_hash in [
        (source_path, selection["source_sha256"]),
        (split_path, selection["split_sha256"]),
        (protocol_path, selection["protocol_sha256"]),
    ]:
        if sha256(path) != expected_hash:
            raise ValueError(f"Checksum mismatch: {path}")

    artifact = (directory / selection["artifact_relative_path"]).resolve()
    if not artifact.is_relative_to(directory):
        raise ValueError("Artifact path escapes benchmark directory.")
    if sha256(artifact) != selection["pipeline_sha256"]:
        raise ValueError("Model artifact changed.")

    threshold = selection["threshold"]
    if (
        type(threshold) not in (int, float)
        or not math.isfinite(threshold)
        or not 0 <= threshold <= 1
    ):
        raise ValueError("Invalid frozen threshold.")

    if selection["features"] != list(COLUMN_MAP.values()):
        raise ValueError("Feature allowlist mismatch.")

    report_path = directory / "heldout_test_report.json"
    intent_path = directory / "heldout_test_intent.json"

    if report_path.exists() or intent_path.exists():
        raise FileExistsError(
            "A test evaluation already exists or was attempted. "
            "Inspect it before taking further action; do not overwrite."
        )

    # Chỉ deserialize artifact tin cậy do nhóm tạo và đã đối chiếu hash.
    pipeline = joblib.load(artifact)
    if list(pipeline.classes_) != [0, 1]:
        raise ValueError("Unexpected model classes.")
    if list(pipeline.feature_names_in_) != selection["features"]:
        raise ValueError("Loaded model feature order mismatch.")

    # Ghi nhận lần mở test trước khi tính prediction/metrics.
    write_json(intent_path, {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "selection_sha256": selection_hash,
        "pipeline_sha256": selection["pipeline_sha256"],
        "threshold": threshold,
        "script_sha256": sha256(Path(__file__)),
        "scope": "heldout_test_fixed_model_fixed_threshold",
    })

    manifest = read_json(split_path)
    test_indices = manifest["indices"]["test"]
    data = pd.read_csv(source_path)

    test = data.iloc[test_indices]
    features = test[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    labels = test["Machine failure"].to_numpy(dtype=int)

    if set(np.unique(labels)) != {0, 1}:
        raise ValueError("Expected both classes in test.")

    probabilities = pipeline.predict_proba(features)
    if (
        probabilities.shape != (len(test), 2)
        or not np.all(np.isfinite(probabilities))
        or np.any(probabilities < 0)
        or np.any(probabilities > 1)
        or not np.allclose(
            probabilities.sum(axis=1), 1.0, rtol=1e-10, atol=1e-10
        )
    ):
        raise ValueError("Invalid model probabilities.")

    scores = probabilities[:, 1]
    predicted = (scores >= threshold).astype(int)

    tn, fp, fn, tp = [
        int(value)
        for value in confusion_matrix(
            labels, predicted, labels=[0, 1]
        ).ravel()
    ]

    precision = float(
        precision_score(labels, predicted, zero_division=0)
    )
    recall = float(recall_score(labels, predicted, zero_division=0))
    gate = selection["gate"]
    gate_pass = (
        recall >= gate["recall_failure_min"]
        and precision >= gate["precision_failure_min"]
    )

    report = {
        "scope": "heldout_test",
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "candidate": selection["candidate"],
        "threshold": threshold,
        "samples": len(test),
        "failures": int(labels.sum()),
        "metrics": {
            "precision_failure": precision,
            "recall_failure": recall,
            "f1_failure": float(
                fbeta_score(labels, predicted, beta=1, zero_division=0)
            ),
            "f2_failure": float(
                fbeta_score(labels, predicted, beta=2, zero_division=0)
            ),
            "average_precision": float(
                average_precision_score(labels, scores)
            ),
            "false_negative_rate": fn / (fn + tp),
            "false_positives_per_1000_records": 1000 * fp / len(test),
            "tn": tn,
            "fp": fp,
            "fn": fn,
            "tp": tp,
        },
        "uncertainty": {
            "recall_wilson95": wilson_interval(tp, tp + fn),
            "precision_wilson95": wilson_interval(tp, tp + fp),
            "limitation": (
                "Binomial intervals assume independent observations; "
                "they do not quantify domain shift or all sources "
                "of model-selection and dataset uncertainty."
            ),
        },
        "quality_gate": {
            **gate,
            "basis": "point_estimates",
            "passed": bool(gate_pass),
        },
        "approved_for_release": False,
        "refitted": False,
        "threshold_tuned_on_test": False,
        "provenance": {
            "selection_sha256": selection_hash,
            "pipeline_sha256": selection["pipeline_sha256"],
            "source_sha256": selection["source_sha256"],
            "split_sha256": selection["split_sha256"],
            "evaluation_script_sha256": sha256(Path(__file__)),
            "sklearn_version": sklearn.__version__,
            "numpy_version": np.__version__,
            "pandas_version": pd.__version__,
            "python_version": sys.version,
        },
    }

    write_json(report_path, report)

    print("\nHELD-OUT TEST — FROZEN RANDOM FOREST")
    print("Threshold:", repr(threshold))
    print(json.dumps(report["metrics"], indent=2))
    print("\nUncertainty:")
    print(json.dumps(report["uncertainty"], indent=2))
    print("\nTest quality gate:", "PASS" if gate_pass else "FAIL")
    print("Approved for release: NO")
    print("Report:", report_path)
    print("Report SHA-256:", sha256(report_path))


if __name__ == "__main__":
    main()