"""Đánh giá HGB đã khóa trên test đã sử dụng; không fit/tune."""

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
SHARED = ROOT / "ml-service/shared/preprocessing"

EXPECTED_DECISION_HASH = (
    "d8c699bc75e6af5a12b0080855f2d610"
    "95284c4ea317058ad05434b8d38abc08"
)

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
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def wilson(successes, total):
    if total == 0:
        return None
    z = 1.959963984540054
    p = successes / total
    denominator = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    half = z * math.sqrt(
        p * (1 - p) / total + z * z / (4 * total * total)
    ) / denominator
    return {
        "low": max(0.0, center - half),
        "high": min(1.0, center + half),
        "successes": successes,
        "total": total,
        "method": "Wilson 95%",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    decision_path = directory / "integration_candidate.json"
    if sha256(decision_path) != EXPECTED_DECISION_HASH:
        raise ValueError("Frozen decision changed.")
    decision = read_json(decision_path)

    if decision["candidate"] != "hgb_interactions":
        raise ValueError("Unexpected candidate.")
    if decision["test_previously_opened_for_v1"] is not True:
        raise ValueError("Prior test exposure must be recorded.")

    metadata_path = directory / "benchmark_metadata.json"
    if sha256(metadata_path) != decision["benchmark_metadata_sha256"]:
        raise ValueError("Benchmark metadata changed.")
    metadata = read_json(metadata_path)

    if sklearn.__version__ != metadata["sklearn_version"]:
        raise ValueError("Scikit-learn version mismatch.")

    artifact = (
        directory / decision["pipeline_relative_path"]
    ).resolve()
    if not artifact.is_relative_to(directory):
        raise ValueError("Artifact path escapes benchmark directory.")

    source = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    transformer = SHARED / "feature_interactions.py"

    for path, expected in [
        (artifact, decision["pipeline_sha256"]),
        (transformer, decision["transformer_sha256"]),
        (source, decision["source_sha256"]),
        (split_path, decision["split_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Checksum mismatch: {path}")

    threshold = decision["threshold"]
    if (
        type(threshold) not in (int, float)
        or not math.isfinite(threshold)
        or not 0 <= threshold <= 1
    ):
        raise ValueError("Invalid frozen threshold.")

    report_path = directory / "reused_test_report.json"
    intent_path = directory / "reused_test_intent.json"

    if report_path.exists() or intent_path.exists():
        raise FileExistsError(
            "Evaluation already exists or was attempted. "
            "Inspect before retrying; do not overwrite."
        )

    sys.path.insert(0, str(SHARED))
    pipeline = joblib.load(artifact)

    if list(pipeline.classes_) != [0, 1]:
        raise ValueError("Unexpected class order.")
    if list(pipeline.feature_names_in_) != list(COLUMN_MAP.values()):
        raise ValueError("Feature identity mismatch.")

    write_json(intent_path, {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "scope": "previously_used_test",
        "independent_new_holdout": False,
        "decision_sha256": sha256(decision_path),
        "pipeline_sha256": sha256(artifact),
        "threshold": threshold,
        "script_sha256": sha256(Path(__file__)),
    })

    split = read_json(split_path)
    test = pd.read_csv(source).iloc[split["indices"]["test"]]
    x = test[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    y = test["Machine failure"].to_numpy(dtype=int)

    if set(np.unique(y)) != {0, 1}:
        raise ValueError("Expected both classes.")

    probabilities = np.asarray(pipeline.predict_proba(x), dtype=float)
    if (
        probabilities.shape != (len(y), 2)
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
        for value in confusion_matrix(y, predicted, labels=[0, 1]).ravel()
    ]
    precision = float(precision_score(y, predicted, zero_division=0))
    recall = float(recall_score(y, predicted, zero_division=0))
    passed = precision >= 0.50 and recall >= 0.85

    report = {
        "scope": "previously_used_test",
        "independent_new_holdout": False,
        "candidate": decision["candidate"],
        "model_version": decision["model_version"],
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "samples": len(y),
        "failures": int(y.sum()),
        "threshold": threshold,
        "refitted": False,
        "threshold_tuned_on_test": False,
        "metrics": {
            "precision_failure": precision,
            "recall_failure": recall,
            "f1_failure": float(
                fbeta_score(y, predicted, beta=1, zero_division=0)
            ),
            "f2_failure": float(
                fbeta_score(y, predicted, beta=2, zero_division=0)
            ),
            "average_precision": float(average_precision_score(y, scores)),
            "false_negative_rate": fn / (tp + fn),
            "false_positives_per_1000_records": 1000 * fp / len(y),
            "tn": tn, "fp": fp, "fn": fn, "tp": tp,
        },
        "uncertainty": {
            "recall_wilson95": wilson(tp, tp + fn),
            "precision_wilson95": wilson(tp, tp + fp),
            "limitation": (
                "Assumes independent observations; does not correct "
                "adaptive development after prior test exposure."
            ),
        },
        "quality_gate": {
            "recall_failure_min": 0.85,
            "precision_failure_min": 0.50,
            "basis": "point_estimates_on_previously_used_test",
            "passed": bool(passed),
        },
        "approved_for_release": False,
        "provenance": {
            "decision_sha256": sha256(decision_path),
            "pipeline_sha256": sha256(artifact),
            "transformer_sha256": sha256(transformer),
            "source_sha256": sha256(source),
            "split_sha256": sha256(split_path),
            "script_sha256": sha256(Path(__file__)),
            "sklearn_version": sklearn.__version__,
        },
    }
    write_json(report_path, report)

    print("\nHGB — PREVIOUSLY USED TEST")
    print("Threshold:", repr(threshold))
    print(json.dumps(report["metrics"], indent=2))
    print(json.dumps(report["uncertainty"], indent=2))
    print("Point-estimate gate:", "PASS" if passed else "FAIL")
    print("Independent new holdout: NO")
    print("Approved for release: NO")
    print("Report:", report_path)
    print("Report SHA-256:", sha256(report_path))


if __name__ == "__main__":
    main()