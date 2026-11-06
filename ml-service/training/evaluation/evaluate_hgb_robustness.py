"""Đo độ nhạy HGB trên validation; không train hoặc đổi threshold."""

import argparse
import hashlib
import importlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
SHARED = ROOT / "ml-service/shared/preprocessing"

DECISION_SHA = (
    "d8c699bc75e6af5a12b0080855f2d610952"
    "84c4ea317058ad05434b8d38abc08"
)
COLUMN_MAP = {
    "Type": "product_type",
    "Air temperature [K]": "air_temperature_k",
    "Process temperature [K]": "process_temperature_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
}
FEATURES = list(COLUMN_MAP.values())
NUMERIC = FEATURES[1:]
SEEDS = [11, 22, 33, 44, 55]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def predict_scores(pipeline, frame):
    probabilities = np.asarray(pipeline.predict_proba(frame), dtype=float)
    if (
        probabilities.shape != (len(frame), 2)
        or not np.isfinite(probabilities).all()
        or np.any(probabilities < 0)
        or np.any(probabilities > 1)
        or not np.allclose(
            probabilities.sum(axis=1), 1.0, rtol=0, atol=1e-10
        )
    ):
        raise ValueError("Invalid model probabilities")
    return probabilities[:, 1]


def evaluate_scenario(pipeline, original, baseline, threshold,
                      feature, perturbation):
    changed = original.copy(deep=True)
    changed[feature] = changed[feature].to_numpy(dtype=float) + perturbation

    finite = np.isfinite(
        changed[NUMERIC].to_numpy(dtype=float)
    ).all(axis=1)
    bad_rpm = changed["rotational_speed_rpm"].to_numpy() <= 0
    bad_wear = changed["tool_wear_min"].to_numpy() < 0
    valid = finite & ~bad_rpm & ~bad_wear

    result = {
        "total_rows": len(changed),
        "valid_rows": int(valid.sum()),
        "rejected_rows": int((~valid).sum()),
        "rejected_fraction": float((~valid).mean()),
        # Các lý do có thể chồng lấn; không cộng để ra rejected_rows.
        "nonfinite_rows": int((~finite).sum()),
        "nonpositive_rpm_rows": int(bad_rpm.sum()),
        "negative_wear_rows": int(bad_wear.sum()),
        "mean_absolute_score_change": None,
        "p95_absolute_score_change": None,
        "max_absolute_score_change": None,
        "label_flip_fraction": None,
        "normal_to_failure": None,
        "failure_to_normal": None,
        "baseline_failure_predictions_on_valid_rows": None,
        "perturbed_failure_predictions_on_valid_rows": None,
    }
    if not valid.any():
        return result

    current = predict_scores(pipeline, changed.loc[valid])
    reference = baseline[valid]
    old_labels = reference >= threshold
    new_labels = current >= threshold
    difference = np.abs(current - reference)

    result.update({
        "mean_absolute_score_change": float(difference.mean()),
        "p95_absolute_score_change": float(np.quantile(difference, 0.95)),
        "max_absolute_score_change": float(difference.max()),
        "label_flip_fraction": float((old_labels != new_labels).mean()),
        "normal_to_failure": int((~old_labels & new_labels).sum()),
        "failure_to_normal": int((old_labels & ~new_labels).sum()),
        "baseline_failure_predictions_on_valid_rows": int(old_labels.sum()),
        "perturbed_failure_predictions_on_valid_rows": int(new_labels.sum()),
    })
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    decision_path = directory / "integration_candidate.json"
    if sha256(decision_path) != DECISION_SHA:
        raise ValueError("Frozen decision changed")
    decision = read_json(decision_path)
    if decision["candidate"] != "hgb_interactions":
        raise ValueError("Unexpected candidate")

    source = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    metadata_path = directory / "benchmark_metadata.json"
    artifact = directory / decision["pipeline_relative_path"]
    transformer = SHARED / "feature_interactions.py"
    saved_path = directory / "hgb_interactions/validation_predictions.csv"
    protocol_path = (
        ROOT / "ml-service/training/evaluation/ROBUSTNESS_PROTOCOL.md"
    )

    for path, expected in [
        (source, decision["source_sha256"]),
        (split_path, decision["split_sha256"]),
        (metadata_path, decision["benchmark_metadata_sha256"]),
        (artifact, decision["pipeline_sha256"]),
        (transformer, decision["transformer_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Checksum mismatch: {path.name}")

    if not protocol_path.is_file():
        raise FileNotFoundError("Create ROBUSTNESS_PROTOCOL.md first")

    metadata = read_json(metadata_path)
    if version("scikit-learn") != metadata["sklearn_version"]:
        raise ValueError("Scikit-learn version differs from training")

    threshold = decision["threshold"]
    if (
        type(threshold) not in (int, float)
        or not np.isfinite(threshold)
        or not 0 <= threshold <= 1
    ):
        raise ValueError("Invalid threshold")

    # Xác minh module thực tế được import trước khi deserialize.
    sys.path.insert(0, str(SHARED))
    module = importlib.import_module("feature_interactions")
    if Path(module.__file__).resolve() != transformer.resolve():
        raise ValueError("Unexpected transformer import path")

    pipeline = joblib.load(artifact)
    if list(pipeline.classes_) != [0, 1]:
        raise ValueError("Class identity mismatch")
    if list(pipeline.feature_names_in_) != FEATURES:
        raise ValueError("Raw feature identity mismatch")

    split = read_json(split_path)
    raw = pd.read_csv(source)
    indices = split["indices"]["validation"]

    frame = (
        raw.iloc[indices][list(COLUMN_MAP)]
        .rename(columns=COLUMN_MAP)
        .reset_index(drop=True)
    )
    original_copy = frame.copy(deep=True)
    train = raw.iloc[split["indices"]["train"]][list(COLUMN_MAP)]
    train = train.rename(columns=COLUMN_MAP)

    saved = pd.read_csv(
        saved_path, dtype={"failure_score": "string"}
    )
    if saved["row_index"].tolist() != indices or len(saved) != 2000:
        raise ValueError("Saved validation membership/order mismatch")
    if not np.array_equal(
        saved["target"].to_numpy(),
        raw.iloc[indices]["Machine failure"].to_numpy(),
    ):
        raise ValueError("Saved validation targets mismatch")

    expected_scores = saved["failure_score"].map(float).to_numpy()
    baseline = predict_scores(pipeline, frame)
    np.testing.assert_allclose(
        baseline, expected_scores, rtol=1e-10, atol=1e-10
    )
    if not np.array_equal(
        baseline >= threshold, expected_scores >= threshold
    ):
        raise ValueError("Baseline labels differ at frozen threshold")

    # Control: không thay input thì score/nhãn không được thay đổi.
    control = evaluate_scenario(
        pipeline, frame, baseline, threshold,
        NUMERIC[0], np.zeros(len(frame)),
    )
    if (
        control["rejected_rows"] != 0
        or control["label_flip_fraction"] != 0
        or control["max_absolute_score_change"] > 1e-10
    ):
        raise ValueError("Zero-perturbation control failed")

    print("Identity / baseline scores / labels / zero-noise control: PASS",
          flush=True)

    rows = []
    iqrs = {}
    for feature in NUMERIC:
        q25, q75 = train[feature].quantile([0.25, 0.75])
        iqr = float(q75 - q25)
        if not np.isfinite(iqr) or iqr < 0:
            raise ValueError(f"Invalid train IQR: {feature}")
        iqrs[feature] = iqr

        for fraction in (0.01, 0.05):
            for seed in SEEDS:
                # Cùng seed dùng cùng vector nhiễu chuẩn để đối chiếu.
                perturbation = np.random.default_rng(seed).normal(
                    0.0, fraction * iqr, size=len(frame)
                )
                rows.append({
                    "feature": feature,
                    "kind": "gaussian_noise",
                    "iqr_fraction": fraction,
                    "seed": seed,
                    "train_iqr": iqr,
                    "zero_iqr": iqr == 0,
                    **evaluate_scenario(
                        pipeline, frame, baseline, threshold,
                        feature, perturbation,
                    ),
                })

        for direction in (-1, 1):
            rows.append({
                "feature": feature,
                "kind": "constant_bias",
                "iqr_fraction": direction * 0.05,
                "seed": None,
                "train_iqr": iqr,
                "zero_iqr": iqr == 0,
                **evaluate_scenario(
                    pipeline, frame, baseline, threshold,
                    feature, np.full(len(frame), direction * 0.05 * iqr),
                ),
            })

        print(f"Completed: {feature}", flush=True)

    pd.testing.assert_frame_equal(frame, original_copy, check_exact=True)
    if len(rows) != 60:
        raise ValueError("Expected 60 scenarios")

    results = pd.DataFrame(rows)
    noise = results[results["kind"] == "gaussian_noise"]
    summary = noise.groupby(
        ["feature", "iqr_fraction"], as_index=False
    ).agg(
        runs=("seed", "count"),
        rejected_rows_min=("rejected_rows", "min"),
        rejected_rows_max=("rejected_rows", "max"),
        mean_abs_change_mean=("mean_absolute_score_change", "mean"),
        flip_fraction_mean=("label_flip_fraction", "mean"),
        flip_fraction_min=("label_flip_fraction", "min"),
        flip_fraction_max=("label_flip_fraction", "max"),
    )

    output = Path(tempfile.mkdtemp(
        prefix="robustness-", dir=directory
    ))
    results.to_csv(output / "scenarios.csv", index=False)
    summary.to_csv(output / "noise_summary.csv", index=False)

    receipt = {
        "scope": "validation_input_sensitivity_not_accuracy",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model_version": decision["model_version"],
        "threshold": threshold,
        "seeds": SEEDS,
        "train_iqr": iqrs,
        "scenario_count": len(rows),
        "zero_perturbation_control": control,
        "baseline_score_and_label_check": "PASS",
        "original_input_unchanged": True,
        "input_sha256": {
            "decision": sha256(decision_path),
            "dataset": sha256(source),
            "split": sha256(split_path),
            "benchmark_metadata": sha256(metadata_path),
            "pipeline": sha256(artifact),
            "transformer": sha256(transformer),
            "saved_predictions": sha256(saved_path),
            "protocol": sha256(protocol_path),
        },
        "script_sha256": sha256(Path(__file__)),
        "dependencies": {
            name: version(name)
            for name in ("numpy", "pandas", "scikit-learn", "joblib")
        },
        "outputs_sha256": {
            path.name: sha256(path)
            for path in sorted(output.iterdir()) if path.is_file()
        },
        "model_or_threshold_changed": False,
        "test_evaluated": False,
        "accuracy_under_perturbation_evaluated": False,
        "robustness_acceptance_gate": None,
        "approved_for_release": False,
        "limitations": [
            "Perturbation levels are experimental, not sensor specifications.",
            "Changed labels are sensitivity measurements, not proven errors.",
            "Invalid rows are counted; comparisons use matching valid rows.",
            "Repeated seeds on the same records are not independent datasets.",
            "No production robustness certification is claimed.",
        ],
    }
    (output / "verification.json").write_text(
        json.dumps(receipt, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    print("\nGAUSSIAN NOISE SUMMARY")
    print(summary.round(6).to_string(index=False))
    print("\nCONSTANT BIAS")
    print(results.loc[
        results["kind"] == "constant_bias",
        ["feature", "iqr_fraction", "rejected_rows",
         "mean_absolute_score_change", "label_flip_fraction",
         "normal_to_failure", "failure_to_normal"],
    ].round(6).to_string(index=False))
    print("\nScenario execution: 60/60 completed")
    print("Output:", output)
    print("Robustness gate: NOT DEFINED")
    print("Model changed / threshold changed / test evaluated: NO")


if __name__ == "__main__":
    main()