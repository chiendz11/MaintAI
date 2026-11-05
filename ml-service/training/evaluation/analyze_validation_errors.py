"""Phân tích lỗi validation của HGB đã khóa; không thay model/threshold."""

import argparse
import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
DECISION_SHA = (
    "d8c699bc75e6af5a12b0080855f2d610952"
    "84c4ea317058ad05434b8d38abc08"
)

FEATURES = [
    "Type",
    "Air temperature [K]",
    "Process temperature [K]",
    "Rotational speed [rpm]",
    "Torque [Nm]",
    "Tool wear [min]",
]
MODES = ["TWF", "HDF", "PWF", "OSF", "RNF"]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-dir", type=Path, required=True)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    decision_path = directory / "integration_candidate.json"
    if sha256(decision_path) != DECISION_SHA:
        raise ValueError("Frozen decision checksum mismatch")

    decision = read_json(decision_path)
    if decision["candidate"] != "hgb_interactions":
        raise ValueError("Unexpected candidate")

    data_path = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    experiment_path = directory / "hgb_interactions/experiment.json"
    prediction_path = directory / "hgb_interactions/validation_predictions.csv"
    pipeline_path = directory / decision["pipeline_relative_path"]

    for path, expected in [
        (data_path, decision["source_sha256"]),
        (split_path, decision["split_sha256"]),
        (experiment_path, decision["experiment_sha256"]),
        (pipeline_path, decision["pipeline_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Checksum mismatch: {path.name}")

    threshold = decision["threshold"]
    if (
        type(threshold) not in (int, float)
        or not np.isfinite(threshold)
        or not 0 <= threshold <= 1
    ):
        raise ValueError("Invalid frozen threshold")

    split = read_json(split_path)
    indices = split["indices"]["validation"]
    raw = pd.read_csv(data_path)

    # Chỉ lấy validation để phân tích lỗi; train dùng tính ranh giới slice.
    validation = raw.iloc[indices].copy()
    validation.insert(0, "row_index", indices)
    validation = validation.set_index("row_index")

    saved = pd.read_csv(
        prediction_path,
        dtype={"failure_score": "string"},
    )
    saved["failure_score"] = saved["failure_score"].map(float)

    if set(saved.columns) != {"row_index", "target", "failure_score"}:
        raise ValueError("Unexpected prediction columns")
    if saved["row_index"].duplicated().any():
        raise ValueError("Duplicate prediction rows")
    if len(saved) != 2000 or set(saved["row_index"]) != set(indices):
        raise ValueError("Validation membership mismatch")

    saved = saved.set_index("row_index").loc[indices]
    if not np.array_equal(
        saved["target"].to_numpy(),
        validation["Machine failure"].to_numpy(),
    ):
        raise ValueError("Saved targets differ from dataset")

    y = saved["target"].to_numpy()
    score = saved["failure_score"].to_numpy(dtype=float)

    if not np.isin(y, [0, 1]).all() or int(y.sum()) != 68:
        raise ValueError("Unexpected validation labels")
    if (
        not np.isfinite(score).all()
        or np.any(score < 0)
        or np.any(score > 1)
    ):
        raise ValueError("Invalid saved scores")
    if not validation[MODES].isin([0, 1]).all().all():
        raise ValueError("Invalid failure-mode flags")

    prediction = (score >= threshold).astype(int)
    outcome = np.select(
        [
            (y == 1) & (prediction == 0),
            (y == 0) & (prediction == 1),
            (y == 1) & (prediction == 1),
        ],
        ["FN", "FP", "TP"],
        default="TN",
    )

    validation["failure_score"] = score
    validation["prediction"] = prediction
    validation["outcome"] = outcome
    validation["score_minus_threshold"] = score - threshold
    validation["absolute_threshold_distance"] = np.abs(score - threshold)

    expected = decision["validation_operating_point"]
    counts = {
        name.lower(): int(np.sum(outcome == name))
        for name in ("TN", "FP", "FN", "TP")
    }
    for name, value in counts.items():
        if value != expected[name]:
            raise ValueError(f"Confusion matrix mismatch: {name}")

    # Slice boundaries học từ train, không từ validation/test.
    train = raw.iloc[split["indices"]["train"]]
    boundaries = {}
    for column, prefix in [
        ("Rotational speed [rpm]", "rpm"),
        ("Torque [Nm]", "torque"),
        ("Tool wear [min]", "wear"),
    ]:
        q25, q75 = train[column].quantile([0.25, 0.75]).tolist()
        boundaries[prefix] = {"train_q25": q25, "train_q75": q75}
        validation[f"{prefix}_slice"] = np.select(
            [
                validation[column] < q25,
                validation[column] > q75,
            ],
            ["below_train_q25", "above_train_q75"],
            default="train_q25_to_q75",
        )

    slice_rows = []
    for column in ("Type", "rpm_slice", "torque_slice", "wear_slice"):
        for value, group in validation.groupby(column, sort=True):
            tp = int((group["outcome"] == "TP").sum())
            fp = int((group["outcome"] == "FP").sum())
            fn = int((group["outcome"] == "FN").sum())
            slice_rows.append({
                "slice": f"{column}={value}",
                "samples": len(group),
                "failures": tp + fn,
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "precision": tp / (tp + fp) if tp + fp else None,
                "recall": tp / (tp + fn) if tp + fn else None,
            })

    # Modes chỉ phục vụ phân tích sau dự đoán, không phải model inputs.
    mode_masks = {
        mode: validation[mode] == 1 for mode in MODES
    }
    active_modes = validation[MODES].sum(axis=1)
    mode_masks["no_mode_flag"] = active_modes == 0
    mode_masks["multiple_mode_flags"] = active_modes > 1

    mode_rows = []
    for name, mask in mode_masks.items():
        group = validation.loc[mask]
        positives = group[group["Machine failure"] == 1]
        detected = int((positives["prediction"] == 1).sum())
        missed = int((positives["prediction"] == 0).sum())
        mode_rows.append({
            "group": name,
            "samples": len(group),
            "target_failures": len(positives),
            "detected_failures": detected,
            "missed_failures": missed,
            "recall_on_target_failures": (
                detected / len(positives) if len(positives) else None
            ),
            "target_normal_rows": int(
                (group["Machine failure"] == 0).sum()
            ),
        })

    errors = validation[
        validation["outcome"].isin(["FN", "FP"])
    ].copy()
    errors = errors.sort_values(
        ["outcome", "absolute_threshold_distance"]
    )
    error_columns = FEATURES + ["Machine failure"] + MODES + [
        "failure_score", "prediction", "outcome",
        "score_minus_threshold", "absolute_threshold_distance",
        "rpm_slice", "torque_slice", "wear_slice",
    ]

    output = Path(tempfile.mkdtemp(
        prefix="validation-errors-", dir=directory
    ))
    errors[error_columns].to_csv(
        output / "errors.csv", index_label="row_index"
    )
    slices = pd.DataFrame(slice_rows)
    modes = pd.DataFrame(mode_rows)
    slices.to_csv(output / "error_slices.csv", index=False)
    modes.to_csv(output / "failure_mode_analysis.csv", index=False)

    notes = """# Validation error analysis

- Scope: frozen HGB candidate, saved validation predictions.
- Failure-mode flags are used only for post-prediction analysis.
- Target remains Machine failure; mode flags do not replace the target.
- Mode groups can overlap. Do not sum their counts as distinct failures.
- A flag is a dataset annotation, not proof of the model's causal reasoning.
- Groups with few failures cannot support strong performance conclusions.
- Threshold distance is a score margin, not calibrated uncertainty.
- Slice boundaries come from the training set.
- No row is removed, relabelled or used to modify the frozen model.
- This report checks saved scores and identities; it does not rerun inference.
- Test data are not evaluated in this analysis.
"""
    (output / "README.md").write_text(notes, encoding="utf-8")

    receipt = {
        "scope": "frozen_candidate_validation_error_analysis",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model_version": decision["model_version"],
        "threshold": threshold,
        "confusion_matrix": counts,
        "slice_boundaries": boundaries,
        "input_sha256": {
            "decision": sha256(decision_path),
            "dataset": sha256(data_path),
            "split": sha256(split_path),
            "experiment": sha256(experiment_path),
            "pipeline": sha256(pipeline_path),
            "saved_predictions": sha256(prediction_path),
        },
        "script_sha256": sha256(Path(__file__)),
        "outputs_sha256": {
            path.name: sha256(path)
            for path in sorted(output.iterdir()) if path.is_file()
        },
        "model_or_threshold_changed": False,
        "test_evaluated": False,
        "approved_for_release": False,
    }
    (output / "verification.json").write_text(
        json.dumps(receipt, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    print("Frozen identities / membership / targets: PASS")
    print("Confusion matrix:", counts)
    print("\nFAILURE MODE ANALYSIS")
    print(modes.to_string(index=False))
    print("\nVALIDATION SLICES")
    print(slices.to_string(index=False))
    print("\nOutput:", output)
    print("Model changed / threshold changed / test evaluated: NO")


if __name__ == "__main__":
    main()