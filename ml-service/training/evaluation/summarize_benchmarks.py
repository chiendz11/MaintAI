"""Tổng hợp các vòng benchmark đã lưu; không train hoặc chọn lại model."""

import hashlib
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
BENCHMARKS = ROOT / "ml-service/artifacts/benchmarks"

ROUNDS = [
    ("v1_baselines", "candidates-k6dcz8rr"),
    ("v2_cv_winners", "cv-winners-v2-o63b6wep"),
    ("v3_interactions", "interactions-validation-mx8qrw6p"),
]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_row(row):
    counts = {}
    for key in ("tn", "fp", "fn", "tp"):
        value = float(row[key])
        if not math.isfinite(value) or value < 0 or not value.is_integer():
            raise ValueError(f"Invalid count: {key}")
        counts[key] = int(value)

    tn, fp, fn, tp = (counts[k] for k in ("tn", "fp", "fn", "tp"))
    if tn + fp != 1932 or fn + tp != 68:
        raise ValueError("Unexpected validation class counts")

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / 68
    expected = {
        "precision": precision,
        "recall": recall,
        "f1": 2 * tp / (2 * tp + fp + fn),
        "f2": 5 * tp / (5 * tp + fp + 4 * fn),
        "fnr": fn / 68,
        "fp_per_1000": fp / 2,
    }

    for key, value in expected.items():
        if not math.isclose(
            float(row[key]), value, rel_tol=1e-10, abs_tol=1e-12
        ):
            raise ValueError(f"{row['candidate']}: inconsistent {key}")

    gate = str(row["gate_pass"]).strip().lower()
    if gate not in ("true", "false"):
        raise ValueError("Invalid gate value")
    if (gate == "true") != (precision >= 0.50 and recall >= 0.85):
        raise ValueError("Quality gate mismatch")

    point = row["operating_point"]
    if point in ("default_0.5", "validation_selected"):
        threshold = float(row["threshold"])
        ap = float(row["average_precision"])
        if not math.isfinite(threshold) or not 0 <= threshold <= 1:
            raise ValueError("Invalid threshold")
        if not math.isfinite(ap) or not 0 <= ap <= 1:
            raise ValueError("Invalid AP")
        if point == "default_0.5" and threshold != 0.5:
            raise ValueError("Default threshold must be 0.5")
    elif point not in ("fixed_label", "fixed_rule"):
        raise ValueError(f"Unknown operating point: {point}")


def markdown_table(frame, columns):
    def display(value):
        if pd.isna(value):
            return "—"
        if isinstance(value, float):
            return f"{value:.6f}"
        return str(value).replace("|", r"\|")

    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for row in frame[columns].itertuples(index=False, name=None):
        lines.append("| " + " | ".join(map(display, row)) + " |")
    return "\n".join(lines)


def main():
    frames = []
    inputs = {}

    for round_name, folder in ROUNDS:
        path = BENCHMARKS / folder / "comparison.csv"
        # Giữ cách đọc số thực thống nhất với báo cáo interactions.
        frame = pd.read_csv(path, dtype=str)
        if frame.duplicated(["candidate", "operating_point"]).any():
            raise ValueError(f"Duplicate rows: {folder}")

        for row in frame.to_dict("records"):
            verify_row(row)

        frame.insert(0, "round", round_name)
        frame.insert(1, "source_benchmark", folder)
        frames.append(frame)
        inputs[f"{folder}/comparison.csv"] = sha256(path)

    combined = pd.concat(frames, ignore_index=True)
    if len(combined) != 20:
        raise ValueError("Expected 20 operating points for this report version")

    numeric = [
        "threshold", "average_precision", "precision", "recall",
        "f1", "f2", "fnr", "fp_per_1000",
        "tn", "fp", "fn", "tp",
        "fit_seconds", "predict_batch_seconds", "artifact_bytes",
    ]
    for column in numeric:
        combined[column] = combined[column].map(
            lambda value: float(value) if pd.notna(value) else float("nan")
        )

    summary = combined[
        combined["operating_point"].isin(
            ["fixed_label", "fixed_rule", "validation_selected"]
        )
    ].copy()

    output_root = ROOT / "ml-service/artifacts/evaluation_reports"
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="comparison-", dir=output_root))

    combined.to_csv(output / "all_operating_points.csv", index=False)
    summary.to_csv(output / "selected_and_fixed_points.csv", index=False)

    columns = [
        "round", "candidate", "threshold", "precision", "recall",
        "f2", "average_precision", "fp", "fn", "gate_pass",
    ]
    report = """# Validation comparison across development rounds

## Scope

This report combines saved benchmark results on the reused validation set.
It does not train, tune, evaluate test data or approve a release.

The rounds represent adaptive development, not independent replications.
Comparisons between rounds can involve changes in both configuration and
features. Controlled feature-effect evidence comes from the paired-fold
ablation, not this table alone.

## Selected or fixed operating points

""" + markdown_table(summary, columns) + """

## Interpretation

- Balanced Logistic Regression raises recall at threshold 0.5, but also
  increases false positives; high recall alone does not pass the gate.
- Better AP does not guarantee a feasible precision/recall operating point.
- At the saved selected thresholds, RF interactions and HGB interactions
  both detect 58 failures. HGB has 8 false positives, versus 28 for RF.
- HGB's selected threshold detects 3 more failures than threshold 0.5,
  at the cost of 6 additional false positives.
- F2 expresses a recall preference; it is not a validated financial cost model.
- Missing AP for fixed-label/rule baselines means not reported here.
  It does not mean AP is mathematically impossible to calculate.
- Fit/predict timings are retained in CSV as historical measurements.
  They are not a controlled repeated latency benchmark across all rounds.
- The frozen integration candidate is unchanged.

## Verification limits

Confusion-matrix totals, derived metrics and quality-gate flags were checked.
AP and thresholds are range-checked, not recomputed from scores in this script.
Class-count agreement alone does not establish identical sample membership.
The separate interaction report verifies membership, labels and score-based
metrics for RF/HGB interactions. Full verification of earlier candidates
requires their saved predictions and provenance.
"""
    (output / "REPORT.md").write_text(report, encoding="utf-8")

    receipt = {
        "scope": "historical_validation_comparison",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "input_sha256": inputs,
        "script_sha256": sha256(Path(__file__)),
        "rows": len(combined),
        "candidate_variants": int(combined["candidate"].nunique()),
        "confusion_matrix_arithmetic": "PASS",
        "quality_gate_arithmetic": "PASS",
        "model_or_threshold_changed": False,
        "test_evaluated": False,
        "approved_for_release": False,
        "outputs_sha256": {
            path.name: sha256(path)
            for path in sorted(output.iterdir()) if path.is_file()
        },
    }
    (output / "verification.json").write_text(
        json.dumps(receipt, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    print(summary[columns].to_string(index=False))
    print("\nConfusion matrix / derived metrics / gate: PASS")
    print("Operating points:", len(combined))
    print("Candidate variants:", combined["candidate"].nunique())
    print("Output:", output)
    print("Model trained / threshold changed / test evaluated: NO")


if __name__ == "__main__":
    main()