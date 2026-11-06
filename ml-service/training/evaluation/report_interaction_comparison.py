"""Kiểm chứng và báo cáo RF/HGB interactions trên validation đã lưu."""

import argparse
import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, precision_recall_curve


ROOT = Path(__file__).resolve().parents[3]
SPLIT_SHA = (
    "94889871b1c814f5d36d67357c44b6fabc"
    "68a18040e7674658a64f7761483752"
)
DATA_SHA = (
    "dc6630cd9b1f0f853922fad78a1b643657"
    "0d3f1ec863f1dd5c4340ac56bc8a8e"
)
CANDIDATES = ("rf_interactions", "hgb_interactions")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def calculate(y, score, threshold):
    predicted = score >= threshold
    positive = y == 1

    tp = int(np.sum(predicted & positive))
    fp = int(np.sum(predicted & ~positive))
    fn = int(np.sum(~predicted & positive))
    tn = int(np.sum(~predicted & ~positive))

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn)
    f1 = 2 * tp / (2 * tp + fp + fn) if 2 * tp + fp + fn else 0.0
    f2 = 5 * tp / (5 * tp + fp + 4 * fn) if 5 * tp + fp + 4 * fn else 0.0

    return {
        "threshold": float(threshold),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "f2": f2,
        "fnr": fn / (fn + tp),
        "fp_per_1000": 1000 * fp / len(y),
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "tp": tp,
        "gate_pass": precision >= 0.50 and recall >= 0.85,
    }


def verify_metrics(actual, expected):
    for key, value in actual.items():
        if key == "gate_pass":
            if type(expected[key]) is not bool or value != expected[key]:
                raise ValueError(f"Gate mismatch: {actual} / {expected}")
        elif key in ("tn", "fp", "fn", "tp"):
            if value != expected[key]:
                raise ValueError(f"Count mismatch: {key}")
        elif not np.isclose(
            value, expected[key], rtol=1e-10, atol=1e-12
        ):
            raise ValueError(
                f"Metric mismatch: {key}: {value} != {expected[key]}"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    data_path = ROOT / "sensor/data/ai4i2020.csv"

    if sha256(split_path) != SPLIT_SHA:
        raise ValueError("Split checksum mismatch")
    if sha256(data_path) != DATA_SHA:
        raise ValueError("Dataset checksum mismatch")

    split = read_json(split_path)
    indices = np.asarray(split["indices"]["validation"], dtype=int)

    # Chỉ dùng nhãn validation để xác minh CSV prediction.
    labels = pd.read_csv(data_path, usecols=["Machine failure"])
    expected_y = labels.iloc[indices]["Machine failure"].to_numpy()

    if len(indices) != 2000 or int(np.sum(expected_y == 1)) != 68:
        raise ValueError("Unexpected validation population")

    rows = []
    curves = {}
    input_hashes = {
        "split": SPLIT_SHA,
        "dataset": DATA_SHA,
    }

    for candidate in CANDIDATES:
        folder = directory / candidate
        experiment_path = folder / "experiment.json"
        predictions_path = folder / "validation_predictions.csv"
        pipeline_path = folder / "pipeline.joblib"

        experiment = read_json(experiment_path)
        if experiment["candidate"] != candidate:
            raise ValueError("Candidate identity mismatch")
        if sha256(pipeline_path) != experiment["pipeline_sha256"]:
            raise ValueError(f"{candidate}: pipeline checksum mismatch")

        # Đọc score dạng chuỗi, sau đó dùng Python float để chuyển đổi rõ ràng.
        frame = pd.read_csv(
            predictions_path,
            dtype={"failure_score": "string"},
        )
        frame["failure_score"] = frame["failure_score"].map(float)
        if set(frame.columns) != {"row_index", "target", "failure_score"}:
            raise ValueError(f"{candidate}: unexpected prediction columns")
        if frame["row_index"].duplicated().any():
            raise ValueError(f"{candidate}: duplicate validation rows")
        if len(frame) != len(indices):
            raise ValueError(f"{candidate}: unexpected sample count")
        if set(frame["row_index"]) != set(indices):
            raise ValueError(f"{candidate}: validation membership mismatch")

        frame = frame.set_index("row_index").loc[indices]
        y = frame["target"].to_numpy()
        score = frame["failure_score"].to_numpy(dtype=float)

        if not np.array_equal(y, expected_y):
            raise ValueError(f"{candidate}: target mismatch")
        if (
            not np.isfinite(score).all()
            or np.any(score < 0)
            or np.any(score > 1)
        ):
            raise ValueError(f"{candidate}: invalid scores")

        ap = float(average_precision_score(y, score))
        if not np.isclose(
            ap, experiment["average_precision"],
            rtol=1e-10, atol=1e-12,
        ):
            raise ValueError(f"{candidate}: AP mismatch")

        for point, key in [
            ("default_0.5", "default_operating_point"),
            ("validation_selected", "validation_selected_operating_point"),
        ]:
            stored = experiment[key]
            threshold = stored["threshold"]

            if (
                type(threshold) not in (int, float)
                or not np.isfinite(threshold)
                or not 0 <= threshold <= 1
            ):
                raise ValueError("Invalid stored threshold")
            if point == "default_0.5" and threshold != 0.5:
                raise ValueError("Unexpected default threshold")

            actual = calculate(y, score, threshold)
            verify_metrics(actual, stored)
            rows.append({
                "candidate": candidate,
                "operating_point": point,
                **actual,
                "average_precision": ap,
                "samples": len(y),
                "failures": int(np.sum(y == 1)),
            })

        precision, recall, thresholds = precision_recall_curve(y, score)
        curves[candidate] = (precision, recall, thresholds, ap)
        for path in (experiment_path, predictions_path, pipeline_path):
            input_hashes[path.relative_to(directory).as_posix()] = sha256(path)

    # Chỉ tạo output sau khi tất cả phép đối chiếu đã thành công.
    output = Path(tempfile.mkdtemp(
        prefix="interaction-report-", dir=directory
    ))
    comparison = pd.DataFrame(rows)
    comparison.to_csv(output / "verified_comparison.csv", index=False)

    fig, ax = plt.subplots(figsize=(8, 6))
    colors = {"rf_interactions": "tab:blue", "hgb_interactions": "tab:orange"}

    for candidate, (precision, recall, thresholds, ap) in curves.items():
        color = colors[candidate]
        ax.step(
            recall, precision, where="post", color=color,
            label=f"{candidate}: AP={ap:.4f}",
        )

        # Điểm cuối PR curve không có threshold tương ứng.
        pd.DataFrame({
            "precision": precision,
            "recall": recall,
            "threshold": np.append(thresholds, np.nan),
        }).to_csv(output / f"{candidate}_pr_curve.csv", index=False)

        candidate_rows = comparison[
            comparison["candidate"] == candidate
        ]
        for row in candidate_rows.to_dict("records"):
            marker = "o" if row["operating_point"] == "default_0.5" else "X"
            ax.scatter(
                row["recall"], row["precision"],
                marker=marker, color=color, s=80, zorder=5,
            )

    ax.axhline(68 / 2000, linestyle=":", color="gray",
               label="Validation prevalence: 3.4%")
    ax.axhline(0.50, linestyle="--", color="gray", linewidth=0.8)
    ax.axvline(0.85, linestyle="--", color="gray", linewidth=0.8)
    ax.set(
        xlabel="Recall (Failure)",
        ylabel="Precision (Failure)",
        title="Validation PR curves — circle: 0.5; X: frozen selected threshold",
        xlim=(0, 1.01), ylim=(0, 1.01),
    )
    ax.legend(loc="lower left")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(output / "validation_pr_curves.png", dpi=180)
    plt.close(fig)

    receipt = {
        "scope": "verification_of_saved_validation_predictions",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "candidates": list(CANDIDATES),
        "samples": 2000,
        "failures": 68,
        "input_sha256": input_hashes,
        "script_sha256": sha256(Path(__file__)),
        "output_sha256": {
            path.name: sha256(path)
            for path in sorted(output.iterdir()) if path.is_file()
        },
        "metrics_readback": "PASS",
        "thresholds_changed": False,
        "model_trained": False,
        "test_evaluated": False,
        "approved_for_release": False,
        "limitation": (
            "Recomputes metrics from saved validation scores; "
            "does not rerun inference or independently authenticate "
            "the original prediction files."
        ),
    }
    (output / "verification.json").write_text(
        json.dumps(receipt, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    print(comparison.round(6).to_string(index=False, col_space=12))
    print("\nValidation membership / targets: PASS")
    print("Stored operating points / AP: PASS")
    print("Output:", output)
    print("Model trained: NO")
    print("Threshold changed: NO")
    print("Test evaluated: NO")


if __name__ == "__main__":
    main()