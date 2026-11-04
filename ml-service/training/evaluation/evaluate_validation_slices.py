"""Phân tích slices validation từ benchmark đã chạy; không dùng test."""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
CANDIDATES = ["random_forest", "hist_gradient_boosting"]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(frame, mask, candidate, slice_name, threshold):
    subset = frame.loc[mask]
    y = subset["target"].to_numpy(dtype=int)
    predicted = subset["failure_score"].to_numpy() >= threshold

    tp = int(np.sum((y == 1) & predicted))
    fn = int(np.sum((y == 1) & ~predicted))
    fp = int(np.sum((y == 0) & predicted))
    tn = int(np.sum((y == 0) & ~predicted))

    return {
        "candidate": candidate,
        "slice": slice_name,
        "samples": len(subset),
        "failures": tp + fn,
        "predicted_failures": tp + fp,
        # Không có mẫu để ước lượng thì để trống, không báo 0 giả.
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "threshold": threshold,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    benchmark_dir = args.benchmark_dir.resolve()

    metadata_path = benchmark_dir / "benchmark_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    source_path = ROOT / "sensor/data/ai4i2020.csv"
    split_path = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )

    if sha256(source_path) != metadata["source_sha256"]:
        raise ValueError("Dataset hash differs from benchmark.")
    if sha256(split_path) != metadata["split_sha256"]:
        raise ValueError("Split hash differs from benchmark.")
    if metadata["evaluation_split"] != "validation":
        raise ValueError("Expected validation benchmark.")
    if metadata["test_evaluated"] is not False:
        raise ValueError("Unexpected benchmark evaluation scope.")

    manifest = json.loads(split_path.read_text(encoding="utf-8"))
    data = pd.read_csv(source_path)

    train = data.iloc[manifest["indices"]["train"]]
    validation_indices = manifest["indices"]["validation"]
    validation = data.iloc[validation_indices].reset_index(drop=True)

    # Chốt ranh giới từ train, không lựa chọn theo lỗi validation.
    slice_columns = {
        "rpm": "Rotational speed [rpm]",
        "torque": "Torque [Nm]",
        "tool_wear": "Tool wear [min]",
    }

    boundaries = {
        name: [
            float(train[column].quantile(0.25)),
            float(train[column].quantile(0.75)),
        ]
        for name, column in slice_columns.items()
    }

    masks = {
        "all": np.ones(len(validation), dtype=bool),
    }
    for product_type in ["L", "M", "H"]:
        masks[f"type={product_type}"] = (
            validation["Type"].to_numpy() == product_type
        )

    for name, column in slice_columns.items():
        q25, q75 = boundaries[name]
        values = validation[column].to_numpy()

        masks[f"{name}:below_train_q25"] = values < q25
        masks[f"{name}:train_q25_to_q75"] = (
            (values >= q25) & (values <= q75)
        )
        masks[f"{name}:above_train_q75"] = values > q75

    results = []
    error_tables = []
    evidence = {
        "scope": "validation_only",
        "source_sha256": sha256(source_path),
        "split_sha256": sha256(split_path),
        "script_sha256": sha256(Path(__file__)),
        "boundaries_source": "train",
        "boundaries": boundaries,
        "inputs": {},
    }

    for candidate in CANDIDATES:
        folder = benchmark_dir / candidate
        experiment_path = folder / "experiment.json"
        predictions_path = folder / "validation_predictions.csv"

        experiment = json.loads(
            experiment_path.read_text(encoding="utf-8")
        )
        predictions = pd.read_csv(predictions_path)

        if predictions["row_index"].tolist() != validation_indices:
            raise ValueError(f"{candidate}: validation indices mismatch.")

        expected_labels = validation["Machine failure"].to_numpy()
        if not np.array_equal(
            predictions["target"].to_numpy(), expected_labels
        ):
            raise ValueError(f"{candidate}: labels mismatch.")

        scores = predictions["failure_score"].to_numpy(dtype=float)
        if (
            not np.all(np.isfinite(scores))
            or np.any(scores < 0)
            or np.any(scores > 1)
        ):
            raise ValueError(f"{candidate}: invalid scores.")

        threshold = experiment[
            "validation_selected_operating_point"
        ]["threshold"]

        if (
            type(threshold) not in (int, float)
            or not np.isfinite(threshold)
            or not 0 <= threshold <= 1
        ):
            raise ValueError(f"{candidate}: invalid threshold.")

        for slice_name, mask in masks.items():
            results.append(
                summarize(
                    predictions, mask, candidate, slice_name, threshold
                )
            )

        # Lưu các lỗi validation để phân tích, không dùng fault labels làm input.
        predicted = scores >= threshold
        wrong = predicted != expected_labels

        errors = validation.loc[wrong].copy()
        errors.insert(
            0, "row_index", np.asarray(validation_indices)[wrong]
        )
        errors.insert(0, "candidate", candidate)
        errors["failure_score"] = scores[wrong]
        errors["threshold"] = threshold
        errors["error_type"] = np.where(
            expected_labels[wrong] == 1, "FN", "FP"
        )
        error_tables.append(errors)

        evidence["inputs"][candidate] = {
            "experiment_sha256": sha256(experiment_path),
            "validation_predictions_sha256": sha256(predictions_path),
            "threshold": threshold,
        }

    output = benchmark_dir / "validation_analysis"
    output.mkdir(exist_ok=True)

    report = pd.DataFrame(results)
    report.to_csv(output / "slices.csv", index=False)
    pd.concat(error_tables, ignore_index=True).to_csv(
        output / "validation_errors.csv", index=False
    )
    (output / "analysis_metadata.json").write_text(
        json.dumps(evidence, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    print("\nVALIDATION SLICES")
    print(
        report[
            ["candidate", "slice", "samples", "failures",
             "precision", "recall", "fp", "fn"]
        ].round(4).to_string(index=False)
    )
    print("\nOutput:", output)
    print("Thresholds: unchanged from benchmark.")
    print("Test evaluated: NO")


if __name__ == "__main__":
    main()