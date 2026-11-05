"""Khóa candidate trước đánh giá test; không load hoặc đánh giá test."""

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()

    directory = args.benchmark_dir.resolve()
    destination = directory / "frozen_selection.json"

    # Không âm thầm ghi đè một quyết định đã khóa.
    if destination.exists():
        raise FileExistsError(
            f"Selection already exists: {destination}. "
            "Inspect the existing decision; do not overwrite."
        )

    benchmark_path = directory / "benchmark_metadata.json"
    benchmark = read_json(benchmark_path)

    if benchmark["evaluation_split"] != "validation":
        raise ValueError("Selection must use validation results.")
    if benchmark["test_evaluated"] is not False:
        raise ValueError("Unexpected benchmark evaluation scope.")

    source = ROOT / "sensor/data/ai4i2020.csv"
    split = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )
    protocol = (
        ROOT / "ml-service/training/evaluation/BENCHMARK_PROTOCOL.md"
    )

    for path, expected in [
        (source, benchmark["source_sha256"]),
        (split, benchmark["split_sha256"]),
        (protocol, benchmark["protocol_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Input changed since benchmark: {path}")

    candidate = "random_forest"
    folder = directory / candidate
    experiment_path = folder / "experiment.json"
    experiment = read_json(experiment_path)
    point = experiment["validation_selected_operating_point"]

    if experiment["candidate"] != candidate:
        raise ValueError("Candidate identity mismatch.")

    if not (
        point["gate_pass"] is True
        and point["recall"] >= 0.85
        and point["precision"] >= 0.50
    ):
        raise ValueError("Selected candidate does not pass validation gate.")

    # Kiểm chứng RF có F2 cao nhất trong các candidate học máy đạt gate.
    feasible = []
    for name in [
        "logreg",
        "logreg_balanced",
        "decision_tree",
        "random_forest",
        "hist_gradient_boosting",
    ]:
        other = read_json(directory / name / "experiment.json")
        other_point = other["validation_selected_operating_point"]

        if (
            other_point["recall"] >= 0.85
            and other_point["precision"] >= 0.50
        ):
            feasible.append({
                "candidate": name,
                "f2": other_point["f2"],
                "average_precision": other["average_precision"],
            })

    best_f2 = max(item["f2"] for item in feasible)
    if point["f2"] != best_f2:
        raise ValueError("RF is not the highest-F2 feasible candidate.")

    artifact = folder / "pipeline.joblib"
    artifact_hash = sha256(artifact)
    if artifact_hash != experiment["pipeline_sha256"]:
        raise ValueError("Pipeline checksum mismatch.")

    analysis = directory / "validation_analysis"
    analysis_metadata = read_json(analysis / "analysis_metadata.json")

    if analysis_metadata["scope"] != "validation_only":
        raise ValueError("Unexpected slice analysis scope.")

    analysis_input = analysis_metadata["inputs"][candidate]
    if analysis_input["experiment_sha256"] != sha256(experiment_path):
        raise ValueError("Slice analysis used a different experiment.")
    if analysis_input["validation_predictions_sha256"] != sha256(
        folder / "validation_predictions.csv"
    ):
        raise ValueError("Validation predictions changed after analysis.")
    if analysis_input["threshold"] != point["threshold"]:
        raise ValueError("Slice analysis used a different threshold.")

    decision = {
        "schema_version": "local-selection-v1",
        "status": "frozen_for_test_not_approved_release",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "candidate": candidate,
        "artifact_relative_path": f"{candidate}/pipeline.joblib",
        "pipeline_sha256": artifact_hash,
        "threshold": point["threshold"],
        "prediction_rule": "FAILURE if score >= threshold else NORMAL",
        "features": experiment["features"],
        "model_params": experiment["model_params"],
        "validation_operating_point": point,
        "validation_average_precision": experiment["average_precision"],
        "feasible_candidates": feasible,
        "selection_reason": (
            "Highest validation F2 among candidates meeting both gates; "
            "slice review identifies limitations but no demonstrated "
            "reason to replace it with the runner-up."
        ),
        "limitations": [
            "Precision equals the validation gate boundary of 0.50.",
            "Validation contains only 68 Failure samples.",
            "Type H contains only 2 validation Failure samples.",
            "Weaker recall on Type M and high tool-wear slice.",
            "No demonstrated statistical superiority over boosting.",
            "Latency/load/resource acceptance is not yet established.",
        ],
        "gate": {
            "version": "maintai-recall085-precision050-v1",
            "recall_failure_min": 0.85,
            "precision_failure_min": 0.50,
        },
        "source_sha256": benchmark["source_sha256"],
        "split_sha256": benchmark["split_sha256"],
        "protocol_sha256": benchmark["protocol_sha256"],
        "benchmark_metadata_sha256": sha256(benchmark_path),
        "experiment_sha256": sha256(experiment_path),
        "slice_report_sha256": sha256(analysis / "slices.csv"),
        "selection_script_sha256": sha256(Path(__file__)),
        "selection_git_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
        ).strip(),
        "test_evaluated": False,
        "refit_before_test": False,
    }

    with destination.open("x", encoding="utf-8") as stream:
        json.dump(
            decision, stream, indent=2,
            ensure_ascii=False, allow_nan=False,
        )
        stream.write("\n")

    print("Result: FROZEN")
    print("Candidate:", candidate)
    print("Threshold:", repr(point["threshold"]))
    print("Pipeline SHA-256:", artifact_hash)
    print("Selection SHA-256:", sha256(destination))
    print("Output:", destination)
    print("Test evaluated: NO")
    print("Approved for release: NO")


if __name__ == "__main__":
    main()