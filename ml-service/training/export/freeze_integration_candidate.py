"""Khóa HGB cho đóng gói thử tích hợp, không phê duyệt release."""

import argparse
import hashlib
import json
import math
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

    destination = directory / "integration_candidate.json"
    if destination.exists():
        raise FileExistsError(
            "Decision already exists; inspect it instead of overwriting."
        )

    metadata_path = directory / "benchmark_metadata.json"
    metadata = read_json(metadata_path)

    if metadata["evaluation_split"] != "validation":
        raise ValueError("Expected validation evaluation.")
    if metadata["test_evaluated"] is not False:
        raise ValueError("Unexpected evaluation scope.")
    if metadata["test_previously_opened_for_v1"] is not True:
        raise ValueError("Prior test exposure must be recorded.")

    candidate = "hgb_interactions"
    experiment_path = directory / candidate / "experiment.json"
    experiment = read_json(experiment_path)
    point = experiment["validation_selected_operating_point"]

    if experiment["candidate"] != candidate:
        raise ValueError("Candidate identity mismatch.")

    threshold = point["threshold"]
    if (
        type(threshold) not in (int, float)
        or not math.isfinite(threshold)
        or not 0 <= threshold <= 1
    ):
        raise ValueError("Invalid threshold.")

    if not (
        point["recall"] >= 0.85
        and point["precision"] >= 0.50
        and point["gate_pass"] is True
    ):
        raise ValueError("Candidate does not pass validation gate.")

    rf = read_json(directory / "rf_interactions/experiment.json")
    rf_point = rf["validation_selected_operating_point"]
    if (
        rf_point["gate_pass"]
        and rf_point["f2"] > point["f2"]
    ):
        raise ValueError("HGB does not lead the feasible candidates by F2.")

    artifact = directory / candidate / "pipeline.joblib"
    transformer = (
        ROOT / "ml-service/shared/preprocessing/feature_interactions.py"
    )
    source = ROOT / "sensor/data/ai4i2020.csv"
    split = (
        ROOT / "ml-service/training/data_preparation/splits"
        / "ai4i-stratified-v1-seed42.json"
    )

    for path, expected in [
        (artifact, experiment["pipeline_sha256"]),
        (transformer, experiment["transformer_sha256"]),
        (source, metadata["source_sha256"]),
        (split, metadata["split_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Checksum mismatch: {path}")

    analysis = directory / "validation_analysis"
    analysis_path = analysis / "analysis_metadata.json"
    analysis_metadata = read_json(analysis_path)
    analysis_input = analysis_metadata["inputs"][candidate]

    if analysis_metadata["scope"] != "validation_only":
        raise ValueError("Unexpected slice analysis scope.")
    if analysis_metadata["source_sha256"] != metadata["source_sha256"]:
        raise ValueError("Slice dataset mismatch.")
    if analysis_metadata["split_sha256"] != metadata["split_sha256"]:
        raise ValueError("Slice split mismatch.")
    if analysis_input["experiment_sha256"] != sha256(experiment_path):
        raise ValueError("Slice experiment mismatch.")
    if analysis_input["threshold"] != threshold:
        raise ValueError("Slice threshold mismatch.")
    if analysis_input["validation_predictions_sha256"] != sha256(
        directory / candidate / "validation_predictions.csv"
    ):
        raise ValueError("Slice predictions mismatch.")

    decision = {
        "schema_version": "local-integration-candidate-v1",
        "status": "frozen_for_local_integration_not_approved_release",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "candidate": candidate,
        "model_version": "hgb-interactions-candidate-v1",
        "preprocessor_version": "sensor-interactions-onehot-v1",
        "threshold": threshold,
        "prediction_rule": "FAILURE if score >= threshold else NORMAL",
        "raw_features": experiment["features"],
        "derived_features": experiment["derived_features"],
        "model_params": experiment["model_params"],
        "pipeline_relative_path": f"{candidate}/pipeline.joblib",
        "pipeline_sha256": sha256(artifact),
        "transformer_module": "feature_interactions",
        "transformer_sha256": sha256(transformer),
        "source_sha256": metadata["source_sha256"],
        "split_sha256": metadata["split_sha256"],
        "experiment_sha256": sha256(experiment_path),
        "benchmark_metadata_sha256": sha256(metadata_path),
        "slice_analysis_metadata_sha256": sha256(analysis_path),
        "slice_report_sha256": sha256(analysis / "slices.csv"),
        "validation_operating_point": point,
        "validation_average_precision": experiment["average_precision"],
        "selection_reason": (
            "Both interaction candidates pass validation gates. "
            "HGB has higher F2 and 8 FP versus RF's 28 FP, "
            "with both detecting 58 of 68 failures."
        ),
        "limitations": [
            "Validation has been reused during development.",
            "Original test was already evaluated for RF v1.",
            "Recall is close to the validation gate boundary.",
            "Type H contains only two validation failures.",
            "Middle RPM slice detects only one of five failures.",
            "High tool-wear slice contains eight of ten false negatives.",
            "Production latency, load and release integration remain unverified.",
        ],
        "quality_gate": {
            "recall_failure_min": 0.85,
            "precision_failure_min": 0.50,
            "validation_passed": True,
            "candidate_test_evaluated": False,
        },
        "approved_for_release": False,
        "test_previously_opened_for_v1": True,
        "refitted_after_selection": False,
        "selection_script_sha256": sha256(Path(__file__)),
        "selection_git_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
    }

    with destination.open("x", encoding="utf-8") as stream:
        json.dump(
            decision, stream, indent=2,
            ensure_ascii=False, allow_nan=False,
        )
        stream.write("\n")

    print("Candidate:", candidate)
    print("Threshold:", repr(threshold))
    print("Pipeline SHA-256:", sha256(artifact))
    print("Transformer SHA-256:", sha256(transformer))
    print("Decision SHA-256:", sha256(destination))
    print("Output:", destination)
    print("Validation gate: PASS")
    print("Candidate test evaluated: NO")
    print("Approved for release: NO")


if __name__ == "__main__":
    main()