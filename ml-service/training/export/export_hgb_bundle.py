"""Xuất bundle HGB cho thử tích hợp local; chưa phê duyệt release."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import sklearn


ROOT = Path(__file__).resolve().parents[3]
EXPECTED_DECISION_SHA256 = (
    "d8c699bc75e6af5a12b0080855f2d610"
    "95284c4ea317058ad05434b8d38abc08"
)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(
            value, stream, indent=2,
            ensure_ascii=False, allow_nan=False,
        )
        stream.write("\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    decision_path = directory / "integration_candidate.json"
    if sha256(decision_path) != EXPECTED_DECISION_SHA256:
        raise ValueError("Frozen decision checksum mismatch.")

    decision = read_json(decision_path)
    benchmark_path = directory / "benchmark_metadata.json"

    if sha256(benchmark_path) != decision["benchmark_metadata_sha256"]:
        raise ValueError("Benchmark metadata changed.")
    benchmark = read_json(benchmark_path)

    experiment_path = directory / decision["candidate"] / "experiment.json"
    if sha256(experiment_path) != decision["experiment_sha256"]:
        raise ValueError("Experiment changed.")

    if decision["approved_for_release"] is not False:
        raise ValueError("Expected unapproved local candidate.")
    if sklearn.__version__ != benchmark["sklearn_version"]:
        raise ValueError("Scikit-learn version mismatch.")

    pipeline_source = (directory / decision["pipeline_relative_path"]).resolve()
    if not pipeline_source.is_relative_to(directory):
        raise ValueError("Pipeline path escapes benchmark directory.")

    shared = ROOT / "ml-service/shared/preprocessing"
    transformer = shared / "feature_interactions.py"

    for path, expected in [
        (pipeline_source, decision["pipeline_sha256"]),
        (transformer, decision["transformer_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Checksum mismatch: {path}")

    output_root = ROOT / "ml-service/artifacts/integration"
    output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="hgb-interactions-", dir=output_root))

    shutil.copyfile(pipeline_source, output / "pipeline.joblib")
    shutil.copyfile(decision_path, output / "integration_candidate.json")

    # Bản source để truy vết; runtime dùng module đóng trong image.
    (output / "source").mkdir()
    shutil.copyfile(transformer, output / "source/feature_interactions.py")

    if sha256(output / "pipeline.joblib") != decision["pipeline_sha256"]:
        raise ValueError("Pipeline copy mismatch.")
    if sha256(output / "source/feature_interactions.py") != decision["transformer_sha256"]:
        raise ValueError("Transformer copy mismatch.")

    dataset_version = "ai4i2020-train-split-v1-seed42"

    metadata = {
        "status": "local_integration_candidate_not_approved_release",
        "approved_for_release": False,
        "model_version": decision["model_version"],
        "preprocessor_version": decision["preprocessor_version"],
        "dataset_version": dataset_version,
        "threshold": decision["threshold"],
        "threshold_status": "selected_on_reused_validation",
        "features": decision["raw_features"],
        "derived_features": decision["derived_features"],
        "transformer_module": "feature_interactions",
        "transformer_sha256": decision["transformer_sha256"],
        "pipeline_sha256": decision["pipeline_sha256"],
        "source_sha256": decision["source_sha256"],
        "split_sha256": decision["split_sha256"],
        "selection_sha256": sha256(decision_path),
        "train_rows": benchmark["train_rows"],
        "validation_rows": benchmark["validation_rows"],
        "evaluation_split": "validation",
        "sklearn_version": benchmark["sklearn_version"],
        "python_version": benchmark["python_version"],
        "source_git_commit": benchmark["source_git_commit"],
        "model_params": decision["model_params"],
    }
    write_json(output / "metadata.json", metadata)

    write_json(output / "evaluation.json", {
        "scope": "validation_only_reused_during_development",
        "candidate": decision["candidate"],
        "threshold": decision["threshold"],
        "metrics": decision["validation_operating_point"],
        "average_precision": decision["validation_average_precision"],
        "quality_gate": decision["quality_gate"],
        "limitations": decision["limitations"],
        "test_previously_opened_for_v1": True,
        "candidate_test_evaluated": False,
        "approved_for_release": False,
    })

    # Exporter cũ kiểm tra split, dataset, pipeline và reference schema.
    # Bổ sung đường dẫn để joblib import được transformer đã kiểm tra hash.
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(shared)

    subprocess.run(
        [
            sys.executable,
            str(ROOT / "ml-service/training/export/export_drift_reference.py"),
            "--model-dir", str(output),
            "--dataset-version", dataset_version,
        ],
        cwd=ROOT,
        env=environment,
        check=True,
    )

    reference = read_json(output / "drift_reference.json")
    if (
        reference["model_version"] != metadata["model_version"]
        or reference["dataset_version"] != dataset_version
        or reference["sample_count"] != metadata["train_rows"]
        or reference["reference_source"] != "training"
    ):
        raise ValueError("Drift reference identity mismatch.")

    files = {
        path.relative_to(output).as_posix(): sha256(path)
        for path in sorted(output.rglob("*"))
        if path.is_file()
    }

    write_json(output / "bundle-manifest.json", {
        "scope": "local_integration_not_approved_release",
        "model_version": metadata["model_version"],
        "selection_sha256": sha256(decision_path),
        "export_script_sha256": sha256(Path(__file__)),
        "drift_exporter_sha256": sha256(
            ROOT / "ml-service/training/export/export_drift_reference.py"
        ),
        "files": files,
    })

    print("\nBundle export: PASS")
    print("Model:", metadata["model_version"])
    print("Threshold:", repr(metadata["threshold"]))
    print("Drift reference: training, six raw features")
    print("Bundle:", output)
    print("Approved for release: NO")


if __name__ == "__main__":
    main()