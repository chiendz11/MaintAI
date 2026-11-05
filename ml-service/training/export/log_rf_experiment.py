"""Nhập kết quả RF đã đánh giá lên MLflow local, không train/promote."""

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Cấu hình trước khi import MLflow.
os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "30"
os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"

from mlflow.entities import Metric, Param
from mlflow.tracking import MlflowClient


TRACKING_URI = "http://127.0.0.1:5000"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def save_receipt(path, receipt):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(receipt, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()

    directory = args.benchmark_dir.resolve()
    folder = directory / "random_forest"
    receipt_path = directory / "mlflow_rf_tracking_receipt.json"

    if receipt_path.exists():
        raise FileExistsError(
            "Tracking receipt already exists. Inspect it before retrying."
        )

    selection_path = directory / "frozen_selection.json"
    test_path = directory / "heldout_test_report.json"
    benchmark_path = directory / "benchmark_metadata.json"
    experiment_path = folder / "experiment.json"
    model_path = folder / "pipeline.joblib"

    selection = read_json(selection_path)
    test = read_json(test_path)
    benchmark = read_json(benchmark_path)
    experiment = read_json(experiment_path)

    # Khóa vào đúng kết quả đã được kiểm tra trong phiên làm việc này.
    expected_selection_hash = (
        "4ddd760641a6147fc6d4481aa99f1c9553"
        "d2e65eb9f1d1ad7da73e4f0d672857"
    )
    expected_test_hash = (
        "fa43e175d7fdbc8c124efab5936a64625"
        "b26e97b0bcda72772599761f336285e"
    )

    if sha256(selection_path) != expected_selection_hash:
        raise ValueError("Frozen selection changed.")
    if sha256(test_path) != expected_test_hash:
        raise ValueError("Test report changed.")
    if sha256(benchmark_path) != selection["benchmark_metadata_sha256"]:
        raise ValueError("Benchmark metadata changed.")
    if sha256(experiment_path) != selection["experiment_sha256"]:
        raise ValueError("Experiment changed.")
    if sha256(model_path) != selection["pipeline_sha256"]:
        raise ValueError("Model artifact changed.")

    if test["provenance"]["selection_sha256"] != sha256(selection_path):
        raise ValueError("Test report references another selection.")
    if test["provenance"]["pipeline_sha256"] != sha256(model_path):
        raise ValueError("Test report references another model.")
    if test["threshold"] != selection["threshold"]:
        raise ValueError("Threshold mismatch.")
    if test["quality_gate"]["passed"] is not False:
        raise ValueError("This importer expects the rejected RF v1 run.")
    if test["approved_for_release"] is not False:
        raise ValueError("Unexpected approval status.")

    artifacts = [
        (model_path, "candidate"),
        (experiment_path, "candidate"),
        (selection_path, "evaluation"),
        (test_path, "evaluation"),
        (benchmark_path, "provenance"),
        (directory / "comparison.csv", "evaluation"),
        (directory / "validation_analysis/slices.csv", "evaluation"),
        (directory / "validation_analysis/analysis_metadata.json", "provenance"),
    ]

    for path, _ in artifacts:
        if not path.is_file():
            raise FileNotFoundError(path)

    client = MlflowClient(
        tracking_uri=TRACKING_URI,
        registry_uri=TRACKING_URI,
    )

    # Dùng experiment local đã kiểm chứng; không tạo production experiment.
    experiment_info = client.get_experiment_by_name("Default")
    if experiment_info is None:
        raise RuntimeError("Local Default experiment was not found.")

    receipt = {
        "state": "CREATE_INTENT",
        "scope": "local_import_of_existing_experiment",
        "tracking_uri": TRACKING_URI,
        "experiment_id": experiment_info.experiment_id,
        "selection_sha256": sha256(selection_path),
        "test_report_sha256": sha256(test_path),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "uploader_script_sha256": sha256(Path(__file__)),
    }

    # Ghi intent trước remote create; nếu lỗi không xóa receipt rồi tạo lại.
    with receipt_path.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2)
        stream.write("\n")

    run = client.create_run(
        experiment_id=experiment_info.experiment_id,
        run_name="rf-v1-heldout-test-rejected",
        tags={
            "maintai.candidate": "random_forest",
            "maintai.execution_mode": "import_existing_results",
            "maintai.scope": "local_development",
            "maintai.test_gate": "FAIL",
            "maintai.approved_for_release": "false",
            "maintai.selection_sha256": sha256(selection_path),
            "maintai.training_commit": benchmark["source_git_commit"],
            "maintai.training_script_sha256": benchmark["script_sha256"],
        },
    )
    run_id = run.info.run_id

    receipt["run_id"] = run_id
    receipt["state"] = "RUN_CREATED"
    save_receipt(receipt_path, receipt)

    params = {
        f"model.{name}": str(value)
        for name, value in experiment["model_params"].items()
    }
    params.update({
        "threshold": repr(selection["threshold"]),
        "dataset_sha256": selection["source_sha256"],
        "split_sha256": selection["split_sha256"],
        "pipeline_sha256": selection["pipeline_sha256"],
        "sklearn_version": benchmark["sklearn_version"],
        "seed": str(benchmark["seed"]),
        "gate_version": selection["gate"]["version"],
        "numeric_scaling": str(experiment["numeric_scaling"]),
    })

    validation = experiment["validation_selected_operating_point"]
    values = {
        f"validation.{name}": float(validation[name])
        for name in [
            "precision", "recall", "f1", "f2",
            "fnr", "fp_per_1000", "tn", "fp", "fn", "tp",
        ]
    }
    values["validation.average_precision"] = experiment["average_precision"]

    values.update({
        f"test.{name}": float(value)
        for name, value in test["metrics"].items()
    })
    values["quality_gate.test_passed"] = 0.0

    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    client.log_batch(
        run_id,
        params=[Param(key, value) for key, value in params.items()],
        metrics=[
            Metric(key, value, timestamp, 0)
            for key, value in values.items()
        ],
        synchronous=True,
    )

    receipt["artifacts"] = {}
    for path, artifact_folder in artifacts:
        remote_path = f"{artifact_folder}/{path.name}"
        client.log_artifact(
            run_id, str(path), artifact_path=artifact_folder,
        )
        receipt["artifacts"][remote_path] = sha256(path)

    receipt["state"] = "UPLOADED"
    save_receipt(receipt_path, receipt)

    # Đọc lại params, metrics và trạng thái phê duyệt.
    loaded = client.get_run(run_id)
    for key, expected in params.items():
        if loaded.data.params.get(key) != expected:
            raise ValueError(f"Parameter read-back mismatch: {key}")
    for key, expected in values.items():
        if loaded.data.metrics.get(key) != expected:
            raise ValueError(f"Metric read-back mismatch: {key}")
    if loaded.data.tags.get("maintai.approved_for_release") != "false":
        raise ValueError("Approval tag mismatch.")

    # Tải lại từng artifact để kiểm chứng byte thực sự được lưu.
    with tempfile.TemporaryDirectory() as temporary:
        for remote_path, expected_hash in receipt["artifacts"].items():
            downloaded = client.download_artifacts(
                run_id, remote_path, temporary,
            )
            if sha256(Path(downloaded)) != expected_hash:
                raise ValueError(f"Artifact mismatch: {remote_path}")

    client.set_terminated(run_id, status="FINISHED")
    if client.get_run(run_id).info.status != "FINISHED":
        raise ValueError("Unexpected run status.")

    receipt["state"] = "VERIFIED"
    save_receipt(receipt_path, receipt)

    print("Params / metrics read-back: PASS")
    print("Artifact download / SHA-256:", len(artifacts), "PASS")
    print("Run ID:", run_id)
    print("Test gate: FAIL")
    print("Approved for release: NO")
    print("Registry model created: NO")
    print("Receipt:", receipt_path)
    print(
        "UI:",
        f"{TRACKING_URI}/#/experiments/"
        f"{experiment_info.experiment_id}/runs/{run_id}",
    )


if __name__ == "__main__":
    main()