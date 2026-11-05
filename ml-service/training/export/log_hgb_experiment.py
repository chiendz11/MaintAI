"""Log HGB và kết quả test đã sử dụng; không train hoặc approve release."""

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

URI = "http://127.0.0.1:5000"
os.environ["MLFLOW_TRACKING_URI"] = URI
os.environ["MLFLOW_REGISTRY_URI"] = URI
os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "30"
os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"

import mlflow
from mlflow.entities import Metric, Param
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri(URI)
mlflow.set_registry_uri(URI)

ROOT = Path(__file__).resolve().parents[3]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def save_receipt(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    receipt_path = directory / "mlflow_hgb_tracking_receipt.json"
    if receipt_path.exists():
        raise FileExistsError("Receipt exists; inspect before retrying.")

    decision_path = directory / "integration_candidate.json"
    report_path = directory / "reused_test_report.json"
    benchmark_path = directory / "benchmark_metadata.json"
    folder = directory / "hgb_interactions"
    experiment_path = folder / "experiment.json"
    transformer = (
        ROOT / "ml-service/shared/preprocessing/feature_interactions.py"
    )

    if sha256(decision_path) != (
        "d8c699bc75e6af5a12b0080855f2d61095284c4ea317058ad05434b8d38abc08"
    ):
        raise ValueError("Frozen decision changed.")
    if sha256(report_path) != (
        "e522e7e0462f0cf7d84a8c2fe265872ae49e9af4327ec077d6f92af33ad78f65"
    ):
        raise ValueError("Evaluation report changed.")

    decision = read_json(decision_path)
    report = read_json(report_path)
    experiment = read_json(experiment_path)

    for path, expected in [
        (benchmark_path, decision["benchmark_metadata_sha256"]),
        (experiment_path, decision["experiment_sha256"]),
        (folder / "pipeline.joblib", decision["pipeline_sha256"]),
        (transformer, decision["transformer_sha256"]),
    ]:
        if sha256(path) != expected:
            raise ValueError(f"Checksum mismatch: {path}")

    if report["provenance"]["decision_sha256"] != sha256(decision_path):
        raise ValueError("Report decision mismatch.")
    if report["provenance"]["pipeline_sha256"] != decision["pipeline_sha256"]:
        raise ValueError("Report model mismatch.")
    if report["threshold"] != decision["threshold"]:
        raise ValueError("Report threshold mismatch.")
    if report["scope"] != "previously_used_test":
        raise ValueError("Expected explicit reused-test scope.")
    if report["independent_new_holdout"] is not False:
        raise ValueError("Incorrect independence declaration.")
    if report["quality_gate"]["passed"] is not True:
        raise ValueError("Unexpected gate result.")
    if report["approved_for_release"] is not False:
        raise ValueError("Unexpected approval status.")

    artifacts = [
        (folder / "pipeline.joblib", "candidate"),
        (experiment_path, "candidate"),
        (transformer, "source"),
        (decision_path, "evaluation"),
        (report_path, "evaluation"),
        (directory / "comparison.csv", "evaluation"),
        (directory / "validation_analysis/slices.csv", "evaluation"),
        (directory / "validation_analysis/analysis_metadata.json", "provenance"),
        (benchmark_path, "provenance"),
    ]
    for path, _ in artifacts:
        if not path.is_file():
            raise FileNotFoundError(path)

    client = MlflowClient(tracking_uri=URI, registry_uri=URI)
    local_experiment = client.get_experiment_by_name("Default")
    if local_experiment is None:
        raise RuntimeError("Local Default experiment not found.")

    receipt = {
        "state": "CREATE_INTENT",
        "scope": "local_import_existing_hgb_results",
        "tracking_uri": URI,
        "decision_sha256": sha256(decision_path),
        "report_sha256": sha256(report_path),
        "script_sha256": sha256(Path(__file__)),
    }
    with receipt_path.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2)
        stream.write("\n")

    tags = {
        "maintai.candidate": "hgb_interactions",
        "maintai.execution_mode": "import_existing_results",
        "maintai.scope": "local_development",
        "maintai.evaluation_scope": "previously_used_test",
        "maintai.independent_new_holdout": "false",
        "maintai.reused_test_gate": "PASS",
        "maintai.approved_for_release": "false",
        "maintai.decision_sha256": sha256(decision_path),
    }

    run = client.create_run(
        local_experiment.experiment_id,
        run_name="hgb-interactions-candidate-v1",
        tags=tags,
    )
    run_id = run.info.run_id
    receipt.update({"run_id": run_id, "state": "RUN_CREATED"})
    save_receipt(receipt_path, receipt)

    params = {
        f"model.{key}": str(value)
        for key, value in experiment["model_params"].items()
    }
    params.update({
        "threshold": repr(decision["threshold"]),
        "model_version": decision["model_version"],
        "preprocessor_version": decision["preprocessor_version"],
        "pipeline_sha256": decision["pipeline_sha256"],
        "transformer_sha256": decision["transformer_sha256"],
        "source_sha256": decision["source_sha256"],
        "split_sha256": decision["split_sha256"],
    })

    point = experiment["validation_selected_operating_point"]
    metrics = {
        f"validation.{key}": float(point[key])
        for key in ["precision", "recall", "f1", "f2", "tp", "fp", "fn", "tn"]
    }
    metrics["validation.average_precision"] = experiment["average_precision"]
    metrics.update({
        f"reused_test.{key}": float(value)
        for key, value in report["metrics"].items()
    })

    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    client.log_batch(
        run_id,
        params=[Param(key, value) for key, value in params.items()],
        metrics=[
            Metric(key, value, timestamp, 0)
            for key, value in metrics.items()
        ],
        synchronous=True,
    )

    receipt["artifacts"] = {}
    for path, remote_folder in artifacts:
        client.log_artifact(run_id, str(path), artifact_path=remote_folder)
        receipt["artifacts"][f"{remote_folder}/{path.name}"] = sha256(path)

    receipt["state"] = "UPLOADED"
    save_receipt(receipt_path, receipt)

    loaded = client.get_run(run_id)
    for key, value in params.items():
        if loaded.data.params.get(key) != value:
            raise ValueError(f"Parameter mismatch: {key}")
    for key, value in metrics.items():
        if loaded.data.metrics.get(key) != value:
            raise ValueError(f"Metric mismatch: {key}")
    for key, value in tags.items():
        if loaded.data.tags.get(key) != value:
            raise ValueError(f"Tag mismatch: {key}")

    with tempfile.TemporaryDirectory() as temporary:
        for remote_path, expected in receipt["artifacts"].items():
            downloaded = client.download_artifacts(
                run_id, remote_path, temporary
            )
            if sha256(Path(downloaded)) != expected:
                raise ValueError(f"Artifact mismatch: {remote_path}")

    client.set_terminated(run_id, status="FINISHED")
    if client.get_run(run_id).info.status != "FINISHED":
        raise ValueError("Run did not finish.")

    receipt["state"] = "VERIFIED"
    save_receipt(receipt_path, receipt)

    print("Params / metrics / tags: PASS")
    print("Artifact download / SHA-256:", len(artifacts), "PASS")
    print("Run ID:", run_id)
    print("Reused-test point-estimate gate: PASS")
    print("Independent new holdout: NO")
    print("Approved for release: NO")
    print("Registry model created: NO")
    print("Receipt:", receipt_path)


if __name__ == "__main__":
    main()