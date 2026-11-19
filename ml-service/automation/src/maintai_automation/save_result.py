"""Offline Quality Gate, MinIO versioned upload, and MLflow @shadow registration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from maintai_automation.mlflow_client import MlflowRestClient
from maintai_automation.s3 import (
    make_s3_client,
    sha256_file,
    upload_file_versioned,
)

REQUIRED_BUNDLE_FILES = (
    "pipeline.joblib",
    "metadata.json",
    "evaluation.json",
    "drift_reference.json",
)


class QualityGateError(RuntimeError):
    """Raised when candidate offline metrics violate the configured quality gate."""


def save_and_register_result(
    *,
    directory: Path,
    release_context_path: Path | None = None,
    min_recall: float = 0.85,
    min_precision: float = 0.50,
    models_bucket: str | None = None,
    model_name: str | None = None,
    experiment_name: str | None = None,
    s3_client=None,
    mlflow_client: MlflowRestClient | None = None,
) -> dict:
    release_ctx = {}
    if release_context_path and release_context_path.is_file():
        release_ctx = json.loads(release_context_path.read_text(encoding="utf-8"))

    target_bucket = models_bucket or str(release_ctx.get("models_bucket") or "maintai-models")
    target_model_name = model_name or str(
        release_ctx.get("mlflow_model_name") or "maintai-classifier"
    )
    target_experiment = experiment_name or str(
        release_ctx.get("mlflow_experiment") or "maintai-local-training"
    )

    result_path = directory / "model-result.json"
    if not result_path.is_file():
        raise FileNotFoundError(f"Missing model-result.json in {directory}")
    model_result = json.loads(result_path.read_text(encoding="utf-8"))

    bundle_dir = Path(model_result["bundle_dir"])
    recorded_files = model_result.get("files") or {}
    for filename in REQUIRED_BUNDLE_FILES:
        file_path = bundle_dir / filename
        if not file_path.is_file():
            raise FileNotFoundError(f"Missing trained bundle artifact: {file_path}")
        actual_sha = sha256_file(file_path)
        if filename in recorded_files and actual_sha != recorded_files[filename]:
            raise ValueError(
                f"Checksum mismatch for {filename}: "
                f"expected {recorded_files[filename]}, got {actual_sha}"
            )

    val_metrics = model_result.get("validation_metrics") or {}
    recall = float(val_metrics["recall_failure"])
    precision = float(val_metrics["precision_failure"])
    if recall < min_recall or precision < min_precision:
        raise QualityGateError(
            f"Offline Quality Gate FAILED: recall_failure={recall:.4f} (min {min_recall:.4f}), "
            f"precision_failure={precision:.4f} (min {min_precision:.4f})"
        )

    s3 = s3_client or make_s3_client()
    model_version = str(model_result["model_version"])
    artifacts: list[dict[str, str]] = []
    for filename in REQUIRED_BUNDLE_FILES:
        src_path = bundle_dir / filename
        key = f"models/{model_version}/{filename}"
        uploaded = upload_file_versioned(s3, target_bucket, key, src_path)
        artifacts.append(
            {
                "bucket": uploaded["bucket"],
                "key": uploaded["key"],
                "version_id": uploaded["version_id"],
                "path": f"/local/models/{model_version}/{filename}",
                "sha256": uploaded["sha256"],
            }
        )

    mlflow = mlflow_client or MlflowRestClient()
    test_metrics = model_result.get("test_metrics") or {}
    mlflow_metrics = {
        "val_recall_failure": recall,
        "val_precision_failure": precision,
        "val_f1": float(val_metrics.get("f1", 0.0)),
        "val_f2": float(val_metrics.get("f2", 0.0)),
        "val_average_precision": float(val_metrics.get("average_precision", 0.0)),
        "threshold": float(model_result.get("threshold", 0.5)),
    }
    if "recall_failure" in test_metrics:
        mlflow_metrics["test_recall_failure"] = float(test_metrics["recall_failure"])
    if "precision_failure" in test_metrics:
        mlflow_metrics["test_precision_failure"] = float(test_metrics["precision_failure"])

    params = {
        "model_version": model_version,
        "dataset_version": str(model_result["dataset_version"]),
        "preprocessor_version": str(model_result.get("preprocessor_version") or ""),
        "manifest_key": str(model_result.get("manifest_key") or ""),
        "manifest_version_id": str(model_result.get("manifest_version_id") or ""),
        "dataset_version_id": str(model_result.get("dataset_version_id") or ""),
        "source_git_sha": str(model_result.get("source_git_sha") or ""),
    }
    tags = {
        "maintai.model_version": model_version,
        "maintai.dataset_version": str(model_result["dataset_version"]),
        "maintai.pipeline_sha256": artifacts[0]["sha256"],
        "maintai.source_git_sha": str(model_result.get("source_git_sha") or ""),
        "maintai.role": "shadow",
    }
    mlflow_info = mlflow.register_shadow_version(
        experiment_name=target_experiment,
        model_name=target_model_name,
        model_version_label=model_version,
        artifact_source_uri=f"s3://{target_bucket}/models/{model_version}",
        metrics=mlflow_metrics,
        params=params,
        tags=tags,
    )

    published = {
        "schema_version": "1.0.0",
        "model_version": model_version,
        "dataset_version": str(model_result["dataset_version"]),
        "manifest_key": model_result.get("manifest_key"),
        "manifest_version_id": model_result.get("manifest_version_id"),
        "dataset_version_id": model_result.get("dataset_version_id"),
        "source_git_sha": str(model_result.get("source_git_sha") or ""),
        "threshold": float(model_result.get("threshold", 0.5)),
        "validation_metrics": val_metrics,
        "test_metrics": test_metrics,
        "artifacts": artifacts,
        "mlflow": mlflow_info,
    }
    (directory / "published-artifacts.json").write_text(
        json.dumps(published, indent=2) + "\n",
        encoding="utf-8",
    )
    return published


def register_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "save-result",
        help="Verify gate, upload artifacts to MinIO, and register @shadow in MLflow",
    )
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--context", type=Path, default=None)
    parser.add_argument("--min-recall", type=float, default=0.85)
    parser.add_argument("--min-precision", type=float, default=0.50)
    parser.add_argument("--models-bucket", default=None)
    parser.add_argument("--model-name", default=None)
    parser.add_argument("--experiment-name", default=None)


def run_cli(args: argparse.Namespace) -> int:
    published = save_and_register_result(
        directory=args.directory,
        release_context_path=args.context,
        min_recall=args.min_recall,
        min_precision=args.min_precision,
        models_bucket=args.models_bucket,
        model_name=args.model_name,
        experiment_name=args.experiment_name,
    )
    print(
        f"Uploaded 4 artifacts for {published['model_version']} and registered MLflow "
        f"v{published['mlflow']['registered_model_version']} (@shadow)"
    )
    return 0
