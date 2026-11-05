"""Đăng ký RF bị từ chối vào Registry local và kiểm tra artifact round-trip."""

import argparse
import hashlib
import json
import os
import tempfile
import time
import yaml
from pathlib import Path

os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "30"
os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"

from mlflow.artifacts import download_artifacts
from mlflow.tracking import MlflowClient


URI = "http://127.0.0.1:5000"
import mlflow

mlflow.set_tracking_uri(URI)
mlflow.set_registry_uri(URI)
EXPECTED_MANIFEST_SHA256 = (
    "3edbc59aa49975e3cf78519e38d0a293"
    "f24e6416ba247de4d897a75a884b2714"
)


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


def verify_package(
    directory,
    expected_files,
    *,
    registry_name=None,
    registry_version=None,
):
    actual_files = {
        path.relative_to(directory).as_posix(): sha256(path)
        for path in directory.rglob("*")
        if path.is_file()
    }

    if registry_name is not None:
        metadata_name = "registered_model_meta"
        metadata_path = directory / metadata_name

        if metadata_name not in actual_files:
            raise ValueError("Missing Registry download metadata.")

        metadata = yaml.safe_load(
            metadata_path.read_text(encoding="utf-8")
        )

        if not isinstance(metadata, dict):
            raise ValueError("Invalid Registry download metadata.")

        if set(metadata) != {"model_name", "model_version"}:
            raise ValueError("Unexpected Registry metadata fields.")

        if metadata["model_name"] != registry_name:
            raise ValueError("Registry metadata model name mismatch.")

        if str(metadata["model_version"]) != str(registry_version):
            raise ValueError("Registry metadata version mismatch.")

        # Chỉ tách file do Registry sinh sau khi đã kiểm tra nội dung.
        actual_files.pop(metadata_name)

    if actual_files != expected_files:
        raise ValueError("Package file list or SHA-256 mismatch.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    receipt_path = directory / "registry_rf_receipt.json"
    if receipt_path.exists():
        raise FileExistsError(
            "Registry receipt already exists. Inspect before retrying."
        )

    package = directory / "registry-model"
    manifest_path = directory / "registry-model-manifest.json"
    tracking = read_json(directory / "mlflow_rf_tracking_receipt.json")
    manifest = read_json(manifest_path)

    if tracking["state"] != "VERIFIED":
        raise ValueError("Tracking has not been verified.")
    if tracking["tracking_uri"] != URI:
        raise ValueError("Unexpected tracking server.")
    if sha256(manifest_path) != EXPECTED_MANIFEST_SHA256:
        raise ValueError("Package manifest changed.")
    if manifest["source_run_id"] != tracking["run_id"]:
        raise ValueError("Source run mismatch.")
    if manifest["selection_sha256"] != tracking["selection_sha256"]:
        raise ValueError("Selection identity mismatch.")

    verify_package(package, manifest["files"])

    client = MlflowClient(tracking_uri=URI, registry_uri=URI)
    run_id = tracking["run_id"]
    run = client.get_run(run_id)

    if run.info.status != "FINISHED":
        raise ValueError("Source run is not FINISHED.")
    if run.data.tags.get("maintai.test_gate") != "FAIL":
        raise ValueError("Unexpected quality gate tag.")
    if run.data.tags.get("maintai.approved_for_release") != "false":
        raise ValueError("Unexpected approval tag.")

    # Namespace local riêng, không dùng tên model production của nhóm.
    name = f"maintai-local-rf-rejected-{run_id[:8]}"
    source_uri = f"runs:/{run_id}/model"

    # Không ghi đè một package đã tồn tại trên run.
    existing = client.list_artifacts(run_id)
    if any(item.path == "model" for item in existing):
        raise RuntimeError(
            "Run already contains model/. Inspect before uploading."
        )

    receipt = {
        "state": "UPLOAD_INTENT",
        "scope": "local_registry_compatibility",
        "tracking_uri": URI,
        "registered_model_name": name,
        "run_id": run_id,
        "source_uri": source_uri,
        "manifest_sha256": sha256(manifest_path),
        "selection_sha256": manifest["selection_sha256"],
        "approved_for_release": False,
        "script_sha256": sha256(Path(__file__)),
    }
    with receipt_path.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2)
        stream.write("\n")

    # Upload chính package đã kiểm chứng; không serialize lại.
    client.log_artifacts(run_id, str(package), artifact_path="model")
    client.log_artifact(
        run_id, str(manifest_path), artifact_path="provenance"
    )

    with tempfile.TemporaryDirectory() as temporary:
        downloaded = client.download_artifacts(
            run_id, "model", temporary
        )
        verify_package(
            Path(downloaded),
            manifest["files"],
            registry_name=name,
            registry_version=model_version,
        )

    receipt["state"] = "CREATE_REGISTERED_MODEL_INTENT"
    save_receipt(receipt_path, receipt)

    tags = {
        "maintai.scope": "local_development",
        "maintai.test_gate": "FAIL",
        "maintai.approved_for_release": "false",
        "maintai.package_manifest_sha256": sha256(manifest_path),
        "maintai.selection_sha256": manifest["selection_sha256"],
    }

    # Tạo rõ ràng một tên mới; không nuốt lỗi bằng get-or-create.
    client.create_registered_model(
        name=name,
        tags=tags,
        description=(
            "Local Registry verification for rejected RF v1. "
            "Not approved for deployment."
        ),
    )

    receipt["state"] = "CREATE_VERSION_INTENT"
    save_receipt(receipt_path, receipt)

    created = client.create_model_version(
        name=name,
        source=source_uri,
        run_id=run_id,
        tags=tags,
        description="Held-out test gate FAIL; retain for traceability.",
        await_creation_for=0,
    )

    # Version do Registry cấp; không giả định luôn là version 1.
    model_version = str(created.version)
    receipt["version"] = model_version
    receipt["state"] = "VERSION_CREATED"
    save_receipt(receipt_path, receipt)

    deadline = time.monotonic() + 45
    while True:
        loaded = client.get_model_version(name, model_version)
        if loaded.status == "READY":
            break
        if loaded.status == "FAILED_REGISTRATION":
            raise RuntimeError("Registry reported FAILED_REGISTRATION.")
        if time.monotonic() >= deadline:
            raise TimeoutError("Version not READY within 45 seconds.")
        time.sleep(2)

    if loaded.run_id != run_id:
        raise ValueError("Registry run identity mismatch.")
    for key, value in tags.items():
        if loaded.tags.get(key) != value:
            raise ValueError(f"Registry tag mismatch: {key}")

    model_uri = f"models:/{name}/{model_version}"
    receipt["model_uri"] = model_uri
    receipt["resolved_source"] = loaded.source
    receipt["state"] = "VERIFY_REGISTRY_DOWNLOAD"
    save_receipt(receipt_path, receipt)

    # Tải qua numeric Registry version, không tải lại trực tiếp qua run.
    with tempfile.TemporaryDirectory() as temporary:
        downloaded = download_artifacts(
            artifact_uri=model_uri,
            dst_path=temporary,
            tracking_uri=URI,
            registry_uri=URI,
        )
        verify_package(Path(downloaded), manifest["files"])

    receipt["state"] = "VERIFIED"
    save_receipt(receipt_path, receipt)

    print("Run artifact upload / SHA-256: PASS")
    print("Registry version status:", loaded.status)
    print("Registry run / tags read-back: PASS")
    print("Registry download / all file hashes: PASS")
    print("Registered model:", name)
    print("Version:", model_version)
    print("Model URI:", model_uri)
    print("Approved for release: NO")
    print("Receipt:", receipt_path)


if __name__ == "__main__":
    main()