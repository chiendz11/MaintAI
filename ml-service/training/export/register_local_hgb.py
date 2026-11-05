"""Đăng ký HGB vào Registry local; không tự phê duyệt release."""

import argparse
import json
import os
import tempfile
import time
from pathlib import Path

URI = "http://127.0.0.1:5000"
os.environ["MLFLOW_TRACKING_URI"] = URI
os.environ["MLFLOW_REGISTRY_URI"] = URI
os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "30"
os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"

import mlflow
from mlflow.artifacts import download_artifacts
from mlflow.tracking import MlflowClient

# Tái sử dụng hàm kiểm tra package và metadata Registry đã sửa ở bước RF.
# Import không chạy main() của script RF.
from register_local_rf import (
    read_json,
    save_receipt,
    sha256,
    verify_package,
)

mlflow.set_tracking_uri(URI)
mlflow.set_registry_uri(URI)

EXPECTED_MANIFEST_HASH = (
    "690571f408ec556d3cac469231a342f76"
    "cd1ee7f5c952498bc7429f39f40dc7c"
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-dir", required=True, type=Path)
    args = parser.parse_args()
    directory = args.benchmark_dir.resolve()

    receipt_path = directory / "registry_hgb_receipt.json"
    if receipt_path.exists():
        raise FileExistsError("Receipt exists; inspect before retrying.")

    tracking = read_json(directory / "mlflow_hgb_tracking_receipt.json")
    manifest_path = directory / "registry-model-manifest.json"
    manifest = read_json(manifest_path)
    package = directory / "registry-model"

    if tracking["state"] != "VERIFIED":
        raise ValueError("Tracking not verified.")
    if tracking["tracking_uri"] != URI:
        raise ValueError("Unexpected server.")
    if sha256(manifest_path) != EXPECTED_MANIFEST_HASH:
        raise ValueError("Package manifest changed.")
    if manifest["source_run_id"] != tracking["run_id"]:
        raise ValueError("Source run mismatch.")
    if manifest["decision_sha256"] != tracking["decision_sha256"]:
        raise ValueError("Decision mismatch.")
    if manifest["report_sha256"] != tracking["report_sha256"]:
        raise ValueError("Evaluation report mismatch.")

    verify_package(package, manifest["files"])

    client = MlflowClient(tracking_uri=URI, registry_uri=URI)
    run_id = tracking["run_id"]
    run = client.get_run(run_id)

    required_run_tags = {
        "maintai.candidate": "hgb_interactions",
        "maintai.reused_test_gate": "PASS",
        "maintai.independent_new_holdout": "false",
        "maintai.approved_for_release": "false",
    }
    if run.info.status != "FINISHED":
        raise ValueError("Source run is not FINISHED.")
    for key, expected in required_run_tags.items():
        if run.data.tags.get(key) != expected:
            raise ValueError(f"Source run tag mismatch: {key}")

    if any(item.path == "model" for item in client.list_artifacts(run_id)):
        raise RuntimeError("Run already has model/. Inspect before uploading.")

    name = f"maintai-local-hgb-candidate-{run_id[:8]}"
    source_uri = f"runs:/{run_id}/model"

    receipt = {
        "state": "UPLOAD_INTENT",
        "scope": "local_registry_candidate",
        "tracking_uri": URI,
        "registered_model_name": name,
        "run_id": run_id,
        "source_uri": source_uri,
        "manifest_sha256": sha256(manifest_path),
        "decision_sha256": manifest["decision_sha256"],
        "report_sha256": manifest["report_sha256"],
        "approved_for_release": False,
        "script_sha256": sha256(Path(__file__)),
        "verification_helper_sha256": sha256(
            Path(__file__).with_name("register_local_rf.py")
        ),
    }
    with receipt_path.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2)
        stream.write("\n")

    client.log_artifacts(run_id, str(package), artifact_path="model")
    client.log_artifact(
        run_id, str(manifest_path), artifact_path="provenance"
    )

    with tempfile.TemporaryDirectory() as temporary:
        downloaded = client.download_artifacts(run_id, "model", temporary)
        verify_package(Path(downloaded), manifest["files"])

    tags = {
        "maintai.scope": "local_development",
        "maintai.reused_test_gate": "PASS",
        "maintai.independent_new_holdout": "false",
        "maintai.approved_for_release": "false",
        "maintai.package_manifest_sha256": sha256(manifest_path),
        "maintai.decision_sha256": manifest["decision_sha256"],
        "maintai.report_sha256": manifest["report_sha256"],
    }

    receipt["state"] = "CREATE_REGISTERED_MODEL_INTENT"
    save_receipt(receipt_path, receipt)

    client.create_registered_model(
        name=name,
        tags=tags,
        description=(
            "HGB interaction candidate for local integration. "
            "Passes point-estimate gates on reused test; "
            "not an independent new holdout and not release-approved."
        ),
    )

    receipt["state"] = "CREATE_VERSION_INTENT"
    save_receipt(receipt_path, receipt)

    created = client.create_model_version(
        name=name,
        source=source_uri,
        run_id=run_id,
        tags=tags,
        await_creation_for=0,
    )

    version = str(created.version)
    receipt.update({"version": version, "state": "VERSION_CREATED"})
    save_receipt(receipt_path, receipt)

    deadline = time.monotonic() + 45
    while True:
        registered = client.get_model_version(name, version)
        if registered.status == "READY":
            break
        if registered.status == "FAILED_REGISTRATION":
            raise RuntimeError("Registration failed.")
        if time.monotonic() >= deadline:
            raise TimeoutError("Version not READY within 45 seconds.")
        time.sleep(2)

    if registered.run_id != run_id:
        raise ValueError("Registry run identity mismatch.")
    for key, expected in tags.items():
        if registered.tags.get(key) != expected:
            raise ValueError(f"Registry tag mismatch: {key}")

    model_uri = f"models:/{name}/{version}"
    receipt.update({
        "model_uri": model_uri,
        "resolved_source": registered.source,
        "state": "VERIFY_REGISTRY_DOWNLOAD",
    })
    save_receipt(receipt_path, receipt)

    with tempfile.TemporaryDirectory() as temporary:
        downloaded = Path(download_artifacts(
            artifact_uri=model_uri,
            dst_path=temporary,
            tracking_uri=URI,
            registry_uri=URI,
        ))

        verify_package(
            downloaded,
            manifest["files"],
            registry_name=name,
            registry_version=version,
        )
        receipt["registered_model_meta_sha256"] = sha256(
            downloaded / "registered_model_meta"
        )

    receipt["state"] = "VERIFIED"
    save_receipt(receipt_path, receipt)

    print("Run upload / original package hashes: PASS")
    print("Registry run / tags: PASS")
    print("Registry download / original package hashes: PASS")
    print("Registry-generated metadata identity: PASS")
    print("Status:", registered.status)
    print("Version:", version)
    print("Model URI:", model_uri)
    print("Independent new holdout: NO")
    print("Approved for release: NO")
    print("Receipt:", receipt_path)


if __name__ == "__main__":
    main()