"""Local provenance preserves measured hashes without asserting MLflow approval."""

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pytest
from processing.shared.inference_client.local_identity import FEATURES, create_local_identity

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def inputs(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    dataset, split = tmp_path / "data.csv", tmp_path / "split.json"
    dataset.write_bytes(b"fixture dataset")
    split.write_bytes(b"fixture split")
    (bundle / "pipeline.joblib").write_bytes(b"fixture fitted combined pipeline")

    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    (bundle / "metadata.json").write_text(
        json.dumps(
            {
                "status": "local_baseline_not_approved_release",
                "features": list(FEATURES),
                "model_version": "test-local-model",
                "preprocessor_version": "test-local-preprocessor",
                "source_sha256": digest(dataset),
                "split_sha256": digest(split),
                "pipeline_sha256": digest(bundle / "pipeline.joblib"),
            }
        )
    )
    for name in ("evaluation.json", "drift_reference.json"):
        (bundle / name).write_text("{}")
    return dict(
        bundle=bundle,
        dataset=dataset,
        split=split,
        feature_schema=ROOT / "contracts/v1.3/schemas/features.schema.json",
        image_reference="maintai-local/ml-service@sha256:" + "a" * 64,
        source_sha="b" * 40,
    )


def test_local_identity_is_deterministic_and_does_not_fabricate_mlflow(inputs):
    identity, evidence = create_local_identity(**inputs)
    assert create_local_identity(**inputs)[0] == identity
    assert identity.mlflow_run_id == "not-recorded:local-demo"
    assert identity.model_artifact_sha256 == identity.preprocessor_sha256
    assert identity.inference_image_digest == inputs["image_reference"]
    assert not evidence["mlflow_recorded"] and not evidence["production_approved"]
    assert not evidence["dataset_manifest"]["approved_training_manifest"]
    assert all(asdict(identity).values())


@pytest.mark.parametrize("name", ["dataset", "split", "pipeline"])
def test_tampered_deliveries_fail_before_identity_creation(inputs, name):
    path = inputs["bundle"] / "pipeline.joblib" if name == "pipeline" else inputs[name]
    path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        create_local_identity(**inputs)


@pytest.mark.parametrize(
    "image", ["latest", "maintai-local/ml-service:dev", "production/ml@sha256:" + "a" * 64]
)
def test_unverified_or_nonlocal_image_references_are_rejected(inputs, image):
    inputs["image_reference"] = image
    with pytest.raises(ValueError, match="OCI"):
        create_local_identity(**inputs)


def test_image_change_changes_release_identity(inputs):
    first, _ = create_local_identity(**inputs)
    inputs["image_reference"] = "maintai-local/ml-service@sha256:" + "c" * 64
    second, _ = create_local_identity(**inputs)
    assert first.release_id != second.release_id
