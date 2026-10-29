"""Reconstruct an explicitly local identity from measured delivery inputs.

No MLflow run or approved production dataset manifest is invented. The caller
must obtain image_reference from the serving image's actual OCI RepoDigest.
"""

import argparse
import hashlib
import json
import re
from dataclasses import asdict
from pathlib import Path

from maintai_messaging.contracts import canonical_json, strict_json
from maintai_messaging.outcomes import ModelIdentity

LOCAL_RUN_MARKER = "not-recorded:local-demo"
FEATURES = (
    "product_type",
    "air_temperature_k",
    "process_temperature_k",
    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min",
)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def create_local_identity(*, bundle, dataset, split, feature_schema, image_reference, source_sha):
    """Return identity and audit evidence, never a production approval."""
    from processing.shared.inference_client.model_identity import validate_model_identity

    if not re.fullmatch(r"maintai-local/ml-service@sha256:[0-9a-f]{64}", image_reference):
        raise ValueError("A verified local serving OCI manifest digest is required")
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", source_sha):
        raise ValueError("Actual application Git revision is required")
    bundle = Path(bundle)
    metadata = strict_json((bundle / "metadata.json").read_bytes())
    if metadata.get("status") != "local_baseline_not_approved_release":
        raise ValueError("Only the explicitly local baseline delivery is supported")
    if metadata.get("features") != list(FEATURES):
        raise ValueError("Delivered feature order differs from the local protocol")
    if sha256(bundle / "pipeline.joblib") != metadata["pipeline_sha256"]:
        raise ValueError("Delivered pipeline checksum differs")
    if sha256(dataset) != metadata["source_sha256"] or sha256(split) != metadata["split_sha256"]:
        raise ValueError("Delivered dataset/split checksum differs")
    schema = strict_json(Path(feature_schema).read_bytes())
    if set(schema["properties"]) != set(FEATURES):
        raise ValueError("Feature schema differs from the delivery")
    dataset_manifest = {
        "schema_version": "1.0.0",
        "dataset_version": "ai4i2020-local-replay-v1",
        "origin": "local-reconstruction-from-delivered-metadata",
        "approved_training_manifest": False,
        "source_sha256": sha256(dataset),
        "split_sha256": sha256(split),
        "metadata_sha256": sha256(bundle / "metadata.json"),
    }
    manifest_hash = hashlib.sha256(canonical_json(dataset_manifest)).hexdigest()
    release_hash = hashlib.sha256(
        canonical_json(
            {
                "dataset_manifest_sha256": manifest_hash,
                "image": image_reference,
                "source_sha": source_sha,
                "pipeline_sha256": metadata["pipeline_sha256"],
                "feature_schema_sha256": sha256(feature_schema),
            }
        )
    ).hexdigest()
    identity = ModelIdentity(
        model_version=metadata["model_version"],
        preprocessor_version=metadata["preprocessor_version"],
        dataset_version=dataset_manifest["dataset_version"],
        dataset_manifest_sha256=manifest_hash,
        split_sha256=sha256(split),
        mlflow_run_id=LOCAL_RUN_MARKER,
        model_artifact_sha256=metadata["pipeline_sha256"],
        # The fitted preprocessor is inside the same sklearn Pipeline artifact.
        preprocessor_sha256=metadata["pipeline_sha256"],
        feature_schema_version="sensor-features-1.3.0",
        feature_schema_sha256=sha256(feature_schema),
        release_id="local-demo-" + release_hash[:24],
        inference_image_digest=image_reference,
    )
    validate_model_identity(identity)
    evidence = {
        "schema_version": "1.0.0",
        "profile": "local-demo-only",
        "mlflow_recorded": False,
        "production_approved": False,
        "model_quality_approved": False,
        "source_git_sha": source_sha,
        "dataset_manifest": dataset_manifest,
        "preprocessor_artifact": "pipeline.joblib (combined fitted pipeline)",
        "bundle_sha256": {
            name: sha256(bundle / name)
            for name in (
                "pipeline.joblib",
                "metadata.json",
                "evaluation.json",
                "drift_reference.json",
            )
        },
        "inference_image_digest": image_reference,
    }
    return identity, evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("bundle", "dataset", "split", "feature-schema", "image-reference", "source-sha"):
        parser.add_argument("--" + name, required=True)
    identity, evidence = create_local_identity(**vars(parser.parse_args()))
    print(json.dumps({"identity": asdict(identity), "evidence": evidence}))


if __name__ == "__main__":
    main()
