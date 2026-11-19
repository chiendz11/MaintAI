"""Deterministic GitOps Kustomization & HPA updaters for shadow rollout and promotion."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from maintai_automation.s3 import validate_sha256, validate_version_id

REQUIRED_FILES = ("pipeline.joblib", "metadata.json", "evaluation.json", "drift_reference.json")
PLACEHOLDER_VERSION_IDS = {"BOOTSTRAP_UPLOAD_REQUIRED", "UNCONFIGURED", "NOT_BUILT"}


class GitOpsContractError(ValueError):
    """Raised when GitOps Kustomize or artifact-lock invariants are violated."""


def validate_artifacts_list(
    artifacts: object,
    *,
    model_dir: str | None = None,
    allow_placeholder: bool = False,
) -> list[dict[str, str]]:
    if not isinstance(artifacts, list) or not artifacts:
        raise GitOpsContractError("artifacts.json must be a non-empty JSON array")
    seen_names: set[str] = set()
    normalized: list[dict[str, str]] = []
    for item in artifacts:
        if not isinstance(item, dict):
            raise GitOpsContractError("Each artifact entry must be a JSON object")
        bucket = item.get("bucket")
        key = item.get("key")
        version_id = item.get("version_id")
        path = item.get("path")
        sha256 = item.get("sha256")
        if not isinstance(bucket, str) or not bucket:
            raise GitOpsContractError("Artifact entry requires non-empty bucket")
        if not isinstance(key, str) or not key or ".." in Path(key).parts:
            raise GitOpsContractError(f"Invalid artifact key: {key!r}")
        validate_version_id(version_id, f"artifact({key}).version_id")
        if not allow_placeholder and version_id in PLACEHOLDER_VERSION_IDS:
            raise GitOpsContractError(
                f"Shadow artifact '{key}' has placeholder version_id '{version_id}'; "
                "run training workflow before promoting to Champion"
            )
        if (
            not isinstance(path, str)
            or not path.startswith("/local/models/")
            or ".." in Path(path).parts
        ):
            raise GitOpsContractError(f"Artifact path must stay inside /local/models/: {path!r}")
        if model_dir and not path.startswith(model_dir.rstrip("/") + "/"):
            raise GitOpsContractError(
                f"Artifact path '{path}' does not match MODEL_DIR '{model_dir}'"
            )
        validate_sha256(sha256 or "", f"artifact({key}).sha256")
        seen_names.add(Path(path).name)
        normalized.append(
            {
                "bucket": bucket,
                "key": key,
                "version_id": str(version_id),
                "path": path,
                "sha256": str(sha256),
            }
        )
    missing = [name for name in REQUIRED_FILES if name not in seen_names]
    if missing:
        raise GitOpsContractError(f"Artifact bundle missing required file(s): {missing}")
    return normalized


def _find_generator(kust: dict, name: str) -> dict:
    for entry in kust.get("configMapGenerator") or []:
        if entry.get("name") == name:
            return entry
    raise GitOpsContractError(f"Missing configMapGenerator '{name}' in kustomization.yaml")


def _literals_dict(entry: dict) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in entry.get("literals") or []:
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        result[k] = v
    return result


def _set_literal(entry: dict, key: str, value: str) -> None:
    literals = list(entry.get("literals") or [])
    replaced = False
    for idx, line in enumerate(literals):
        if line.startswith(f"{key}="):
            literals[idx] = f"{key}={value}"
            replaced = True
            break
    if not replaced:
        literals.append(f"{key}={value}")
    entry["literals"] = literals


def _ensure_resource(kust: dict, resource_name: str, *, before: str | None = None) -> None:
    resources = list(kust.get("resources") or [])
    if resource_name in resources:
        return
    if before and before in resources:
        resources.insert(resources.index(before), resource_name)
    else:
        resources.append(resource_name)
    kust["resources"] = resources


def _remove_resource(kust: dict, resource_name: str) -> None:
    resources = list(kust.get("resources") or [])
    kust["resources"] = [item for item in resources if item != resource_name]


def _dump_kustomization(kust: dict) -> str:
    return yaml.safe_dump(kust, sort_keys=False, width=120)


def ensure_shadow_hpa(hpa_text: str) -> tuple[str, bool]:
    docs = [d for d in yaml.safe_load_all(hpa_text) if isinstance(d, dict)]
    if any(d.get("metadata", {}).get("name") == "ml-service-shadow-hpa" for d in docs):
        return hpa_text, False
    docs.append(
        {
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {"name": "ml-service-shadow-hpa", "namespace": "maintai-local"},
            "spec": {
                "scaleTargetRef": {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "ml-service-shadow",
                },
                "minReplicas": 1,
                "maxReplicas": 2,
                "metrics": [
                    {
                        "type": "Resource",
                        "resource": {
                            "name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 80},
                        },
                    }
                ],
            },
        }
    )
    rendered = "\n---\n".join(yaml.safe_dump(d, sort_keys=False).rstrip() for d in docs) + "\n"
    return rendered, True


def update_shadow_kustomization(
    kustomization_text: str,
    *,
    artifacts: list[dict[str, str]],
    model_version: str,
    producer_git_sha: str,
) -> str:
    kust = yaml.safe_load(kustomization_text)
    if not isinstance(kust, dict):
        raise GitOpsContractError("Invalid kustomization.yaml content")
    model_dir = f"/local/models/{model_version}"
    validated = validate_artifacts_list(artifacts, model_dir=model_dir)

    shadow_art = _find_generator(kust, "ml-service-shadow-artifacts")
    _set_literal(
        shadow_art,
        "artifacts.json",
        json.dumps(validated, separators=(",", ":")),
    )

    shadow_cfg = _find_generator(kust, "ml-service-shadow-config")
    _set_literal(shadow_cfg, "MODEL_DIR", model_dir)
    _set_literal(shadow_cfg, "SERVICE_ROLE", "shadow")
    _set_literal(shadow_cfg, "S3_ENDPOINT_URL", "http://minio:9000")
    if producer_git_sha:
        _set_literal(shadow_cfg, "PRODUCER_GIT_SHA", producer_git_sha)

    proc_cfg = _find_generator(kust, "processing-worker-config")
    _set_literal(proc_cfg, "ML_BASE_URL", "http://ml-service-mirror:8001")

    _ensure_resource(kust, "ml-service-shadow.yaml", before="business-edge.yaml")
    _ensure_resource(kust, "ml-traffic-mirror.yaml", before="business-edge.yaml")
    return _dump_kustomization(kust)


def promote_shadow_to_champion(
    kustomization_text: str,
    hpa_text: str,
    *,
    decommission_shadow: bool = True,
) -> tuple[str, str, dict]:
    kust = yaml.safe_load(kustomization_text)
    if not isinstance(kust, dict):
        raise GitOpsContractError("Invalid kustomization.yaml content")

    shadow_art_entry = _find_generator(kust, "ml-service-shadow-artifacts")
    shadow_cfg_entry = _find_generator(kust, "ml-service-shadow-config")
    shadow_art_map = _literals_dict(shadow_art_entry)
    shadow_cfg_map = _literals_dict(shadow_cfg_entry)

    raw_artifacts = shadow_art_map.get("artifacts.json")
    if not raw_artifacts:
        raise GitOpsContractError("ml-service-shadow-artifacts has no artifacts.json")
    model_dir = shadow_cfg_map.get("MODEL_DIR", "")
    if not model_dir.startswith("/local/models/"):
        raise GitOpsContractError(f"Invalid shadow MODEL_DIR: {model_dir!r}")

    parsed_artifacts = json.loads(raw_artifacts)
    validated = validate_artifacts_list(
        parsed_artifacts, model_dir=model_dir, allow_placeholder=False
    )

    champion_art_entry = _find_generator(kust, "ml-service-artifacts")
    champion_cfg_entry = _find_generator(kust, "ml-service-config")
    _set_literal(
        champion_art_entry,
        "artifacts.json",
        json.dumps(validated, separators=(",", ":")),
    )
    _set_literal(champion_cfg_entry, "MODEL_DIR", model_dir)
    _set_literal(champion_cfg_entry, "SERVICE_ROLE", "champion")
    shadow_git_sha = shadow_cfg_map.get("PRODUCER_GIT_SHA")
    if shadow_git_sha:
        _set_literal(champion_cfg_entry, "PRODUCER_GIT_SHA", shadow_git_sha)

    proc_cfg = _find_generator(kust, "processing-worker-config")
    if decommission_shadow:
        _set_literal(proc_cfg, "ML_BASE_URL", "http://ml-service:8001")
        _remove_resource(kust, "ml-service-shadow.yaml")
        _remove_resource(kust, "ml-traffic-mirror.yaml")

    hpa_docs = [d for d in yaml.safe_load_all(hpa_text) if isinstance(d, dict)]
    if decommission_shadow:
        filtered = [
            d for d in hpa_docs if d.get("metadata", {}).get("name") != "ml-service-shadow-hpa"
        ]
        rendered_docs = [yaml.safe_dump(d, sort_keys=False).rstrip() for d in filtered]
        updated_hpa = "\n---\n".join(rendered_docs) + "\n"
    else:
        updated_hpa = hpa_text

    summary = {
        "promoted_model_dir": model_dir,
        "model_version": Path(model_dir).name,
        "producer_git_sha": shadow_git_sha or "",
        "decommission_shadow": decommission_shadow,
        "artifacts": validated,
    }
    return _dump_kustomization(kust), updated_hpa, summary
