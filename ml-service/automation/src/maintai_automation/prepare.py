"""Prepare verified approved manifest and dataset CSV in the workflow slot workspace."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from maintai_automation.s3 import (
    S3VerificationError,
    download_object_locking_version,
    download_verified,
    make_s3_client,
    sha256_bytes,
    validate_sha256,
    validate_version_id,
)

MODEL_PREFIX_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,40}$")


class ManifestValidationError(ValueError):
    """Raised when an approved dataset manifest violates contract invariants."""


def _validate_manifest(manifest: dict, *, expected_bucket: str) -> dict:
    if not isinstance(manifest, dict):
        raise ManifestValidationError("Approved dataset manifest must be a JSON object")
    if manifest.get("schema_version") != "1.0.0":
        raise ManifestValidationError("Manifest schema_version must be '1.0.0'")
    dataset_version = manifest.get("dataset_version")
    if not isinstance(dataset_version, str) or not dataset_version.strip():
        raise ManifestValidationError("Manifest requires non-empty dataset_version")
    dataset = manifest.get("dataset")
    if not isinstance(dataset, dict):
        raise ManifestValidationError("Manifest requires 'dataset' descriptor object")
    ds_bucket = dataset.get("bucket")
    ds_key = dataset.get("key")
    if ds_bucket != expected_bucket:
        raise ManifestValidationError(
            f"Manifest dataset bucket must be '{expected_bucket}', got '{ds_bucket}'"
        )
    if not isinstance(ds_key, str) or not ds_key or ".." in Path(ds_key).parts:
        raise ManifestValidationError(f"Invalid dataset key in manifest: {ds_key!r}")
    validate_version_id(dataset.get("version_id"), "dataset.version_id")
    validate_sha256(dataset.get("sha256", ""), "dataset.sha256")
    prefix = str(manifest.get("model_version_prefix") or "hgb-interactions").strip()
    if not MODEL_PREFIX_RE.fullmatch(prefix):
        raise ManifestValidationError(f"Invalid model_version_prefix: {prefix!r}")
    return manifest


def prepare_slot(
    *,
    record: dict,
    directory: Path,
    release_context_path: Path | None = None,
    s3_client=None,
) -> dict:
    release_ctx = {}
    if release_context_path and release_context_path.is_file():
        release_ctx = json.loads(release_context_path.read_text(encoding="utf-8"))

    approved_bucket = str(
        release_ctx.get("approved_bucket") or record.get("bucket") or "maintai-approved"
    )
    bucket = str(record["bucket"])
    key = str(record["key"])
    expected_vid = str(record.get("version_id") or "").strip() or None

    directory.mkdir(parents=True, exist_ok=True)
    client = s3_client or make_s3_client()

    manifest_dest = directory / "dataset.manifest.json"
    manifest_info = download_object_locking_version(
        client,
        bucket,
        key,
        manifest_dest,
        expected_version_id=expected_vid,
    )
    try:
        manifest = json.loads(manifest_dest.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestValidationError("Downloaded manifest is not valid JSON") from exc

    _validate_manifest(manifest, expected_bucket=approved_bucket)
    dataset_spec = manifest["dataset"]
    dataset_dest = directory / "dataset.csv"
    try:
        dataset_info = download_verified(
            client,
            dataset_spec["bucket"],
            dataset_spec["key"],
            dataset_dest,
            expected_version_id=dataset_spec["version_id"],
            expected_sha256=dataset_spec["sha256"],
        )
    except S3VerificationError:
        raise

    prefix = str(manifest.get("model_version_prefix") or "hgb-interactions").strip()
    seed = int(manifest.get("split_seed", 42))
    lineage_digest = sha256_bytes(
        f"{manifest_info['sha256']}:{dataset_info['version_id']}:{seed}".encode()
    )
    model_version = f"{prefix}-{lineage_digest[:8]}"

    context = {
        "schema_version": "1.0.0",
        "trigger_source": record.get("trigger_source", "manual"),
        "manifest_bucket": bucket,
        "manifest_key": key,
        "manifest_version_id": manifest_info["version_id"],
        "manifest_sha256": manifest_info["sha256"],
        "dataset_bucket": dataset_info["bucket"],
        "dataset_key": dataset_info["key"],
        "dataset_version": manifest["dataset_version"],
        "dataset_version_id": dataset_info["version_id"],
        "dataset_sha256": dataset_info["sha256"],
        "dataset_path": str(dataset_dest),
        "split_seed": seed,
        "model_family": str(manifest.get("model_family") or "hgb_interactions"),
        "model_version": model_version,
        "output_dir": str(directory / "bundle"),
    }
    (directory / "context.json").write_text(json.dumps(context, indent=2) + "\n", encoding="utf-8")
    (directory / "train-required.txt").write_text("true\n", encoding="utf-8")
    (directory / "publish-required.txt").write_text("true\n", encoding="utf-8")
    return context


def register_parser(subparsers) -> None:
    parser = subparsers.add_parser("prepare", help="Download & verify manifest and dataset CSV")
    parser.add_argument("--record", required=True, help="JSON record produced by normalize")
    parser.add_argument("--directory", required=True, type=Path, help="Slot workspace directory")
    parser.add_argument("--context", type=Path, default=None, help="Release context JSON path")


def run_cli(args: argparse.Namespace) -> int:
    record = json.loads(args.record)
    context = prepare_slot(
        record=record,
        directory=args.directory,
        release_context_path=args.context,
    )
    print(
        f"Prepared {context['dataset_version']}@{context['dataset_version_id']} "
        f"-> model_version={context['model_version']}"
    )
    return 0
