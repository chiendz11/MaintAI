"""Fail-closed initContainer fetcher staging MinIO artifacts into emptyDir with SHA-256."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from maintai_automation.s3 import (
    S3VerificationError,
    download_verified,
    make_s3_client,
    sha256_file,
    validate_sha256,
    validate_version_id,
)

ALLOWED_MOUNT_PREFIX = "/local/models/"


def fetch_artifacts_from_lock(config_path: Path, *, s3_client=None) -> list[dict[str, str]]:
    if not config_path.is_file():
        raise FileNotFoundError(f"Artifact lock file not found: {config_path}")
    items = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(items, list) or not items:
        raise S3VerificationError("artifacts.json must be a non-empty list")

    client = s3_client or make_s3_client()
    results: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            raise S3VerificationError("Invalid artifact descriptor in artifacts.json")
        bucket = item.get("bucket")
        key = item.get("key")
        version_id = item.get("version_id")
        dest_raw = item.get("path")
        expected_sha = item.get("sha256")
        if not isinstance(bucket, str) or not bucket:
            raise S3VerificationError("Artifact entry requires non-empty bucket")
        if not isinstance(key, str) or not key or ".." in Path(key).parts:
            raise S3VerificationError(f"Invalid artifact key: {key!r}")
        validate_version_id(version_id, f"artifact({key}).version_id")
        expected_hex = validate_sha256(expected_sha or "", f"artifact({key}).sha256")
        if (
            not isinstance(dest_raw, str)
            or not dest_raw.startswith(ALLOWED_MOUNT_PREFIX)
            or ".." in Path(dest_raw).parts
        ):
            raise S3VerificationError(
                f"Destination path must be inside {ALLOWED_MOUNT_PREFIX}: {dest_raw!r}"
            )

        dest = Path(dest_raw)
        verified = download_verified(
            client,
            bucket,
            key,
            dest,
            expected_version_id=str(version_id),
            expected_sha256=expected_hex,
        )
        post_sha = sha256_file(dest)
        if post_sha != expected_hex:
            dest.unlink(missing_ok=True)
            raise S3VerificationError(f"Post-write checksum mismatch on {dest}")
        results.append({**verified, "path": str(dest)})
    return results


def register_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "fetch-artifacts",
        help="Download MinIO artifacts from artifacts.json and verify VersionId & SHA-256",
    )
    parser.add_argument("--config", required=True, type=Path, help="Path to artifacts.json")


def run_cli(args: argparse.Namespace) -> int:
    fetched = fetch_artifacts_from_lock(args.config)
    print(f"Verified and staged {len(fetched)} model artifact(s) from MinIO.")
    return 0
