"""Normalize MinIO Webhook payloads and manual Argo Submit dataset-manifest inputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import unquote_plus

MANIFEST_PREFIX = "datasets/approved/"
MANIFEST_SUFFIX = ".manifest.json"
DEFAULT_APPROVED_BUCKET = "maintai-approved"


class NormalizeError(ValueError):
    """Raised when webhook payload or manual parameter cannot produce a valid trigger record."""


def _validate_record(
    bucket: str,
    key: str,
    version_id: str | None,
    *,
    trigger_source: str,
    expected_bucket: str,
) -> dict[str, str]:
    if bucket != expected_bucket:
        raise NormalizeError(f"Unsupported bucket '{bucket}'; expected '{expected_bucket}'")
    clean_key = unquote_plus(key).lstrip("/")
    if (
        not clean_key.startswith(MANIFEST_PREFIX)
        or not clean_key.endswith(MANIFEST_SUFFIX)
        or ".." in Path(clean_key).parts
    ):
        raise NormalizeError(
            f"Manifest key '{clean_key}' must match '{MANIFEST_PREFIX}*{MANIFEST_SUFFIX}'"
        )
    return {
        "slot": clean_key.replace("/", "_").replace(".", "_"),
        "bucket": bucket,
        "key": clean_key,
        "version_id": str(version_id or "").strip(),
        "trigger_source": trigger_source,
    }


def normalize_inputs(
    *,
    event_body: str = "",
    dataset_manifest: str = "",
    manifest_version_id: str = "",
    bucket: str = DEFAULT_APPROVED_BUCKET,
) -> list[dict[str, str]]:
    raw_event = (event_body or "").strip()
    if raw_event and raw_event != "{}":
        try:
            payload = json.loads(raw_event)
        except json.JSONDecodeError as exc:
            raise NormalizeError("Invalid JSON in --event-body") from exc
        if isinstance(payload, dict) and "body" in payload and isinstance(payload["body"], dict):
            payload = payload["body"]
        records = payload.get("Records") if isinstance(payload, dict) else None
        if isinstance(records, list) and records:
            normalized: list[dict[str, str]] = []
            seen: set[tuple[str, str, str]] = set()
            for entry in records:
                if not isinstance(entry, dict):
                    continue
                s3_info = entry.get("s3") or {}
                rec_bucket = str((s3_info.get("bucket") or {}).get("name") or "")
                obj_info = s3_info.get("object") or {}
                rec_key = str(obj_info.get("key") or "")
                rec_vid = str(obj_info.get("versionId") or "").strip()
                item = _validate_record(
                    rec_bucket,
                    rec_key,
                    rec_vid,
                    trigger_source="webhook",
                    expected_bucket=bucket,
                )
                dedupe_key = (item["bucket"], item["key"], item["version_id"])
                if dedupe_key not in seen:
                    seen.add(dedupe_key)
                    item["slot"] = f"ds-{len(normalized)}"
                    normalized.append(item)
            if not normalized:
                raise NormalizeError("Webhook event contained no valid Records")
            return normalized
        raise NormalizeError("Webhook --event-body is missing non-empty S3 Records")

    raw_manifest = (dataset_manifest or "").strip()
    if not raw_manifest:
        raise NormalizeError("Either non-empty --event-body or --dataset-manifest is required")

    target_bucket = bucket
    target_key = raw_manifest
    if raw_manifest.startswith("s3://"):
        without_scheme = raw_manifest[len("s3://") :]
        parts = without_scheme.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise NormalizeError(f"Invalid S3 URI: {raw_manifest}")
        target_bucket, target_key = parts[0], parts[1]

    item = _validate_record(
        target_bucket,
        target_key,
        manifest_version_id,
        trigger_source="manual",
        expected_bucket=bucket,
    )
    item["slot"] = "ds-0"
    return [item]


def register_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "normalize", help="Normalize webhook or manual submit parameters"
    )
    parser.add_argument("--event-body", default="", help="Webhook event JSON payload")
    parser.add_argument(
        "--dataset-manifest", default="", help="Manual submit manifest key or s3:// URI"
    )
    parser.add_argument("--manifest-version-id", default="", help="Optional manifest S3 VersionId")
    parser.add_argument("--bucket", default=DEFAULT_APPROVED_BUCKET)
    parser.add_argument("--output", required=True, type=Path)


def run_cli(args: argparse.Namespace) -> int:
    records = normalize_inputs(
        event_body=args.event_body,
        dataset_manifest=args.dataset_manifest,
        manifest_version_id=args.manifest_version_id,
        bucket=args.bucket,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
    print(f"Normalized {len(records)} dataset manifest record(s) -> {args.output}")
    return 0
