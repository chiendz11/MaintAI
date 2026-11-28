"""Versioned S3 publication for drift-worker reports and M7 DriftManifest artifacts."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from botocore.exceptions import ClientError

from drift_monitor.config import DriftConfig
from drift_monitor.models import DriftReport


def _build_s3_client(config: DriftConfig):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=config.s3_endpoint,
        region_name=config.s3_region,
        config=Config(
            connect_timeout=2,
            read_timeout=5,
            retries={"total_max_attempts": 2, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )


def publish_drift_report(
    config: DriftConfig,
    report: DriftReport,
    *,
    s3_client=None,
    published_at: datetime | None = None,
) -> dict:
    """Upload report.json and its version-pinned DriftManifest to S3/MinIO."""
    if not config.s3_bucket:
        raise ValueError("s3_bucket is required to publish drift report")

    client = s3_client or _build_s3_client(config)
    pub_dt = (published_at or datetime.now(UTC)).astimezone(UTC)
    pub_iso = pub_dt.isoformat().replace("+00:00", "Z")

    report_payload = report.model_dump(mode="json")
    computed_str = str(report_payload.get("computed_at") or "")
    report_date = computed_str[:10] if len(computed_str) >= 10 else pub_iso[:10]
    report_bytes = json.dumps(
        report_payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    report_sha256 = hashlib.sha256(report_bytes).hexdigest()
    report_key = f"drift-reports/{report_date}/{report.report_id}.json"
    manifest_key = f"drift-report-manifests/published/{report_date}/{report.report_id}.json"

    if hasattr(client, "get_object"):
        try:
            existing_obj = client.get_object(Bucket=config.s3_bucket, Key=manifest_key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in {"NoSuchKey", "404", "NotFound"}:
                raise
        else:
            raw_body = existing_obj["Body"]
            raw_bytes = raw_body.read() if hasattr(raw_body, "read") else raw_body
            existing = json.loads(raw_bytes)
            if (
                existing.get("report_id") != report.report_id
                or existing.get("report_ref", {}).get("sha256") != report_sha256
            ):
                raise RuntimeError("Existing drift manifest digest mismatch")
            return {
                "manifest_key": manifest_key,
                "manifest": existing,
                "report_bytes": report_bytes,
                "already_published": True,
            }

    put_report = client.put_object(
        Bucket=config.s3_bucket,
        Key=report_key,
        Body=report_bytes,
        ContentType="application/json",
    )
    version_id = str(put_report.get("VersionId") or "").strip()
    if not version_id:
        raise RuntimeError(
            f"Bucket {config.s3_bucket} did not return VersionId (versioning required)"
        )

    manifest = {
        "schema_version": "1.0.0",
        "report_id": report.report_id,
        "computed_at": report_payload["computed_at"],
        "publication_at": pub_iso,
        "model_version": report.reference.model_version,
        "dataset_version": report.reference.dataset_version,
        "overall_status": report.overall_status.value,
        "report_ref": {
            "bucket": config.s3_bucket,
            "key": report_key,
            "version_id": version_id,
            "sha256": report_sha256,
            "size_bytes": len(report_bytes),
        },
    }
    manifest_bytes = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    client.put_object(
        Bucket=config.s3_bucket,
        Key=manifest_key,
        Body=manifest_bytes,
        ContentType="application/json",
    )
    return {
        "manifest_key": manifest_key,
        "manifest": manifest,
        "report_bytes": report_bytes,
        "already_published": False,
    }
