"""Verify DriftCatalog reads and validates S3 drift manifests/reports."""

import hashlib
import io
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


class _FakeBotoS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        vid = f"ver-{len(self.objects) + 1}"
        self.objects[(Bucket, Key)] = (Body, vid, ContentType)
        return {"VersionId": vid}

    def list_objects_v2(self, *, Bucket, Prefix, MaxKeys=25, ContinuationToken=None):
        contents = [
            {"Key": key}
            for (b, key) in sorted(self.objects)
            if b == Bucket and key.startswith(Prefix)
        ][:MaxKeys]
        return {"Contents": contents, "IsTruncated": False}

    def get_object(self, *, Bucket, Key, VersionId=None):
        if (Bucket, Key) not in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
                "GetObject",
            )
        body, vid, _ = self.objects[(Bucket, Key)]
        if VersionId and VersionId != vid:
            raise KeyError("Version mismatch")
        return {
            "Body": io.BytesIO(body),
            "VersionId": vid,
            "ContentLength": len(body),
        }


def test_drift_worker_s3_output_validates_in_business_drift_catalog():
    from backend.shared.alerts.domain import AlertError
    from backend.shared.monitoring.catalog import DriftCatalog
    from backend.shared.monitoring.s3 import S3Reports

    report = json.loads(
        (ROOT / "contracts/v1.3/examples/http/drift/report-stable.json").read_text(encoding="utf-8")
    )
    report_id = "drift_roundtrip_20261127"
    computed_at = "2026-11-27T01:00:00Z"
    publication_at = "2026-11-27T02:00:00Z"
    report["report_id"] = report_id
    report["computed_at"] = computed_at
    report["reference"] = {
        "model_version": "logreg-baseline-v0",
        "dataset_version": "ai4i2020-train-split-v1-seed42",
    }

    report_bytes = json.dumps(
        report,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    report_sha256 = hashlib.sha256(report_bytes).hexdigest()
    report_key = f"drift-reports/2026-11-27/{report_id}.json"
    manifest_key = f"drift-report-manifests/published/2026-11-27/{report_id}.json"

    fake_s3 = _FakeBotoS3()
    put_report = fake_s3.put_object(
        Bucket="maintai-drift",
        Key=report_key,
        Body=report_bytes,
        ContentType="application/json",
    )
    manifest = {
        "schema_version": "1.0.0",
        "report_id": report_id,
        "computed_at": computed_at,
        "publication_at": publication_at,
        "model_version": report["reference"]["model_version"],
        "dataset_version": report["reference"]["dataset_version"],
        "overall_status": report["overall_status"],
        "report_ref": {
            "bucket": "maintai-drift",
            "key": report_key,
            "version_id": put_report["VersionId"],
            "sha256": report_sha256,
            "size_bytes": len(report_bytes),
        },
    }
    fake_s3.put_object(
        Bucket="maintai-drift",
        Key=manifest_key,
        Body=json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )

    store = S3Reports(fake_s3, "maintai-drift")
    entries, next_token = store.page("drift-report-manifests/published/2026-11-27/")
    assert next_token is None
    assert len(entries) == 1
    key, loaded_manifest = entries[0]
    assert key == manifest_key
    assert loaded_manifest["report_id"] == report_id

    catalog = DriftCatalog(lambda: None, store, b"k" * 32, model_version="logreg-baseline-v0")
    loaded = catalog._report(loaded_manifest)
    assert loaded["report_id"] == report_id
    assert loaded["reference"]["model_version"] == "logreg-baseline-v0"

    tampered_manifest = dict(loaded_manifest)
    tampered_manifest["report_ref"] = {
        **loaded_manifest["report_ref"],
        "sha256": "0" * 64,
    }
    with pytest.raises(AlertError, match="DRIFT_REPORT_UNAVAILABLE"):
        catalog._report(tampered_manifest)
