"""Verify DriftCatalog reads and validates manifests/reports published by drift-worker."""

import io
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DRIFT_SRC = ROOT / "drift-worker" / "src"
if str(DRIFT_SRC) not in sys.path:
    sys.path.insert(0, str(DRIFT_SRC))


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


def test_drift_worker_s3_output_validates_in_business_drift_catalog(tmp_path):
    from drift_monitor.config import DriftConfig
    from drift_monitor.models import Features, RuntimeSample
    from drift_monitor.reference import build_reference
    from drift_monitor.report import evaluate
    from drift_monitor.s3_publisher import publish_drift_report

    from backend.shared.monitoring.catalog import DriftCatalog
    from backend.shared.monitoring.s3 import S3Reports

    samples = [
        RuntimeSample(
            event_id=f"evt_rt_{i:04d}",
            received_at=datetime(2026, 11, 27, tzinfo=UTC) + timedelta(seconds=i),
            features=Features(
                product_type=["L", "L", "M", "H"][i % 4],
                air_temperature_k=298.0 + (i % 5),
                process_temperature_k=308.0 + (i % 5),
                rotational_speed_rpm=1450.0 + (i % 4) * 40,
                torque_nm=35.0 + (i % 4) * 5,
                tool_wear_min=float((i % 5) * 15),
            ),
        )
        for i in range(250)
    ]
    ref = build_reference(
        [s.features for s in samples],
        model_version="logreg-baseline-v0",
        dataset_version="ai4i2020-train-split-v1-seed42",
        reference_source="test_fixture",
    )
    ref_path = tmp_path / "drift_reference.json"
    ref_path.write_text(ref.model_dump_json(), encoding="utf-8")

    cfg = DriftConfig(
        reference_path=ref_path,
        model_version="logreg-baseline-v0",
        dataset_version="ai4i2020-train-split-v1-seed42",
        s3_bucket="maintai-drift",
        s3_region="us-east-1",
    )
    report = evaluate(cfg, samples, computed_at=datetime(2026, 11, 27, 1, 0, tzinfo=UTC))
    fake_s3 = _FakeBotoS3()
    pub = publish_drift_report(
        cfg,
        report,
        s3_client=fake_s3,
        published_at=datetime(2026, 11, 27, 2, 0, tzinfo=UTC),
    )

    store = S3Reports(fake_s3, "maintai-drift")
    entries, next_token = store.page("drift-report-manifests/published/2026-11-27/")
    assert next_token is None
    assert len(entries) == 1
    key, manifest = entries[0]
    assert key == pub["manifest_key"]
    assert manifest["report_id"] == report.report_id

    catalog = DriftCatalog(lambda: None, store, b"k" * 32, model_version="logreg-baseline-v0")
    loaded = catalog._report(manifest)
    assert loaded["report_id"] == report.report_id
    assert loaded["reference"]["model_version"] == "logreg-baseline-v0"
