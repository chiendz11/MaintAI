import hashlib
import io
import json
from datetime import UTC, datetime
from pathlib import Path

import jsonschema
import pytest
from botocore.exceptions import ClientError

from drift_monitor.models import DriftStatus
from drift_monitor.report import evaluate
from drift_monitor.s3_publisher import publish_drift_report
from drift_monitor.worker import run_worker
from tests.unit.test_worker_resilience import MemoryRepository


class FakeS3Client:
    def __init__(self, *, return_version=True):
        self.objects = {}
        self.put_calls = 0
        self.return_version = return_version

    def get_object(self, *, Bucket, Key):
        item = self.objects.get((Bucket, Key))
        if item is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
                "GetObject",
            )
        return {"Body": io.BytesIO(item["Body"]), "VersionId": item["VersionId"]}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        self.put_calls += 1
        vid = f"v-{len(self.objects) + 1}" if self.return_version else ""
        self.objects[(Bucket, Key)] = {
            "Body": Body,
            "ContentType": ContentType,
            "VersionId": vid,
        }
        return {"VersionId": vid} if self.return_version else {}


def test_publish_drift_report_matches_contracts(config, samples):
    report = evaluate(config, samples, computed_at=datetime(2026, 11, 27, 1, 0, tzinfo=UTC))
    config.s3_bucket = "maintai-drift"
    s3 = FakeS3Client()
    pub = publish_drift_report(
        config,
        report,
        s3_client=s3,
        published_at=datetime(2026, 11, 27, 2, 15, tzinfo=UTC),
    )

    manifest_key = pub["manifest_key"]
    assert manifest_key == f"drift-report-manifests/published/2026-11-27/{report.report_id}.json"
    manifest = pub["manifest"]
    assert manifest["schema_version"] == "1.0.0"
    assert manifest["report_id"] == report.report_id
    assert manifest["overall_status"] == DriftStatus.STABLE.value
    ref = manifest["report_ref"]
    assert ref["bucket"] == "maintai-drift"
    assert ref["key"] == f"drift-reports/2026-11-27/{report.report_id}.json"
    assert ref["version_id"] == "v-1"
    assert ref["size_bytes"] == len(pub["report_bytes"])
    assert ref["sha256"] == hashlib.sha256(pub["report_bytes"]).hexdigest()

    root = Path(__file__).resolve().parents[3]
    runtime_schema = json.loads((root / "contracts/v1.3/schemas/runtime.schema.json").read_text())
    report_schema = json.loads(
        (root / "contracts/v1.3/schemas/drift-report.schema.json").read_text()
    )
    jsonschema.validate(
        manifest,
        {"$ref": "#/$defs/DriftManifest", "$defs": runtime_schema["$defs"]},
    )
    jsonschema.validate(json.loads(pub["report_bytes"]), report_schema)


def test_publish_requires_version_id(config, samples):
    report = evaluate(config, samples)
    config.s3_bucket = "maintai-drift"
    with pytest.raises(RuntimeError, match="VersionId"):
        publish_drift_report(config, report, s3_client=FakeS3Client(return_version=False))


def test_required_s3_failure_prevents_checkpoint(config, samples, monkeypatch):
    config.s3_bucket = "maintai-drift"
    config.s3_required = True
    repo = MemoryRepository(samples)

    def boom(*args, **kwargs):
        raise OSError("S3 unreachable")

    monkeypatch.setattr("drift_monitor.worker.publish_drift_report", boom)
    res = run_worker(config, repo)
    assert res.exit_code == 1
    assert res.execution_error == "S3_PUBLISH_UNAVAILABLE"
    assert repo.current_checkpoint is None


def test_publish_is_idempotent_when_manifest_already_exists(config, samples):
    report = evaluate(config, samples, computed_at=datetime(2026, 11, 27, 1, 0, tzinfo=UTC))
    config.s3_bucket = "maintai-drift"
    s3 = FakeS3Client()

    first = publish_drift_report(
        config,
        report,
        s3_client=s3,
        published_at=datetime(2026, 11, 27, 2, 15, tzinfo=UTC),
    )
    assert first["already_published"] is False
    assert s3.put_calls == 2

    second = publish_drift_report(
        config,
        report,
        s3_client=s3,
        published_at=datetime(2026, 11, 28, 3, 30, tzinfo=UTC),
    )
    assert second["already_published"] is True
    assert s3.put_calls == 2
    assert second["manifest"] == first["manifest"]


def test_publish_rejects_conflicting_existing_manifest(config, samples):
    report = evaluate(config, samples, computed_at=datetime(2026, 11, 27, 1, 0, tzinfo=UTC))
    config.s3_bucket = "maintai-drift"
    s3 = FakeS3Client()
    first = publish_drift_report(
        config,
        report,
        s3_client=s3,
        published_at=datetime(2026, 11, 27, 2, 15, tzinfo=UTC),
    )
    bad = dict(first["manifest"])
    bad["report_ref"] = {**bad["report_ref"], "sha256": "0" * 64}
    s3.objects[("maintai-drift", first["manifest_key"])]["Body"] = json.dumps(bad).encode("utf-8")

    with pytest.raises(RuntimeError, match="digest mismatch"):
        publish_drift_report(
            config,
            report,
            s3_client=s3,
            published_at=datetime(2026, 11, 27, 4, 0, tzinfo=UTC),
        )
