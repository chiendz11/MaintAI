import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from maintai_automation.fetch_artifacts import fetch_artifacts_from_lock
from maintai_automation.prepare import ManifestValidationError, prepare_slot
from maintai_automation.s3 import S3VerificationError


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FakeS3Client:
    def __init__(self, objects: dict[tuple[str, str, str | None], tuple[bytes, str]]):
        self.objects = objects
        self.calls: list[dict] = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        vid = kwargs.get("VersionId")
        entry = self.objects.get((bucket, key, vid))
        if entry is None:
            raise KeyError(f"Missing fake object: {(bucket, key, vid)}")
        payload, returned_vid = entry
        return {"Body": io.BytesIO(payload), "VersionId": returned_vid}


class PrepareAndFetchTest(unittest.TestCase):
    def test_prepare_verifies_version_id_and_sha256_and_writes_context(self):
        csv_bytes = b"Type,Air temperature [K]\nM,298.1\n"
        csv_sha = _sha256(csv_bytes)
        manifest = {
            "schema_version": "1.0.0",
            "dataset_version": "ai4i2020-approved-v1",
            "dataset": {
                "bucket": "maintai-approved",
                "key": "datasets/approved/ai4i2020.csv",
                "version_id": "ds-vid-1",
                "sha256": csv_sha,
            },
            "split_seed": 42,
            "model_family": "hgb_interactions",
            "model_version_prefix": "hgb-interactions",
        }
        manifest_bytes = json.dumps(manifest).encode("utf-8")
        s3 = FakeS3Client(
            {
                ("maintai-approved", "datasets/approved/ai4i-v1.manifest.json", None): (
                    manifest_bytes,
                    "man-vid-1",
                ),
                ("maintai-approved", "datasets/approved/ai4i2020.csv", "ds-vid-1"): (
                    csv_bytes,
                    "ds-vid-1",
                ),
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            slot = Path(tmp) / "ds-0"
            ctx = prepare_slot(
                record={
                    "bucket": "maintai-approved",
                    "key": "datasets/approved/ai4i-v1.manifest.json",
                    "version_id": "",
                    "trigger_source": "manual",
                },
                directory=slot,
                s3_client=s3,
            )
            self.assertEqual(ctx["manifest_version_id"], "man-vid-1")
            self.assertEqual(ctx["dataset_version_id"], "ds-vid-1")
            self.assertEqual(ctx["dataset_sha256"], csv_sha)
            self.assertTrue(ctx["model_version"].startswith("hgb-interactions-"))
            self.assertEqual((slot / "train-required.txt").read_text().strip(), "true")
            self.assertEqual((slot / "dataset.csv").read_bytes(), csv_bytes)

    def test_prepare_fails_closed_on_sha256_mismatch_or_missing_version_id(self):
        csv_bytes = b"corrupted"
        manifest = {
            "schema_version": "1.0.0",
            "dataset_version": "ai4i2020-approved-v1",
            "dataset": {
                "bucket": "maintai-approved",
                "key": "datasets/approved/ai4i2020.csv",
                "version_id": "ds-vid-1",
                "sha256": "0" * 64,
            },
        }
        s3 = FakeS3Client(
            {
                ("maintai-approved", "datasets/approved/ai4i-v1.manifest.json", "man-vid-1"): (
                    json.dumps(manifest).encode("utf-8"),
                    "man-vid-1",
                ),
                ("maintai-approved", "datasets/approved/ai4i2020.csv", "ds-vid-1"): (
                    csv_bytes,
                    "ds-vid-1",
                ),
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            slot = Path(tmp) / "ds-0"
            with self.assertRaises(S3VerificationError):
                prepare_slot(
                    record={
                        "bucket": "maintai-approved",
                        "key": "datasets/approved/ai4i-v1.manifest.json",
                        "version_id": "man-vid-1",
                    },
                    directory=slot,
                    s3_client=s3,
                )
            self.assertFalse((slot / "dataset.csv").exists())

        manifest_no_vid = {
            "schema_version": "1.0.0",
            "dataset_version": "ai4i2020-approved-v1",
            "dataset": {
                "bucket": "maintai-approved",
                "key": "datasets/approved/ai4i2020.csv",
                "sha256": "a" * 64,
            },
        }
        s3_no_vid = FakeS3Client(
            {
                ("maintai-approved", "datasets/approved/m.manifest.json", None): (
                    json.dumps(manifest_no_vid).encode("utf-8"),
                    "man-vid-2",
                )
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(S3VerificationError):
                prepare_slot(
                    record={
                        "bucket": "maintai-approved",
                        "key": "datasets/approved/m.manifest.json",
                    },
                    directory=Path(tmp),
                    s3_client=s3_no_vid,
                )

    def test_fetch_artifacts_rejects_escape_version_mismatch_and_corrupt_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "artifacts.json"
            cfg.write_text(
                json.dumps(
                    [
                        {
                            "bucket": "maintai-models",
                            "key": "models/m1/pipeline.joblib",
                            "version_id": "v1",
                            "path": "/etc/passwd",
                            "sha256": "a" * 64,
                        }
                    ]
                )
            )
            with self.assertRaises(S3VerificationError):
                fetch_artifacts_from_lock(cfg, s3_client=FakeS3Client({}))

            cfg.write_text(
                json.dumps(
                    [
                        {
                            "bucket": "maintai-models",
                            "key": "models/m1/pipeline.joblib",
                            "version_id": "v1",
                            "path": "/local/models/m1/../escape.joblib",
                            "sha256": "a" * 64,
                        }
                    ]
                )
            )
            with self.assertRaises(S3VerificationError):
                fetch_artifacts_from_lock(cfg, s3_client=FakeS3Client({}))

            # Mismatched returned VersionId from S3
            payload = b"model-bytes"
            bad_vid_s3 = FakeS3Client(
                {("maintai-models", "models/m1/pipeline.joblib", "v1"): (payload, "v2")}
            )
            cfg.write_text(
                json.dumps(
                    [
                        {
                            "bucket": "maintai-models",
                            "key": "models/m1/pipeline.joblib",
                            "version_id": "v1",
                            "path": "/local/models/m1/pipeline.joblib",
                            "sha256": _sha256(payload),
                        }
                    ]
                )
            )
            with self.assertRaises(S3VerificationError):
                fetch_artifacts_from_lock(cfg, s3_client=bad_vid_s3)

    def test_prepare_rejects_bad_manifest_schema(self):
        s3 = FakeS3Client(
            {
                ("maintai-approved", "datasets/approved/bad.manifest.json", None): (
                    json.dumps({"schema_version": "0.9.0"}).encode("utf-8"),
                    "man-1",
                )
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ManifestValidationError):
                prepare_slot(
                    record={
                        "bucket": "maintai-approved",
                        "key": "datasets/approved/bad.manifest.json",
                    },
                    directory=Path(tmp),
                    s3_client=s3,
                )
