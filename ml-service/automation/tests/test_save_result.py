import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import httpx
from maintai_automation.mlflow_client import MlflowRestClient
from maintai_automation.save_result import QualityGateError, save_and_register_result


class FakePutS3:
    def __init__(self):
        self.uploads: list[tuple[str, str, int]] = []

    def put_object(self, *, Bucket: str, Key: str, Body: bytes):
        self.uploads.append((Bucket, Key, len(Body)))
        return {"VersionId": f"s3vid-{Path(Key).name}"}


def _write_slot(root: Path, *, recall: float, precision: float) -> Path:
    bundle = root / "bundle"
    bundle.mkdir(parents=True, exist_ok=True)
    files = {}
    for name in ("pipeline.joblib", "metadata.json", "evaluation.json", "drift_reference.json"):
        data = f"content-of-{name}".encode()
        (bundle / name).write_bytes(data)
        files[name] = hashlib.sha256(data).hexdigest()

    model_result = {
        "schema_version": "1.0.0",
        "model_version": "hgb-interactions-deadbeef",
        "preprocessor_version": "sensor-interactions-1234",
        "dataset_version": "ai4i2020-approved-v1",
        "manifest_key": "datasets/approved/ai4i-v1.manifest.json",
        "manifest_version_id": "man-v1",
        "dataset_version_id": "ds-v1",
        "source_git_sha": "a" * 40,
        "bundle_dir": str(bundle),
        "files": files,
        "threshold": 0.09,
        "validation_metrics": {
            "recall_failure": recall,
            "precision_failure": precision,
            "f1": 0.86,
            "f2": 0.85,
            "average_precision": 0.90,
        },
        "test_metrics": {"recall_failure": 0.88, "precision_failure": 0.78},
    }
    (root / "model-result.json").write_text(json.dumps(model_result), encoding="utf-8")
    return root


class SaveResultAndGateTest(unittest.TestCase):
    def test_quality_gate_rejects_low_recall_or_precision_before_upload(self):
        for recall, precision in ((0.849, 0.95), (0.95, 0.499)):
            with tempfile.TemporaryDirectory() as tmp:
                slot = _write_slot(Path(tmp), recall=recall, precision=precision)
                s3 = FakePutS3()
                mlflow = MlflowRestClient(
                    "http://mlflow.test",
                    client=httpx.Client(
                        transport=httpx.MockTransport(lambda req: httpx.Response(500))
                    ),
                )
                with self.assertRaises(QualityGateError):
                    save_and_register_result(
                        directory=slot,
                        s3_client=s3,
                        mlflow_client=mlflow,
                    )
                self.assertEqual(s3.uploads, [])

    def test_save_result_uploads_4_artifacts_and_sets_only_shadow_alias(self):
        calls: list[tuple[str, str, dict | None]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8")) if request.content else None
            calls.append((request.method, request.url.path, body))
            if request.url.path.endswith("/experiments/get-by-name"):
                return httpx.Response(404, json={"error_code": "RESOURCE_DOES_NOT_EXIST"})
            if request.url.path.endswith("/experiments/create"):
                return httpx.Response(200, json={"experiment_id": "7"})
            if request.url.path.endswith("/runs/create"):
                return httpx.Response(200, json={"run": {"info": {"run_id": "run-xyz"}}})
            if request.url.path.endswith("/registered-models/get"):
                return httpx.Response(
                    200, json={"registered_model": {"name": "maintai-classifier"}}
                )
            if request.url.path.endswith("/model-versions/create"):
                return httpx.Response(200, json={"model_version": {"version": "3"}})
            return httpx.Response(200, json={})

        with tempfile.TemporaryDirectory() as tmp:
            slot = _write_slot(Path(tmp), recall=0.86, precision=0.88)
            s3 = FakePutS3()
            mlflow = MlflowRestClient(
                "http://mlflow.test",
                public_url="http://127.0.0.1:15000",
                client=httpx.Client(transport=httpx.MockTransport(handler)),
            )
            published = save_and_register_result(
                directory=slot,
                s3_client=s3,
                mlflow_client=mlflow,
            )

            self.assertEqual(len(s3.uploads), 4)
            self.assertEqual(len(published["artifacts"]), 4)
            self.assertTrue(
                all(a["version_id"].startswith("s3vid-") for a in published["artifacts"])
            )
            self.assertEqual(
                published["mlflow"]["run_url"],
                "http://127.0.0.1:15000/#/experiments/7/runs/run-xyz",
            )

            alias_calls = [c for c in calls if c[1].endswith("/registered-models/alias")]
            self.assertEqual(len(alias_calls), 1)
            self.assertEqual(alias_calls[0][2]["alias"], "shadow")
            self.assertEqual(alias_calls[0][2]["version"], "3")
            self.assertFalse(any(c[2] and c[2].get("alias") == "champion" for c in alias_calls))
