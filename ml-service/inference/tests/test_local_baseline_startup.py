"""Kiểm tra startup/health bằng artifact tạm và model giả.

Không sử dụng dataset, không train và không đo chất lượng model.
"""

import hashlib
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from fastapi.testclient import TestClient


API_FILE = (
    Path(__file__).resolve().parents[1]
    / "api"
    / "local_baseline.py"
)

# Load module riêng để không dùng chung STATE với file test cũ.
spec = importlib.util.spec_from_file_location(
    "maintai_baseline_startup_tests",
    API_FILE,
)
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


class FakePipeline:
    """Giả lập pipeline đã fit để kiểm tra logic khởi động."""

    def __init__(self, probabilities=None):
        self.feature_names_in_ = np.array(api.FEATURES)
        self.classes_ = np.array([0, 1])
        self.probabilities = np.array(
            [[0.8, 0.2]] if probabilities is None else probabilities,
            dtype=float,
        )

    def predict_proba(self, frame):
        if list(frame.columns) != api.FEATURES:
            raise ValueError("Feature order mismatch")
        return self.probabilities.copy()


def valid_request():
    return {
        "event_id": "evt_startup_test",
        "machine_id": "M-test",
        "features": {
            "product_type": "M",
            "air_temperature_k": 298.1,
            "process_temperature_k": 308.6,
            "rotational_speed_rpm": 1551,
            "torque_nm": 42.8,
            "tool_wear_min": 108,
        },
    }


class TestLocalBaselineStartup(unittest.TestCase):
    def setUp(self):
        # Mỗi test có thư mục riêng, không đụng artifact thật.
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.model_dir = Path(self.temp.name)

        # Không deserialize file này: joblib.load được mock.
        artifact_bytes = b"startup-test-placeholder"
        (self.model_dir / "pipeline.joblib").write_bytes(artifact_bytes)

        self.metadata = {
            "pipeline_sha256": hashlib.sha256(
                artifact_bytes
            ).hexdigest(),
            "features": list(api.FEATURES),
            "sklearn_version": api.sklearn.__version__,
            "threshold": 0.5,
            "model_version": "test-model-v0",
            "preprocessor_version": "test-preprocessor-v0",
        }
        self.write_metadata()

        env = patch.dict(
            os.environ,
            {
                "MAINTAI_LOCAL_ONLY": "1",
                "MODEL_DIR": str(self.model_dir),
            },
        )
        env.start()
        self.addCleanup(env.stop)
        self.addCleanup(self.clear_state)
        self.clear_state()

    def clear_state(self):
        api.STATE["pipeline"] = None
        api.STATE["metadata"] = None

    def write_metadata(self):
        (self.model_dir / "metadata.json").write_text(
            json.dumps(self.metadata),
            encoding="utf-8",
        )

    def assert_unavailable(self, client):
        live = client.get("/health/live")
        self.assertEqual(live.status_code, 200)
        self.assertEqual(live.json()["status"], "UP")

        ready = client.get("/health/ready")
        self.assertEqual(ready.status_code, 503)
        self.assertEqual(
            ready.json(),
            {
                "status": "DOWN",
                "checks": {"model_loaded": False},
            },
        )

        prediction = client.post(
            "/api/v1/predict",
            json=valid_request(),
        )
        self.assertEqual(prediction.status_code, 503)
        self.assertEqual(
            prediction.json()["error"]["code"],
            "MODEL_NOT_READY",
        )
        self.assertNotIn("prediction", prediction.json())
        self.assertNotIn("failure_probability", prediction.json())

    def test_successful_startup_and_shutdown(self):
        pipeline = FakePipeline()

        with patch.object(
            api.joblib, "load", return_value=pipeline
        ) as loader:
            # Context manager chạy startup khi vào, shutdown khi ra.
            with TestClient(api.app) as client:
                self.assertEqual(
                    client.get("/health/live").status_code, 200
                )

                ready = client.get("/health/ready")
                self.assertEqual(ready.status_code, 200)
                self.assertTrue(
                    ready.json()["checks"]["model_loaded"]
                )

                response = client.post(
                    "/api/v1/predict",
                    json=valid_request(),
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json()["failure_probability"], 0.2
                )

                loader.assert_called_once()

        # Shutdown phải giải phóng trạng thái.
        self.assertIsNone(api.STATE["pipeline"])
        self.assertIsNone(api.STATE["metadata"])

    def test_checksum_mismatch_rejected_before_deserialization(self):
        (self.model_dir / "pipeline.joblib").write_bytes(b"corrupted")

        with patch.object(api.joblib, "load") as loader:
            with self.assertLogs(api.LOGGER, level="ERROR"):
                with TestClient(api.app) as client:
                    self.assert_unavailable(client)

            loader.assert_not_called()

    def test_missing_artifact_does_not_reuse_previous_model(self):
        # Giả lập trạng thái còn sót từ lần chạy trước.
        api.STATE["pipeline"] = FakePipeline()
        api.STATE["metadata"] = dict(self.metadata)
        (self.model_dir / "pipeline.joblib").unlink()

        with patch.object(api.joblib, "load") as loader:
            with self.assertLogs(api.LOGGER, level="ERROR"):
                with TestClient(api.app) as client:
                    self.assert_unavailable(client)

            loader.assert_not_called()

    def test_sklearn_version_mismatch_rejected(self):
        self.metadata["sklearn_version"] = "incompatible-test-version"
        self.write_metadata()

        with patch.object(api.joblib, "load") as loader:
            with self.assertLogs(api.LOGGER, level="ERROR"):
                with TestClient(api.app) as client:
                    self.assert_unavailable(client)

            loader.assert_not_called()

    def test_loaded_feature_order_mismatch_rejected(self):
        pipeline = FakePipeline()
        pipeline.feature_names_in_ = np.array(
            list(reversed(api.FEATURES))
        )

        with patch.object(api.joblib, "load", return_value=pipeline):
            with self.assertLogs(api.LOGGER, level="ERROR"):
                with TestClient(api.app) as client:
                    self.assert_unavailable(client)

    def test_invalid_startup_probabilities_rejected(self):
        cases = {
            "nan": [[float("nan"), 0.2]],
            "out_of_range": [[-0.1, 1.1]],
            "wrong_sum": [[0.8, 0.8]],
            "wrong_shape": [[0.2]],
        }

        for name, probabilities in cases.items():
            with self.subTest(case=name):
                pipeline = FakePipeline(probabilities)

                with patch.object(
                    api.joblib, "load", return_value=pipeline
                ):
                    with self.assertLogs(api.LOGGER, level="ERROR"):
                        with TestClient(api.app) as client:
                            self.assert_unavailable(client)

    def test_probe_exception_keeps_model_unavailable(self):
        pipeline = FakePipeline()

        with patch.object(
            pipeline,
            "predict_proba",
            side_effect=RuntimeError("Simulated preprocessing failure"),
        ):
            with patch.object(
                api.joblib, "load", return_value=pipeline
            ):
                with self.assertLogs(api.LOGGER, level="ERROR"):
                    with TestClient(api.app) as client:
                        self.assert_unavailable(client)

    def test_local_mode_requires_explicit_opt_in(self):
        with patch.dict(os.environ, {"MAINTAI_LOCAL_ONLY": "0"}):
            with self.assertRaisesRegex(
                RuntimeError, "MAINTAI_LOCAL_ONLY=1"
            ):
                with TestClient(api.app):
                    pass


if __name__ == "__main__":
    unittest.main()