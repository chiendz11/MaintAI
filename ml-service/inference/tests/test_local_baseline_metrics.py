"""Kiểm tra metrics phân biệt request thành công và lỗi."""

import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[3]
API_FILE = ROOT / "ml-service/inference/api/local_baseline.py"


class FakePipeline:
    def predict_proba(self, frame):
        return np.array([[0.8, 0.2]])


class TestLocalBaselineMetrics(unittest.TestCase):
    def setUp(self):
        # Mỗi test load module riêng để có registry/counters mới.
        spec = importlib.util.spec_from_file_location(
            "maintai_metrics_tests", API_FILE
        )
        self.api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.api)

        self.api.STATE.update(
            pipeline=FakePipeline(),
            metadata={
                "threshold": 0.5,
                "model_version": "test-model-v0",
                "preprocessor_version": "test-preprocessor-v0",
            },
        )

        self.body = json.loads(
            (
                ROOT
                / "contracts/v1.3/examples/http/ml/predict-request.json"
            ).read_text(encoding="utf-8")
        )

        # Không chạy startup: test này tập trung vào metrics HTTP.
        self.client = TestClient(self.api.app)
        self.addCleanup(self.client.close)

    def value(self, name, labels=None):
        value = self.api.METRICS.get_sample_value(
            name, labels or {}
        )
        return 0.0 if value is None else value

    def test_request_outcomes_counted_once(self):
        # 1. Thành công: được tính vào prediction và histogram.
        response = self.client.post(
            "/api/v1/predict", json=self.body
        )
        self.assertEqual(response.status_code, 200)

        # 2. Input sai: không chạy inference.
        invalid = json.loads(json.dumps(self.body))
        invalid["features"]["rotational_speed_rpm"] = -1
        response = self.client.post(
            "/api/v1/predict", json=invalid
        )
        self.assertEqual(response.status_code, 422)

        # 3. Inference lỗi: có duration, không có prediction.
        with patch.object(
            self.api.STATE["pipeline"],
            "predict_proba",
            side_effect=RuntimeError("Simulated failure"),
        ):
            with self.assertLogs(self.api.LOGGER, level="ERROR"):
                response = self.client.post(
                    "/api/v1/predict", json=self.body
                )
        self.assertEqual(response.status_code, 500)

        # 4. Model unavailable: không chạy inference.
        self.api.STATE["pipeline"] = None
        response = self.client.post(
            "/api/v1/predict", json=self.body
        )
        self.assertEqual(response.status_code, 503)

        for status in [
            "SUCCESS", "INVALID_INPUT", "ERROR", "NOT_READY"
        ]:
            with self.subTest(status=status):
                self.assertEqual(
                    self.value(
                        "maintai_ml_inference_requests_total",
                        {"status": status},
                    ),
                    1.0,
                )

        self.assertEqual(
            self.value(
                "maintai_ml_predictions_total",
                {"prediction": "NORMAL"},
            ),
            1.0,
        )
        self.assertEqual(
            self.value(
                "maintai_ml_predictions_total",
                {"prediction": "FAILURE"},
            ),
            0.0,
        )
        self.assertEqual(
            self.value("maintai_ml_failure_probability_count"),
            1.0,
        )
        self.assertAlmostEqual(
            self.value("maintai_ml_failure_probability_sum"),
            0.2,
        )
        self.assertEqual(
            self.value("maintai_ml_inference_duration_seconds_count"),
            2.0,
        )

    def test_scraping_does_not_count_as_prediction(self):
        for _ in range(2):
            response = self.client.get("/metrics")
            self.assertEqual(response.status_code, 200)
            self.assertIn(
                "text/plain", response.headers["Content-Type"]
            )
            self.assertIn(
                "# TYPE maintai_ml_inference_requests_total counter",
                response.text,
            )

        self.assertEqual(
            self.value("maintai_ml_inference_duration_seconds_count"),
            0.0,
        )
        for status in [
            "SUCCESS", "INVALID_INPUT", "ERROR", "NOT_READY"
        ]:
            self.assertEqual(
                self.value(
                    "maintai_ml_inference_requests_total",
                    {"status": status},
                ),
                0.0,
            )


if __name__ == "__main__":
    unittest.main()