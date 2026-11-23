"""Test hành vi API local bằng model giả; không đo chất lượng ML."""

import importlib.util
import json
import math
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

spec = importlib.util.spec_from_file_location(
    "maintai_local_baseline", API_FILE
)
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


class FakePipeline:
    """Trả score cố định để kiểm tra API độc lập thuật toán."""

    def __init__(self, probability=0.2):
        self.probability = probability

    def predict_proba(self, frame):
        if list(frame.columns) != api.FEATURES:
            raise ValueError("Sai thứ tự feature")
        return np.array([[1 - self.probability, self.probability]])


def valid_request():
    return {
        "event_id": "evt_test",
        "machine_id": "M-001",
        "features": {
            "product_type": "M",
            "air_temperature_k": 298.1,
            "process_temperature_k": 308.6,
            "rotational_speed_rpm": 1551,
            "torque_nm": 42.8,
            "tool_wear_min": 108,
        },
    }


class TestLocalBaselineAPI(unittest.TestCase):
    def setUp(self):
        self.metadata = {
            "model_version": "test-model",
            "preprocessor_version": "test-preprocessor",
            "threshold": 0.5,
        }
        api.STATE["pipeline"] = FakePipeline()
        api.STATE["metadata"] = self.metadata

        # Không chạy lifespan: các test này chủ động thay STATE bằng fixture.
        self.client = TestClient(api.app)

    def tearDown(self):
        self.client.close()
        api.STATE["pipeline"] = None
        api.STATE["metadata"] = None
        api.STATE["fast_path"] = None

    def test_valid_request(self):
        response = self.client.post(
            "/api/v1/predict", json=valid_request()
        )
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result["event_id"], "evt_test")
        self.assertEqual(result["prediction"], "NORMAL")
        self.assertEqual(result["model_threshold"], 0.5)
        self.assertEqual(result["model_version"], "test-model")
        self.assertTrue(math.isfinite(result["failure_probability"]))
        self.assertIn("X-Request-ID", response.headers)
        self.assertIn("X-Correlation-ID", response.headers)

    def test_threshold_boundary_is_failure(self):
        api.STATE["pipeline"] = FakePipeline(probability=0.5)
        response = self.client.post(
            "/api/v1/predict", json=valid_request()
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["prediction"], "FAILURE")

    def test_negative_rpm_rejected(self):
        body = valid_request()
        body["features"]["rotational_speed_rpm"] = -1
        response = self.client.post("/api/v1/predict", json=body)
        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json()["error"]["code"], "VALIDATION_FAILED"
        )
        self.assertNotIn("prediction", response.json())

    def test_missing_feature_rejected(self):
        body = valid_request()
        del body["features"]["torque_nm"]
        response = self.client.post("/api/v1/predict", json=body)
        self.assertEqual(response.status_code, 422)

    def test_extra_feature_rejected(self):
        body = valid_request()
        body["features"]["Machine failure"] = 1
        response = self.client.post("/api/v1/predict", json=body)
        self.assertEqual(response.status_code, 422)

    def test_wrong_type_rejected(self):
        body = valid_request()
        body["features"]["torque_nm"] = "42.8"
        response = self.client.post("/api/v1/predict", json=body)
        self.assertEqual(response.status_code, 422)

    def test_unknown_category_rejected(self):
        body = valid_request()
        body["features"]["product_type"] = "X"
        response = self.client.post("/api/v1/predict", json=body)
        self.assertEqual(response.status_code, 422)

    def test_nonfinite_sensor_rejected(self):
        for token in ["NaN", "Infinity", "-Infinity"]:
            with self.subTest(token=token):
                # Cố ý gửi token không chuẩn để kiểm tra chặn input.
                raw = json.dumps(valid_request()).replace(
                    '"torque_nm": 42.8',
                    f'"torque_nm": {token}',
                )
                response = self.client.post(
                    "/api/v1/predict",
                    content=raw,
                    headers={"Content-Type": "application/json"},
                )
                self.assertIn(response.status_code, [400, 422])
                self.assertNotIn("prediction", response.json())

    def test_malformed_json_rejected(self):
        response = self.client.post(
            "/api/v1/predict",
            content='{"features":',
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 400)

    def test_unavailable_model_returns_503(self):
        api.STATE["pipeline"] = None
        response = self.client.post(
            "/api/v1/predict", json=valid_request()
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["error"]["code"], "MODEL_NOT_READY"
        )
        self.assertNotIn("failure_probability", response.json())

    def test_invalid_model_output_returns_error(self):
        for probability in [float("nan"), -0.1, 1.1]:
            with self.subTest(probability=probability):
                api.STATE["pipeline"] = FakePipeline(probability)
                response = self.client.post(
                    "/api/v1/predict", json=valid_request()
                )
                self.assertEqual(response.status_code, 500)
                self.assertNotIn("prediction", response.json())

    def test_request_ids_propagated(self):
        response = self.client.post(
            "/api/v1/predict",
            json=valid_request(),
            headers={
                "X-Request-ID": "req_known",
                "X-Correlation-ID": "cor_known",
            },
        )
        self.assertEqual(response.headers["X-Request-ID"], "req_known")
        self.assertEqual(
            response.headers["X-Correlation-ID"], "cor_known"
        )

    def test_trace_ids_not_passed_to_pipeline(self):
        body = valid_request()
        body["event_id"] = "evt_other"
        body["machine_id"] = "M-999"

        pipeline = FakePipeline()
        with patch.object(
            pipeline, "predict_proba", wraps=pipeline.predict_proba
        ) as predict:
            api.STATE["pipeline"] = pipeline
            response = self.client.post("/api/v1/predict", json=body)
            self.assertEqual(response.status_code, 200)
            frame = predict.call_args.args[0]
            self.assertEqual(list(frame.columns), api.FEATURES)

    def test_feature_key_order_does_not_change_score(self):
        first = valid_request()
        second = valid_request()
        second["features"] = dict(
            reversed(list(second["features"].items()))
        )

        response_a = self.client.post("/api/v1/predict", json=first)
        response_b = self.client.post("/api/v1/predict", json=second)

        self.assertEqual(response_a.status_code, 200)
        self.assertEqual(response_b.status_code, 200)
        self.assertEqual(
            response_a.json()["failure_probability"],
            response_b.json()["failure_probability"],
        )

    def test_fast_path_matches_real_hgb_pipeline_within_1e12_and_falls_back_cleanly(self):
        import joblib
        import pandas as pd

        repo_root = Path(__file__).resolve().parents[3]
        hgb_dir = repo_root / "ml-service/artifacts/integration/hgb-interactions-m56w5ff4"
        if not (hgb_dir / "pipeline.joblib").is_file():
            self.skipTest("Integration HGB artifact not present")
        import sys

        sys.path.insert(0, str(repo_root / "ml-service/shared/preprocessing"))
        pipeline = joblib.load(hgb_dir / "pipeline.joblib")
        fast_path = api._build_verified_fast_path(pipeline)
        self.assertIsNotNone(fast_path)

        sample_probes = [
            ("L", 297.2, 307.9, 1412.0, 51.4, 42.0),
            ("M", 298.1, 308.6, 1551.0, 42.8, 108.0),
            ("H", 301.4, 311.1, 1340.0, 64.5, 215.0),
            ("H", 304.0, 312.5, 1210.0, 72.1, 240.0),
        ]
        for pt, air, proc, rpm, trq, wear in sample_probes:
            f_obj = api.Features(
                product_type=pt,
                air_temperature_k=air,
                process_temperature_k=proc,
                rotational_speed_rpm=rpm,
                torque_nm=trq,
                tool_wear_min=wear,
            )
            df = pd.DataFrame([f_obj.model_dump()], columns=api.FEATURES)
            ref_prob = float(pipeline.predict_proba(df)[0, 1])
            fast_prob = fast_path.predict_proba_single(f_obj)
            self.assertLessEqual(abs(ref_prob - fast_prob), 1e-12)

        # Fallback: if STATE["pipeline"] changes (e.g. FakePipeline), stale fast_path is ignored
        api.STATE["fast_path"] = fast_path
        api.STATE["pipeline"] = FakePipeline(0.25)
        resp = self.client.post("/api/v1/predict", json=valid_request())
        self.assertEqual(resp.status_code, 200)
        self.assertAlmostEqual(resp.json()["failure_probability"], 0.25, places=12)

        # Unknown pipeline structure returns None cleanly without error
        self.assertIsNone(api._build_verified_fast_path(FakePipeline(0.25)))


if __name__ == "__main__":
    unittest.main()