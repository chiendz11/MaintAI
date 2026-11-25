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
        return np.tile([1 - self.probability, self.probability], (len(frame), 1))


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

    def test_predict_batch_preserves_order_and_rejects_empty_or_oversized(self):
        req_a = valid_request()
        req_b = valid_request()
        req_b["event_id"] = "evt_second"
        req_b["features"]["product_type"] = "H"
        response = self.client.post("/api/v1/predict/batch", json={"items": [req_a, req_b]})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["batch_size"], 2)
        self.assertEqual([r["event_id"] for r in body["results"]], ["evt_test", "evt_second"])
        self.assertEqual(body["results"][0]["prediction"], "NORMAL")

        self.assertEqual(self.client.post("/api/v1/predict/batch", json={"items": []}).status_code, 422)
        too_many = {"items": [valid_request() for _ in range(65)]}
        self.assertEqual(self.client.post("/api/v1/predict/batch", json=too_many).status_code, 422)

    def test_vectorized_fast_path_parity_matches_sklearn_pipeline(self):
        import pandas as pd
        from sklearn.compose import ColumnTransformer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler

        rows = [
            {"product_type": "L", "air_temperature_k": 297.0, "process_temperature_k": 307.5, "rotational_speed_rpm": 1400.0, "torque_nm": 52.0, "tool_wear_min": 200.0},
            {"product_type": "M", "air_temperature_k": 298.5, "process_temperature_k": 308.8, "rotational_speed_rpm": 1550.0, "torque_nm": 41.0, "tool_wear_min": 95.0},
            {"product_type": "H", "air_temperature_k": 301.0, "process_temperature_k": 310.9, "rotational_speed_rpm": 1820.0, "torque_nm": 30.0, "tool_wear_min": 18.0},
            {"product_type": "L", "air_temperature_k": 300.2, "process_temperature_k": 311.1, "rotational_speed_rpm": 1320.0, "torque_nm": 61.2, "tool_wear_min": 225.0},
        ]
        df = pd.DataFrame(rows, columns=api.FEATURES)
        y = np.array([1, 0, 0, 1])
        pre = ColumnTransformer(
            [
                ("cat", OneHotEncoder(categories=[["L", "M", "H"]], handle_unknown="ignore"), ["product_type"]),
                ("num", StandardScaler(), api.FEATURES[1:]),
            ]
        )
        pipe = Pipeline([("pre", pre), ("clf", LogisticRegression())]).fit(df, y)
        fast = api._build_fast_predictor(pipe, api.Path("."))
        self.assertIsNotNone(fast)
        api.STATE["pipeline"] = pipe
        api.STATE["fast_predict"] = fast
        api.STATE["fast_pipeline"] = pipe
        batch_req = {"items": [{**valid_request(), "event_id": f"e-{i}", "features": r} for i, r in enumerate(rows)]}
        res = self.client.post("/api/v1/predict/batch", json=batch_req)
        self.assertEqual(res.status_code, 200)
        ref = pipe.predict_proba(df)[:, 1]
        got = [item["failure_probability"] for item in res.json()["results"]]
        self.assertLess(float(np.max(np.abs(np.array(got) - ref))), 1e-12)


if __name__ == "__main__":
    unittest.main()