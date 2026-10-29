"""Kiểm tra response API theo schema repo; không đo model quality."""

import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parents[3]
SCHEMAS = ROOT / "contracts" / "v1.3" / "schemas"
API_FILE = ROOT / "ml-service" / "inference" / "api" / "local_baseline.py"

spec = importlib.util.spec_from_file_location(
    "maintai_contract_tests", API_FILE
)
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


# Đăng ký schema local để giải quyết $ref, không tải schema từ mạng.
registry = Registry()

for name in [
    "common.schema.json",
    "features.schema.json",
    "prediction.schema.json",
]:
    path = SCHEMAS / name
    schema = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    registry = registry.with_resource(
        path.as_uri(),
        Resource.from_contents(schema),
    )


def validate_payload(payload, filename, fragment=""):
    """Đối chiếu JSON với schema và kiểm tra format timestamp."""
    reference = (SCHEMAS / filename).as_uri() + fragment
    validator = Draft202012Validator(
        {"$ref": reference},
        registry=registry,
        format_checker=FormatChecker(),
    )
    validator.validate(payload)


class FakePipeline:
    def __init__(self, probability=0.2):
        self.probability = probability

    def predict_proba(self, frame):
        return np.array([
            [1 - self.probability, self.probability]
        ])


class TestLocalBaselineContract(unittest.TestCase):
    def setUp(self):
        self.request = json.loads(
            (
                ROOT
                / "contracts/v1.3/examples/http/ml/predict-request.json"
            ).read_text(encoding="utf-8")
        )

        state_patch = patch.dict(
            api.STATE,
            {
                "pipeline": FakePipeline(),
                "metadata": {
                    "threshold": 0.5,
                    "model_version": "test-model-v0",
                    "preprocessor_version": "test-preprocessor-v0",
                },
            },
        )
        state_patch.start()
        self.addCleanup(state_patch.stop)

        # Không chạy lifespan ở file này: startup đã có tests riêng.
        # Ta cấp STATE giả để tập trung kiểm tra HTTP response.
        self.client = TestClient(api.app)
        self.addCleanup(self.client.close)

    def assert_trace_headers(self, response):
        self.assertTrue(response.headers.get("X-Request-ID"))
        self.assertTrue(response.headers.get("X-Correlation-ID"))

    def assert_error(self, response, status, code, retryable):
        self.assertEqual(response.status_code, status)
        body = response.json()

        validate_payload(
            body, "common.schema.json", "#/$defs/ErrorEnvelope"
        )
        self.assertEqual(body["error"]["code"], code)
        self.assertIs(body["error"]["retryable"], retryable)

        self.assert_trace_headers(response)
        self.assertEqual(
            body["error"]["request_id"],
            response.headers["X-Request-ID"],
        )
        self.assertEqual(
            body["error"]["correlation_id"],
            response.headers["X-Correlation-ID"],
        )

    def test_repo_request_example_matches_schema(self):
        validate_payload(
            self.request,
            "prediction.schema.json",
            "#/$defs/Request",
        )

    def test_prediction_schema_and_threshold_rule(self):
        # Kiểm tra dưới ngưỡng, đúng ngưỡng và trên ngưỡng.
        for probability, label in [
            (0.2, "NORMAL"),
            (0.5, "FAILURE"),
            (0.8, "FAILURE"),
        ]:
            with self.subTest(probability=probability):
                api.STATE["pipeline"] = FakePipeline(probability)
                response = self.client.post(
                    "/api/v1/predict", json=self.request
                )

                self.assertEqual(response.status_code, 200)
                body = response.json()
                validate_payload(body, "prediction.schema.json")

                self.assertEqual(body["prediction"], label)
                self.assertEqual(
                    body["failure_probability"], probability
                )
                self.assertEqual(body["model_threshold"], 0.5)
                self.assertEqual(
                    body["event_id"], self.request["event_id"]
                )
                self.assert_trace_headers(response)

    def test_liveness_schema(self):
        response = self.client.get("/health/live")
        self.assertEqual(response.status_code, 200)
        validate_payload(
            response.json(),
            "common.schema.json",
            "#/$defs/Liveness",
        )
        self.assert_trace_headers(response)

    def test_readiness_schema_ready_and_unavailable(self):
        response = self.client.get("/health/ready")
        self.assertEqual(response.status_code, 200)
        validate_payload(
            response.json(),
            "common.schema.json",
            "#/$defs/Readiness",
        )
        self.assertEqual(response.json()["status"], "UP")

        api.STATE["pipeline"] = None
        response = self.client.get("/health/ready")
        self.assertEqual(response.status_code, 503)
        validate_payload(
            response.json(),
            "common.schema.json",
            "#/$defs/Readiness",
        )
        self.assertEqual(response.json()["status"], "DOWN")
        self.assertFalse(
            response.json()["checks"]["model_loaded"]
        )

    def test_validation_error_schema(self):
        del self.request["features"]["torque_nm"]
        response = self.client.post(
            "/api/v1/predict", json=self.request
        )
        self.assert_error(
            response, 422, "VALIDATION_FAILED", False
        )

    def test_malformed_json_error_schema(self):
        response = self.client.post(
            "/api/v1/predict",
            content="{",
            headers={"Content-Type": "application/json"},
        )
        self.assert_error(
            response, 400, "INVALID_REQUEST", False
        )

    def test_model_unavailable_error_schema(self):
        api.STATE["pipeline"] = None
        response = self.client.post(
            "/api/v1/predict", json=self.request
        )
        self.assert_error(
            response, 503, "MODEL_NOT_READY", True
        )

    def test_inference_error_schema(self):
        with patch.object(
            api.STATE["pipeline"],
            "predict_proba",
            side_effect=RuntimeError("Simulated inference error"),
        ):
            with self.assertLogs(api.LOGGER, level="ERROR"):
                response = self.client.post(
                    "/api/v1/predict", json=self.request
                )

        self.assert_error(
            response, 500, "INTERNAL_ERROR", False
        )


if __name__ == "__main__":
    unittest.main()