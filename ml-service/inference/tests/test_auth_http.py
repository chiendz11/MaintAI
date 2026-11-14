"""Auth HTTP với API thật, RSA thật và model fixture có theo dõi số lần gọi."""

import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import jwt
import numpy as np
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient


API = Path(__file__).resolve().parents[1] / "api"
sys.path.insert(0, str(API))

import local_baseline as api
from authenticated_local import AuthenticatedInference, create_app
from auth_runtime import CachedServiceVerifier


class TestAuthHTTP(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        public = json.loads(
            jwt.algorithms.RSAAlgorithm.to_jwk(cls.key.public_key())
        )
        public.update({"kid": "fixture", "alg": "RS256", "use": "sig"})
        cls.jwks = {"keys": [public]}

    def setUp(self):
        self.fetch = Mock(return_value=self.jwks)
        verifier = CachedServiceVerifier(
            issuer="https://issuer.example.invalid",
            audience="ml-fixture",
            roles_claim="roles",
            fetch_jwks=self.fetch,
        )
        self.pipeline = Mock()
        self.pipeline.predict_proba.return_value = np.array([[0.8, 0.2]])

        api.STATE["pipeline"] = self.pipeline
        api.STATE["metadata"] = {
            "model_version": "fixture-model",
            "preprocessor_version": "fixture-preprocessor",
            "threshold": 0.5,
        }

        # Fixture chủ động thay model; không chạy lifespan nạp artifact.
        self.client = TestClient(AuthenticatedInference(api.app, verifier))
        self.body = {
            "event_id": "evt-auth-test",
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

    def tearDown(self):
        self.client.close()
        api.STATE["pipeline"] = None
        api.STATE["metadata"] = None

    def headers(self, *, role="PROCESSING_SERVICE", expired=False):
        now = int(time.time())
        token = jwt.encode(
            {
                "iss": "https://issuer.example.invalid",
                "aud": "ml-fixture",
                "sub": "processing-fixture",
                "exp": now - 60 if expired else now + 300,
                "roles": [role],
            },
            self.key,
            algorithm="RS256",
            headers={"kid": "fixture"},
        )
        return {"Authorization": "Bearer " + token}

    def assert_blocked(self, response, status, code):
        self.assertEqual(response.status_code, status)
        error = response.json()["error"]
        self.assertEqual(error["code"], code)
        self.assertFalse(error["retryable"])
        self.assertNotIn("prediction", response.json())
        self.pipeline.predict_proba.assert_not_called()

    def test_missing_token_blocks_before_json_validation(self):
        response = self.client.post(
            "/api/v1/predict",
            content="{broken",
            headers={"Content-Type": "application/json"},
        )
        self.assert_blocked(response, 401, "UNAUTHORIZED")
        self.assertEqual(response.headers["WWW-Authenticate"], "Bearer")
        self.fetch.assert_not_called()

    def test_wrong_role_blocks_model(self):
        response = self.client.post(
            "/api/v1/predict",
            json=self.body,
            headers=self.headers(role="BUSINESS_SERVICE"),
        )
        self.assert_blocked(response, 403, "FORBIDDEN")

    def test_expired_token_blocks_model(self):
        response = self.client.post(
            "/api/v1/predict",
            json=self.body,
            headers=self.headers(expired=True),
        )
        self.assert_blocked(response, 401, "UNAUTHORIZED")

    def test_duplicate_authorization_rejected(self):
        token = self.headers()["Authorization"]
        response = self.client.post(
            "/api/v1/predict",
            json=self.body,
            headers=[
                ("Authorization", token),
                ("Authorization", token),
            ],
        )
        self.assert_blocked(response, 401, "UNAUTHORIZED")

    def test_valid_token_reaches_model_once(self):
        response = self.client.post(
            "/api/v1/predict",
            json=self.body,
            headers=self.headers(),
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["prediction"], "NORMAL")
        self.pipeline.predict_proba.assert_called_once()

        frame = self.pipeline.predict_proba.call_args.args[0]
        self.assertEqual(list(frame.columns), api.FEATURES)

    def test_valid_token_keeps_body_validation(self):
        body = dict(self.body)
        body["features"] = dict(body["features"], rotational_speed_rpm=-1)
        response = self.client.post(
            "/api/v1/predict", json=body, headers=self.headers()
        )
        self.assertEqual(response.status_code, 422)
        self.pipeline.predict_proba.assert_not_called()

    def test_dependency_failure_never_returns_prediction(self):
        self.fetch.side_effect = OSError("Fixture JWKS outage")
        response = self.client.post(
            "/api/v1/predict",
            json=self.body,
            headers=self.headers(),
        )
        self.assert_blocked(response, 500, "INTERNAL_ERROR")

    def test_outage_changes_readiness_but_not_liveness(self):
        self.fetch.side_effect = OSError("Fixture JWKS outage")

        self.assertEqual(self.client.get("/health/live").status_code, 200)
        response = self.client.get("/health/ready")
        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["checks"]["auth_ready"])
        self.assertTrue(response.json()["checks"]["model_loaded"])

    def test_public_operations_without_token(self):
        for path in ("/health/live", "/health/ready", "/metrics"):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 200)
        self.pipeline.predict_proba.assert_not_called()

    def test_rejection_preserves_trace_ids(self):
        response = self.client.post(
            "/api/v1/predict",
            json=self.body,
            headers={
                "X-Request-ID": "req-auth-test",
                "X-Correlation-ID": "cor-auth-test",
            },
        )
        self.assert_blocked(response, 401, "UNAUTHORIZED")
        self.assertEqual(response.headers["X-Request-ID"], "req-auth-test")
        self.assertEqual(
            response.json()["error"]["correlation_id"], "cor-auth-test"
        )

    def test_factory_requires_explicit_local_mode(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(RuntimeError):
                create_app()

    def test_factory_requires_identity_configuration(self):
        with patch.dict(
            "os.environ", {"MAINTAI_LOCAL_ONLY": "1"}, clear=True
        ):
            with self.assertRaises(RuntimeError):
                create_app()


if __name__ == "__main__":
    unittest.main()