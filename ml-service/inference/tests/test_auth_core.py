"""Kiểm tra auth bằng chữ ký RSA thật; không gọi IdP/network."""

import copy
import json
import sys
import time
import unittest
from pathlib import Path

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa


API = Path(__file__).resolve().parents[1] / "api"
sys.path.insert(0, str(API))

from auth_core import AuthFailure, ServiceTokenVerifier


class TestServiceTokenVerifier(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        cls.other_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        public = json.loads(
            jwt.algorithms.RSAAlgorithm.to_jwk(cls.private_key.public_key())
        )
        public.update({"kid": "fixture-key", "alg": "RS256", "use": "sig"})
        cls.jwks = {"keys": [public]}

    def setUp(self):
        self.verifier = self.make_verifier(self.jwks)

    def make_verifier(self, jwks):
        return ServiceTokenVerifier(
            issuer="https://issuer.example.invalid",
            audience="ml-fixture",
            roles_claim="roles",
            jwks=jwks,
        )

    def token(self, *, changes=None, remove=(), key=None, kid="fixture-key"):
        now = int(time.time())
        claims = {
            "iss": "https://issuer.example.invalid",
            "aud": "ml-fixture",
            "sub": "processing-fixture",
            "iat": now - 5,
            "exp": now + 300,
            "roles": ["PROCESSING_SERVICE"],
        }
        claims.update(changes or {})
        for name in remove:
            claims.pop(name, None)

        return jwt.encode(
            claims,
            self.private_key if key is None else key,
            algorithm="RS256",
            headers={"kid": kid},
        )

    def assert_rejected(self, authorization, status):
        with self.assertRaises(AuthFailure) as caught:
            self.verifier.verify(authorization)
        self.assertEqual(caught.exception.status, status)
        self.assertEqual(
            caught.exception.code,
            "UNAUTHORIZED" if status == 401 else "FORBIDDEN",
        )

    def test_valid_processing_service(self):
        identity = self.verifier.verify("Bearer " + self.token())
        self.assertEqual(identity.subject, "processing-fixture")

    def test_bearer_scheme_is_case_insensitive(self):
        identity = self.verifier.verify("bearer " + self.token())
        self.assertEqual(identity.subject, "processing-fixture")

    def test_missing_malformed_or_oversized_credentials(self):
        for value in (None, "", "Bearer", "Basic abc", "Bearer abc", "x" * 16385):
            with self.subTest(value_type=type(value).__name__):
                self.assert_rejected(value, 401)

    def test_wrong_signature(self):
        self.assert_rejected(
            "Bearer " + self.token(key=self.other_key), 401
        )

    def test_unknown_key(self):
        self.assert_rejected(
            "Bearer " + self.token(kid="unknown-key"), 401
        )

    def test_invalid_claims(self):
        cases = [
            {"iss": "https://other.example.invalid"},
            {"aud": "business"},
            {"exp": int(time.time()) - 60},
            {"nbf": int(time.time()) + 300},
            {"iat": int(time.time()) + 300},
            {"sub": ""},
            {"sub": " "},
            {"exp": str(int(time.time()) + 300)},
            {"iat": True},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                self.assert_rejected(
                    "Bearer " + self.token(changes=changes), 401
                )

    def test_required_claims(self):
        for name in ("iss", "aud", "exp", "sub"):
            with self.subTest(claim=name):
                self.assert_rejected(
                    "Bearer " + self.token(remove=(name,)), 401
                )

    def test_wrong_or_malformed_roles(self):
        for roles in (
            [],
            ["BUSINESS_SERVICE"],
            "PROCESSING_SERVICE",
            ["PROCESSING_SERVICE", 123],
            None,
        ):
            with self.subTest(roles=roles):
                self.assert_rejected(
                    "Bearer " + self.token(changes={"roles": roles}), 403
                )

    def test_hmac_algorithm_rejected(self):
        token = jwt.encode(
            {"sub": "processing-fixture"},
            "fixture-secret-" * 4,
            algorithm="HS256",
            headers={"kid": "fixture-key"},
        )
        self.assert_rejected("Bearer " + token, 401)

    def test_duplicate_keys_rejected(self):
        duplicate = copy.deepcopy(self.jwks)
        duplicate["keys"].append(copy.deepcopy(duplicate["keys"][0]))
        with self.assertRaises(ValueError):
            self.make_verifier(duplicate)

    def test_empty_keyset_rejected(self):
        with self.assertRaises(ValueError):
            self.make_verifier({"keys": []})

    def test_missing_configuration_rejected(self):
        with self.assertRaises(ValueError):
            ServiceTokenVerifier(
                issuer="",
                audience="ml-fixture",
                roles_claim="roles",
                jwks=self.jwks,
            )


if __name__ == "__main__":
    unittest.main()