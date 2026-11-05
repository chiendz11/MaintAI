"""Kiểm tra cache, rotation và lỗi nguồn khóa bằng RSA thật."""

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

from auth_core import AuthFailure
from auth_runtime import (
    AuthDependencyUnavailable,
    CachedServiceVerifier,
    make_jwks_fetcher,
)


class TestAuthRuntime(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.keys = {
            kid: rsa.generate_private_key(
                public_exponent=65537, key_size=2048
            )
            for kid in ("old", "new")
        }
        cls.public = {}
        for kid, private in cls.keys.items():
            public = json.loads(
                jwt.algorithms.RSAAlgorithm.to_jwk(private.public_key())
            )
            public.update({"kid": kid, "alg": "RS256", "use": "sig"})
            cls.public[kid] = public

    def setUp(self):
        self.now = 100.0
        self.calls = 0
        self.fail = False
        self.document = {"keys": [copy.deepcopy(self.public["old"])]}

        def fetch():
            self.calls += 1
            if self.fail:
                raise OSError("Fixture dependency failure")
            return copy.deepcopy(self.document)

        self.verifier = CachedServiceVerifier(
            issuer="https://issuer.example.invalid",
            audience="ml-fixture",
            roles_claim="roles",
            fetch_jwks=fetch,
            cache_seconds=30,
            clock=lambda: self.now,
        )

    def token(self, kid="old"):
        token = jwt.encode(
            {
                "iss": "https://issuer.example.invalid",
                "aud": "ml-fixture",
                "sub": "processing-fixture",
                "exp": int(time.time()) + 300,
                "roles": ["PROCESSING_SERVICE"],
            },
            self.keys[kid],
            algorithm="RS256",
            headers={"kid": kid},
        )
        return "Bearer " + token

    def test_first_request_loads_and_next_request_uses_cache(self):
        self.verifier.verify(self.token())
        self.verifier.verify(self.token())
        self.assertEqual(self.calls, 1)

    def test_malformed_credentials_do_not_fetch(self):
        for value in (None, "", "Bearer broken", "Basic abc"):
            with self.subTest(value=value):
                with self.assertRaises(AuthFailure):
                    self.verifier.verify(value)
        self.assertEqual(self.calls, 0)

    def test_expired_cache_refreshes(self):
        self.verifier.verify(self.token())
        self.now += 31
        self.verifier.verify(self.token())
        self.assertEqual(self.calls, 2)

    def test_rotation_accepts_new_key(self):
        self.verifier.verify(self.token())
        self.document = {"keys": [copy.deepcopy(self.public["new"])]}
        self.now += 6

        identity = self.verifier.verify(self.token("new"))
        self.assertEqual(identity.subject, "processing-fixture")
        self.assertEqual(self.calls, 2)

        with self.assertRaises(AuthFailure):
            self.verifier.verify(self.token("old"))

    def test_unknown_key_refresh_is_rate_limited(self):
        self.verifier.verify(self.token())
        for _ in range(10):
            with self.assertRaises(AuthFailure):
                self.verifier.verify(self.token("new"))
        self.assertEqual(self.calls, 1)

        self.now += 6
        with self.assertRaises(AuthFailure):
            self.verifier.verify(self.token("new"))
        self.assertEqual(self.calls, 2)

    def test_dependency_failure_without_cache(self):
        self.fail = True
        with self.assertRaises(AuthDependencyUnavailable):
            self.verifier.verify(self.token())
        self.assertFalse(self.verifier.check())
        self.assertEqual(self.calls, 1)

    def test_expired_keys_are_not_used_after_refresh_failure(self):
        self.verifier.verify(self.token())
        self.now += 31
        self.fail = True

        with self.assertRaises(AuthDependencyUnavailable):
            self.verifier.verify(self.token())
        self.assertFalse(self.verifier.check())

    def test_valid_cached_key_survives_temporary_source_outage(self):
        self.verifier.verify(self.token())
        self.fail = True
        identity = self.verifier.verify(self.token())
        self.assertEqual(identity.subject, "processing-fixture")
        self.assertEqual(self.calls, 1)

    def test_recovery_after_dependency_returns(self):
        self.fail = True
        self.assertFalse(self.verifier.check())

        self.fail = False
        self.now += 2
        self.assertTrue(self.verifier.check())
        self.verifier.verify(self.token())
        self.assertEqual(self.calls, 2)

    def test_invalid_refresh_cannot_replace_verified_snapshot(self):
        self.verifier.verify(self.token())
        self.document = {"keys": []}
        self.now += 6

        with self.assertRaises(AuthDependencyUnavailable):
            self.verifier.verify(self.token("new"))

        # Snapshot cũ vẫn còn trong TTL và vẫn xác minh đúng key cũ.
        self.verifier.verify(self.token("old"))
        self.assertEqual(self.calls, 2)

    def test_insecure_jwks_configuration_rejected(self):
        for url in (
            "http://issuer.example.invalid/keys",
            "https://user:password@issuer.example.invalid/keys",
            "https://issuer.example.invalid/keys#fragment",
        ):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    make_jwks_fetcher(url)

        with self.assertRaises(ValueError):
            make_jwks_fetcher(
                "https://issuer.example.invalid/keys",
                tls_verify=False,
            )


if __name__ == "__main__":
    unittest.main()