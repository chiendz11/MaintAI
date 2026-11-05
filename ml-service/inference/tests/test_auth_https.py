"""Kiểm tra HTTPS/JWKS thật trên loopback trong image inference."""

import ipaddress
import json
import ssl
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


# Trong image, source API nằm tại /app.
sys.path.insert(0, "/app")

from auth_runtime import CachedServiceVerifier, make_jwks_fetcher


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        mode = self.server.mode
        self.server.request_count += 1

        status = 200
        headers = {"Content-Type": "application/json"}
        body = json.dumps(self.server.jwks).encode()

        if mode == "slow":
            time.sleep(1.5)
        elif mode == "redirect":
            status = 302
            headers["Location"] = "/keys"
        elif mode == "oversized":
            body = b"x" * 140000
        elif mode == "invalid_json":
            body = b"{broken"
        elif mode == "duplicate_json":
            body = b'{"keys":[],"keys":[]}'
        elif mode == "unavailable":
            status = 503
        elif mode == "encoded":
            headers["Content-Encoding"] = "gzip"

        try:
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            # Client chủ động ngắt khi timeout hoặc từ chối response.
            pass


class TestAuthHTTPS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="maintai-auth-tls-")
        cls.root = Path(cls.directory.name)

        now = datetime.now(timezone.utc)
        ca_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        ca_name = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "MaintAI test CA")
        ])
        ca = (
            x509.CertificateBuilder()
            .subject_name(ca_name)
            .issuer_name(ca_name)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=0),
                critical=True,
            )
            .sign(ca_key, hashes.SHA256())
        )

        tls_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        leaf = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, "localhost")
            ]))
            .issuer_name(ca.subject)
            .public_key(tls_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=True,
            )
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]),
                critical=False,
            )
            .sign(ca_key, hashes.SHA256())
        )

        ca_path = cls.root / "ca.pem"
        certificate_path = cls.root / "server.pem"
        key_path = cls.root / "server-key.pem"

        ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        certificate_path.write_bytes(
            leaf.public_bytes(serialization.Encoding.PEM)
        )
        key_path.write_bytes(
            tls_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )

        # Khóa ký token riêng với khóa TLS.
        cls.signing_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )
        public = json.loads(
            jwt.algorithms.RSAAlgorithm.to_jwk(
                cls.signing_key.public_key()
            )
        )
        public.update({"kid": "https-fixture", "alg": "RS256", "use": "sig"})

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.daemon_threads = True
        cls.server.jwks = {"keys": [public]}
        cls.server.mode = "valid"
        cls.server.request_count = 0

        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(certificate_path, key_path)
        cls.server.socket = server_context.wrap_socket(
            cls.server.socket, server_side=True
        )

        cls.thread = threading.Thread(
            target=cls.server.serve_forever, daemon=True
        )
        cls.thread.start()

        cls.url = f"https://127.0.0.1:{cls.server.server_port}/keys"
        cls.client_context = ssl.create_default_context(cafile=str(ca_path))

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=3)
        cls.directory.cleanup()

    def setUp(self):
        self.server.mode = "valid"
        self.server.request_count = 0
        self.fetch = make_jwks_fetcher(
            self.url, tls_verify=self.client_context
        )

    def test_trusted_https_and_signed_token_with_cache(self):
        verifier = CachedServiceVerifier(
            issuer="https://issuer.example.invalid",
            audience="ml-fixture",
            roles_claim="roles",
            fetch_jwks=self.fetch,
        )
        token = jwt.encode(
            {
                "iss": "https://issuer.example.invalid",
                "aud": "ml-fixture",
                "sub": "processing-fixture",
                "exp": int(time.time()) + 300,
                "roles": ["PROCESSING_SERVICE"],
            },
            self.signing_key,
            algorithm="RS256",
            headers={"kid": "https-fixture"},
        )

        for _ in range(2):
            identity = verifier.verify("Bearer " + token)
            self.assertEqual(identity.subject, "processing-fixture")

        self.assertEqual(self.server.request_count, 1)

    def test_untrusted_certificate_rejected(self):
        fetch = make_jwks_fetcher(self.url)
        with self.assertRaises(httpx.TransportError):
            fetch()

    def test_total_deadline_enforced(self):
        self.server.mode = "slow"
        started = time.monotonic()
        with self.assertRaises((TimeoutError, httpx.TimeoutException)):
            self.fetch()
        self.assertLess(time.monotonic() - started, 2.5)

    def test_redirect_is_not_followed(self):
        self.server.mode = "redirect"
        with self.assertRaises(ValueError):
            self.fetch()
        self.assertEqual(self.server.request_count, 1)

    def test_oversized_response_rejected(self):
        self.server.mode = "oversized"
        with self.assertRaises(ValueError):
            self.fetch()

    def test_invalid_json_rejected(self):
        self.server.mode = "invalid_json"
        with self.assertRaises(ValueError):
            self.fetch()

    def test_duplicate_json_members_rejected(self):
        self.server.mode = "duplicate_json"
        with self.assertRaises(ValueError):
            self.fetch()

    def test_http_failure_rejected(self):
        self.server.mode = "unavailable"
        with self.assertRaises(ValueError):
            self.fetch()

    def test_unexpected_content_encoding_rejected(self):
        self.server.mode = "encoded"
        with self.assertRaises(ValueError):
            self.fetch()


if __name__ == "__main__":
    unittest.main(verbosity=2)