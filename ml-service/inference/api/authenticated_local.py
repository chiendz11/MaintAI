"""API thử tích hợp có JWT; chưa phải production release."""

import os
import ssl
from uuid import uuid4

from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse

import local_baseline
from auth_core import AuthFailure
from auth_runtime import (
    AuthDependencyUnavailable,
    CachedServiceVerifier,
    make_jwks_fetcher,
)


class AuthenticatedInference:
    def __init__(self, app, verifier):
        self.app = app
        self.verifier = verifier

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"].rstrip("/")
        method = scope["method"]

        if method == "GET" and path == "/health/ready":
            auth_ready = await run_in_threadpool(self.verifier.check)
            if not auth_ready:
                model_loaded = (
                    local_baseline.STATE["pipeline"] is not None
                    and local_baseline.STATE["metadata"] is not None
                )
                response = JSONResponse(
                    status_code=503,
                    content={
                        "status": "DOWN",
                        "checks": {
                            "model_loaded": model_loaded,
                            "auth_ready": False,
                        },
                    },
                    headers=self.trace_headers(scope),
                )
                await response(scope, receive, send)
                return

        if method != "POST" or path != "/api/v1/predict":
            await self.app(scope, receive, send)
            return

        # Không đọc body trước khi xác thực.
        authorization = [
            value.decode("latin-1")
            for key, value in scope.get("headers", [])
            if key.lower() == b"authorization"
        ]

        try:
            if len(authorization) != 1:
                raise AuthFailure(401, "UNAUTHORIZED")

            identity = await run_in_threadpool(
                self.verifier.verify,
                authorization[0],
            )
        except AuthFailure as exc:
            await self.reject(
                scope, receive, send,
                status=exc.status,
                code=exc.code,
                message=(
                    "Authentication required"
                    if exc.status == 401
                    else "Insufficient service permissions"
                ),
            )
            return
        except AuthDependencyUnavailable:
            # Contract hiện tại chưa khai báo AUTH_UNAVAILABLE.
            # Dùng INTERNAL_ERROR đã có, không giả thành model lỗi hoặc NORMAL.
            await self.reject(
                scope, receive, send,
                status=500,
                code="INTERNAL_ERROR",
                message="Authentication service unavailable",
            )
            return

        scope.setdefault("state", {})["service_subject"] = identity.subject
        await self.app(scope, receive, send)

    @staticmethod
    def trace_headers(scope):
        headers = {
            key.lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        return {
            "X-Request-ID": (
                headers.get(b"x-request-id") or f"req_{uuid4().hex}"
            ),
            "X-Correlation-ID": (
                headers.get(b"x-correlation-id") or f"cor_{uuid4().hex}"
            ),
        }

    async def reject(self, scope, receive, send, *, status, code, message):
        headers = self.trace_headers(scope)
        if status == 401:
            headers["WWW-Authenticate"] = "Bearer"

        # Request bị chặn không đi qua middleware metrics của API phía trong.
        local_baseline.ML_REQUESTS.labels(
            status="UNAUTHORIZED" if status == 401 else
                   "FORBIDDEN" if status == 403 else "ERROR"
        ).inc()

        response = JSONResponse(
            status_code=status,
            content={
                "error": {
                    "code": code,
                    "message": message,
                    "retryable": False,
                    "details": {},
                    "request_id": headers["X-Request-ID"],
                    "correlation_id": headers["X-Correlation-ID"],
                }
            },
            headers=headers,
        )
        await response(scope, receive, send)


def create_app():
    if os.environ.get("MAINTAI_LOCAL_ONLY") != "1":
        raise RuntimeError(
            "Authenticated integration candidate requires MAINTAI_LOCAL_ONLY=1"
        )

    names = (
        "MAINTAI_AUTH_ISSUER",
        "MAINTAI_AUTH_AUDIENCE",
        "MAINTAI_AUTH_ROLES_CLAIM",
        "MAINTAI_AUTH_JWKS_URL",
    )
    configuration = {}
    for name in names:
        value = os.environ.get(name)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing required configuration: {name}")
        configuration[name] = value

    # Có thể dùng CA fixture riêng; không tắt kiểm tra TLS.
    ca_file = os.environ.get("MAINTAI_AUTH_CA_FILE")
    tls_context = ssl.create_default_context(cafile=ca_file or None)

    verifier = CachedServiceVerifier(
        issuer=configuration["MAINTAI_AUTH_ISSUER"],
        audience=configuration["MAINTAI_AUTH_AUDIENCE"],
        roles_claim=configuration["MAINTAI_AUTH_ROLES_CLAIM"],
        fetch_jwks=make_jwks_fetcher(
            configuration["MAINTAI_AUTH_JWKS_URL"],
            tls_verify=tls_context,
        ),
    )

    return AuthenticatedInference(local_baseline.app, verifier)