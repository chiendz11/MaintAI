import hmac
import logging
import os
from functools import lru_cache
from pathlib import Path
from uuid import uuid4

import jwt
from api.fallback import FALLBACK_MESSAGE
from api.schemas import (
    MaintenanceBriefRequest,
    MaintenanceBriefResponse,
)
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from llm_generator.generator import LLMProviderError

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["RAG Briefs"],
)


# Exception dùng khi RAG không khả dụng
class RAGUnavailableError(Exception):
    pass


class _OIDCAuthError(Exception):
    def __init__(self, status_code: int, code: str, message: str):
        self.status_code = status_code
        self.code = code
        self.message = message
        super().__init__(message)


@lru_cache(maxsize=4)
def _jwk_client(jwks_url: str) -> jwt.PyJWKClient:
    return jwt.PyJWKClient(jwks_url, timeout=5, lifespan=300)


def _verify_oidc_token(
    auth_header: str,
    *,
    jwks_url: str,
    issuer: str,
    audience: str | None,
    required_scope: str | None,
) -> None:
    scheme, _, provided = auth_header.partition(" ")
    token = provided.strip()
    if (
        scheme.lower() != "bearer"
        or not token
        or len(token) > 16384
        or any(ch.isspace() for ch in token)
    ):
        raise _OIDCAuthError(401, "UNAUTHORIZED", "Missing or invalid bearer token")
    try:
        signing_key = _jwk_client(jwks_url).get_signing_key_from_jwt(token)
        required_claims = ["exp", "iss", "sub"]
        options: dict[str, object] = {"require": required_claims}
        decode_kwargs: dict[str, object] = {
            "algorithms": ["RS256", "ES256"],
            "issuer": issuer,
            "leeway": 5,
        }
        if audience:
            required_claims.append("aud")
            decode_kwargs["audience"] = audience
        else:
            options["verify_aud"] = False
        decode_kwargs["options"] = options
        claims = jwt.decode(token, signing_key.key, **decode_kwargs)
    except jwt.PyJWKClientConnectionError:
        raise
    except (jwt.PyJWTError, ValueError, TypeError) as exc:
        raise _OIDCAuthError(401, "UNAUTHORIZED", "Invalid or expired bearer token") from exc

    token_use = claims.get("token_use")
    subject = claims.get("sub")
    if (
        (token_use is not None and token_use != "access")
        or not isinstance(subject, str)
        or not subject
    ):
        raise _OIDCAuthError(401, "UNAUTHORIZED", "Invalid bearer token claims")

    if required_scope:
        scopes = claims.get("scope", "")
        if not isinstance(scopes, str) or required_scope not in scopes.split():
            raise _OIDCAuthError(403, "FORBIDDEN", "Missing required scope")


# Hàm tạo HTTP error response
def error_response(
    status_code: int,
    code: str,
    message: str,
    retryable: bool,
    request_id: str,
    details: dict | None = None,
) -> JSONResponse:

    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "retryable": retryable,
                "details": details if details is not None else {},
                "request_id": request_id,
                "correlation_id": request_id,
            }
        },
    )


# Điểm kết nối với RAG Pipeline
@lru_cache(maxsize=1)
def get_pipeline():
    from pipeline import RAGPipeline

    return RAGPipeline()


def _load_auth_token() -> str | None:
    token_file = os.getenv("RAG_AUTH_TOKEN_FILE", "").strip()
    env_name = os.getenv("RAG_ENV", os.getenv("ENVIRONMENT", "")).strip().lower()
    req_raw = os.getenv("RAG_AUTH_REQUIRED", "").strip().lower()
    if req_raw in {"1", "true", "yes"}:
        required = True
    elif req_raw in {"0", "false", "no"}:
        required = False
    else:
        required = bool(token_file) or env_name in {"prod", "production", "shared"}

    if not required and not token_file:
        return None
    if not token_file:
        raise RAGUnavailableError("RAG_AUTH_TOKEN_FILE is required when RAG auth is enabled")
    path = Path(token_file)
    if not path.is_file() or path.stat().st_size > 16384:
        raise RAGUnavailableError("RAG_AUTH_TOKEN_FILE must be a bounded regular file")
    token = path.read_text(encoding="ascii").strip()
    if not token or not token.isprintable() or any(ch.isspace() for ch in token):
        raise RAGUnavailableError("Invalid RAG auth token format")
    return token


# Endpoint tạo Maintenance Brief
@router.post(
    "/briefs",
    response_model=MaintenanceBriefResponse,
    status_code=200,
)
def create_maintenance_brief(
    request: MaintenanceBriefRequest,
    http_request: Request,
):
    request_id = f"req_{uuid4().hex}"

    jwks_url = os.getenv("RAG_OIDC_JWKS_URL", "").strip()
    issuer = os.getenv("RAG_OIDC_ISSUER", "").strip()
    audience = os.getenv("RAG_OIDC_AUDIENCE", "").strip() or None
    required_scope = os.getenv("RAG_OIDC_REQUIRED_SCOPE", "").strip() or None

    if jwks_url or issuer:
        if not (jwks_url and issuer):
            logger.error("Incomplete RAG OIDC auth configuration: request_id=%s", request_id)
            return error_response(
                status_code=503,
                code="RAG_UNAVAILABLE",
                message="RAG authentication configuration unavailable",
                retryable=True,
                request_id=request_id,
            )
        try:
            _verify_oidc_token(
                http_request.headers.get("Authorization", ""),
                jwks_url=jwks_url,
                issuer=issuer,
                audience=audience,
                required_scope=required_scope,
            )
        except jwt.PyJWKClientConnectionError:
            logger.exception("RAG OIDC JWKS unavailable: request_id=%s", request_id)
            return error_response(
                status_code=503,
                code="RAG_UNAVAILABLE",
                message="RAG identity provider unavailable",
                retryable=True,
                request_id=request_id,
            )
        except _OIDCAuthError as exc:
            return error_response(
                status_code=exc.status_code,
                code=exc.code,
                message=exc.message,
                retryable=False,
                request_id=request_id,
            )
    else:
        try:
            expected_token = _load_auth_token()
        except (RAGUnavailableError, OSError, UnicodeDecodeError):
            logger.exception("RAG auth configuration unavailable: request_id=%s", request_id)
            return error_response(
                status_code=503,
                code="RAG_UNAVAILABLE",
                message="RAG authentication configuration unavailable",
                retryable=True,
                request_id=request_id,
            )

        if expected_token is not None:
            auth_header = http_request.headers.get("Authorization", "")
            scheme, _, provided = auth_header.partition(" ")
            if scheme.lower() != "bearer" or not hmac.compare_digest(provided.strip(), expected_token):
                return error_response(
                    status_code=401,
                    code="UNAUTHORIZED",
                    message="Missing or invalid bearer token",
                    retryable=False,
                    request_id=request_id,
                )

    logger.info(
        "Received RAG request: request_id=%s alert_id=%s",
        request_id,
        request.alert.alert_id,
    )

    try:
        # Lấy pipeline
        pipeline = get_pipeline()

        # Chạy RAG Pipeline
        result = pipeline.run(request)

        # Kiểm tra response theo contract
        response = MaintenanceBriefResponse.model_validate(result)

        return response

    except LLMProviderError as error:
        logger.warning(
            "RAG provider unavailable: request_id=%s reason=%s",
            request_id,
            error.reason,
        )

        return error_response(
            status_code=503,
            code="RAG_UNAVAILABLE",
            message="RAG generation provider is unavailable",
            retryable=error.retryable,
            request_id=request_id,
            details={
                "dependency": "groq",
                "reason": error.reason,
                "fallback_message": FALLBACK_MESSAGE,
            },
        )

    except (
        RAGUnavailableError,
        ConnectionError,
        TimeoutError,
        RuntimeError,
    ):
        logger.exception(
            "RAG unavailable: request_id=%s",
            request_id,
        )

        return error_response(
            status_code=503,
            code="RAG_UNAVAILABLE",
            message="RAG service is unavailable",
            retryable=True,
            request_id=request_id,
            details={
                "fallback_message": FALLBACK_MESSAGE,
            },
        )

    except Exception:
        logger.exception(
            "Unexpected RAG error: request_id=%s",
            request_id,
        )

        return error_response(
            status_code=500,
            code="INTERNAL_ERROR",
            message="Internal RAG service error",
            retryable=False,
            request_id=request_id,
            details={
                "fallback_message": FALLBACK_MESSAGE,
            },
        )
