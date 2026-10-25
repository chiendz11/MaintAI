"""OIDC access-token verification and least-privilege machine scope."""

import threading
import time
from dataclasses import dataclass

import httpx
import jwt

from simulator.settings import RuntimeSettings


class AuthError(Exception):
    def __init__(self, status: int, code: str):
        self.status, self.code = status, code


@dataclass(frozen=True)
class Principal:
    subject: str
    machines: frozenset[str]

    def allows(self, machine: str) -> bool:
        return "*" in self.machines or machine in self.machines


def claim_at(claims: dict, path: str):
    value = claims
    for part in path.split("."):
        value = value.get(part) if isinstance(value, dict) else None
    return value


class TokenVerifier:
    def __init__(self, settings: RuntimeSettings):
        self.settings = settings
        self.jwks = (
            jwt.PyJWKClient(settings.jwks_url, timeout=5, lifespan=300)
            if settings.jwks_url
            else None
        )

    def verify(self, authorization: str | None, peer: str | None) -> Principal:
        if self.settings.auth_mode == "local":
            from simulator.settings import loopback

            if not loopback(peer):
                raise AuthError(403, "FORBIDDEN")
            return Principal("local-operator", frozenset({"*"}))
        if not authorization or not authorization.startswith("Bearer "):
            raise AuthError(401, "UNAUTHORIZED")
        token = authorization[7:]
        if not token or len(token) > 16384:
            raise AuthError(401, "UNAUTHORIZED")
        try:
            key = self.jwks.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                key.key,
                algorithms=["RS256", "ES256"],
                issuer=self.settings.jwt_issuer,
                audience=self.settings.jwt_audience,
                options={"require": ["exp", "iat", "iss", "aud", "sub"]},
                leeway=5,
            )
        except jwt.PyJWKClientConnectionError as exc:
            raise AuthError(503, "AUTH_UNAVAILABLE") from exc
        except (jwt.PyJWTError, ValueError, TypeError) as exc:
            raise AuthError(401, "UNAUTHORIZED") from exc
        roles = claim_at(claims, self.settings.roles_claim)
        machines = claim_at(claims, self.settings.machines_claim)
        scopes = claims.get("scope", "")
        if not (
            isinstance(roles, list)
            and all(isinstance(x, str) for x in roles)
            and {"SIMULATOR_OPERATOR", "ADMIN"}.intersection(roles)
            and isinstance(machines, list)
            and all(isinstance(x, str) and x for x in machines)
            and isinstance(scopes, str)
            and self.settings.control_scope in scopes.split()
            and isinstance(claims["sub"], str)
            and claims["sub"]
        ):
            raise AuthError(403, "FORBIDDEN")
        return Principal(claims["sub"], frozenset(machines))


class IngestionCredentials:
    """Secret files are read at use; OAuth credentials never enter logs or payloads."""

    def __init__(self, settings: RuntimeSettings):
        self.settings = settings
        self.lock = threading.Lock()
        self.cached_token = ""
        self.expires_at = 0.0

    def __call__(self) -> str | None:
        s = self.settings
        if s.ingestion_auth == "none":
            return None
        try:
            if s.ingestion_auth == "token_file":
                value = s.ingestion_token_file.read_text().strip()
                if not value or any(c.isspace() for c in value):
                    raise ValueError("Invalid credential")
                return value
            with self.lock:
                if time.time() < self.expires_at:
                    return self.cached_token
                with httpx.Client(timeout=5, follow_redirects=False) as client:
                    response = client.post(
                        s.oauth_token_url,
                        auth=(s.oauth_client_id, s.oauth_secret_file.read_text().strip()),
                        data={"grant_type": "client_credentials", "scope": s.oauth_scope},
                    )
                    response.raise_for_status()
                    body = response.json()
                    token, ttl = body["access_token"], float(body["expires_in"])
                    if (
                        body.get("token_type", "").lower() != "bearer"
                        or not isinstance(token, str)
                        or ttl <= 0
                    ):
                        raise ValueError("Invalid OAuth result")
                    self.cached_token = token
                    self.expires_at = time.time() + max(0, ttl - 30)
                    return token
        except (OSError, ValueError, KeyError, TypeError, httpx.HTTPError) as exc:
            raise AuthError(503, "INGESTION_CREDENTIAL_UNAVAILABLE") from exc
