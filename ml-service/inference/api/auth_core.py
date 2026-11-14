"""Xác minh JWT bằng bộ public keys tin cậy do runtime cung cấp."""

import math
from dataclasses import dataclass

import jwt


class AuthFailure(Exception):
    def __init__(self, status, code):
        super().__init__(code)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class ServiceIdentity:
    subject: str


class ServiceTokenVerifier:
    """Không tải URL hoặc nhận key do người gọi API cung cấp."""

    def __init__(self, *, issuer, audience, roles_claim, jwks):
        for name, value in {
            "issuer": issuer,
            "audience": audience,
            "roles_claim": roles_claim,
        }.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Missing auth configuration: {name}")

        if not isinstance(jwks, dict):
            raise ValueError("Invalid JWKS")

        raw_keys = jwks.get("keys")
        if not isinstance(raw_keys, list) or not 1 <= len(raw_keys) <= 128:
            raise ValueError("Invalid JWKS key count")

        keys = {}
        for raw in raw_keys:
            if not isinstance(raw, dict):
                raise ValueError("Invalid JWK")

            # Bỏ qua key thuộc thuật toán/mục đích khác.
            if raw.get("kty") != "RSA":
                continue
            if raw.get("alg", "RS256") != "RS256":
                continue
            if raw.get("use", "sig") != "sig":
                continue

            operations = raw.get("key_ops")
            if operations is not None:
                if (
                    not isinstance(operations, list)
                    or any(not isinstance(item, str) for item in operations)
                    or "verify" not in operations
                ):
                    raise ValueError("Invalid JWK operations")

            kid = raw.get("kid")
            if not isinstance(kid, str) or not 1 <= len(kid) <= 256:
                raise ValueError("Invalid key identifier")
            if kid in keys:
                raise ValueError("Duplicate key identifier")

            # Verifier chỉ nhận public key.
            if any(name in raw for name in ("d", "p", "q", "dp", "dq", "qi")):
                raise ValueError("Private key must not enter verifier")

            try:
                key = jwt.PyJWK.from_dict(raw, algorithm="RS256").key
            except (jwt.PyJWTError, ValueError, TypeError, KeyError):
                raise ValueError("Invalid RSA public key") from None

            if key.key_size < 2048:
                raise ValueError("RSA key must contain at least 2048 bits")

            keys[kid] = key

        if not keys:
            raise ValueError("No usable verification key")

        self.issuer = issuer
        self.audience = audience
        self.roles_claim = roles_claim
        self._keys = keys

    def verify(self, authorization):
        if (
            not isinstance(authorization, str)
            or not authorization
            or len(authorization) > 16384
        ):
            raise AuthFailure(401, "UNAUTHORIZED")

        parts = authorization.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise AuthFailure(401, "UNAUTHORIZED")

        token = parts[1]

        try:
            header = jwt.get_unverified_header(token)
            kid = header.get("kid")

            if (
                header.get("alg") != "RS256"
                or not isinstance(kid, str)
                or not 1 <= len(kid) <= 256
                or kid not in self._keys
                or "crit" in header
            ):
                raise AuthFailure(401, "UNAUTHORIZED")

            claims = jwt.decode(
                token,
                self._keys[kid],
                algorithms=["RS256"],
                issuer=self.issuer,
                audience=self.audience,
                leeway=0,
                options={
                    "require": ["exp", "iss", "aud", "sub"],
                },
            )

            # Không chấp nhận bool, chuỗi hoặc NaN làm thời gian.
            for name in ("exp", "iat", "nbf"):
                if name in claims:
                    value = claims[name]
                    if (
                        type(value) not in (int, float)
                        or not math.isfinite(value)
                    ):
                        raise AuthFailure(401, "UNAUTHORIZED")

            subject = claims["sub"]
            if (
                not isinstance(subject, str)
                or not subject.strip()
                or len(subject) > 256
            ):
                raise AuthFailure(401, "UNAUTHORIZED")

        except (jwt.PyJWTError, ValueError, TypeError, KeyError, OverflowError):
            raise AuthFailure(401, "UNAUTHORIZED") from None

        roles = claims.get(self.roles_claim)
        if (
            not isinstance(roles, list)
            or any(not isinstance(role, str) for role in roles)
            or "PROCESSING_SERVICE" not in roles
        ):
            raise AuthFailure(403, "FORBIDDEN")

        return ServiceIdentity(subject=subject)