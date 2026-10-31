"""Short user delegation for Processing; browser credentials never leave Business."""

import time
from uuid import uuid4

import jwt
from api.auth import RegistryFailure
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key


class DelegationSigner:
    def __init__(self, key, *, kid, issuer, service_subject):
        if isinstance(key, bytes):
            key = load_pem_private_key(key, password=None)
        if not isinstance(key, RSAPrivateKey) or key.key_size < 2048:
            raise ValueError("Explicit RSA private key of at least 2048 bits is required")
        if any(
            not isinstance(v, str) or not v or len(v) > 256 for v in (kid, issuer, service_subject)
        ):
            raise ValueError("Explicit delegation identity and key version are required")
        self.key, self.kid, self.issuer = key, kid, issuer
        self.service_subject = service_subject

    def sign(self, actor):
        now = int(time.time())
        roles = actor.roles.intersection({"VIEWER", "TECHNICIAN", "ADMIN"})
        expires = min(now + 60, int(actor.expires_at or 0))
        if not roles or expires <= now or not actor.machines:
            raise RegistryFailure(401 if expires <= now else 403, "UNAUTHORIZED")
        return jwt.encode(
            {
                "iss": self.issuer,
                "aud": "processing-history",
                "sub": actor.subject,
                "azp": self.service_subject,
                "iat": now,
                "nbf": now,
                "exp": expires,
                "jti": uuid4().hex,
                "roles": sorted(roles),
                "machines": sorted(actor.machines),
            },
            self.key,
            algorithm="RS256",
            headers={"kid": self.kid},
        )

    def jwks(self):
        value = jwt.algorithms.RSAAlgorithm.to_jwk(self.key.public_key(), as_dict=True)
        return {"keys": [{**value, "kid": self.kid, "alg": "RS256", "use": "sig"}]}
