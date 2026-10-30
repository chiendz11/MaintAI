"""Business signs verified user scope separately from the service credential."""

import time

import jwt
import pytest
from api.auth import Identity, RegistryFailure
from business_processing_client.delegation import DelegationSigner
from cryptography.hazmat.primitives.asymmetric import rsa


def test_delegation_signs_bounded_verified_actor_and_full_scope():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signer = DelegationSigner(
        key, kid="business-one", issuer="https://business.test", service_subject="business-service"
    )
    actor = Identity(
        "user-one", frozenset({"M-001", "M-002"}), time.time() + 120, frozenset({"TECHNICIAN"})
    )
    value = jwt.decode(
        signer.sign(actor),
        key.public_key(),
        algorithms=["RS256"],
        issuer="https://business.test",
        audience="processing-history",
    )
    assert value["sub"] == actor.subject
    assert value["azp"] == "business-service"
    assert value["machines"] == ["M-001", "M-002"]
    assert value["roles"] == ["TECHNICIAN"]
    assert 0 < value["exp"] - value["iat"] <= 60
    with pytest.raises(RegistryFailure):
        signer.sign(
            Identity("sensor", actor.machines, time.time() + 120, frozenset({"SENSOR_SERVICE"}))
        )
