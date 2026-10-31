"""M6 facade uses two server credentials and never retries ambiguous POSTs."""

import asyncio
import time

import httpx
import jwt
import pytest
from api.auth import Identity, RegistryFailure
from business_processing_client.delegation import DelegationSigner
from business_processing_client.history import ProcessingHistoryClient
from cryptography.hazmat.primitives.asymmetric import rsa


def test_history_client_signs_delegation_and_does_not_retry_unknown_post(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signer = DelegationSigner(
        key, kid="one", issuer="https://business.test", service_subject="business-service"
    )
    credential = tmp_path / "service"
    credential.write_text("service-credential")
    actor = Identity("tech", frozenset({"M-001"}), time.time() + 120, frozenset({"TECHNICIAN"}))
    seen = []

    def upstream(request):
        seen.append(request)
        assert request.headers["Authorization"] == "Bearer service-credential"
        assert request.headers["Idempotency-Key"] == "intent-one"
        claims = jwt.decode(
            request.headers["X-MaintAI-History-Delegation"],
            key.public_key(),
            algorithms=["RS256"],
            audience="processing-history",
            issuer="https://business.test",
        )
        assert claims["sub"] == actor.subject
        raise httpx.ReadTimeout("unknown acceptance")

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as http:
            client = ProcessingHistoryClient(
                http, base_url="https://processing.test", token_file=credential, signer=signer
            )
            with pytest.raises(RegistryFailure) as error:
                await client.request(
                    actor,
                    "POST",
                    "history-exports",
                    value={},
                    idempotency_key="intent-one",
                    correlation_id="cor-one",
                )
            assert error.value.status == 504
        assert len(seen) == 1

    asyncio.run(run())
