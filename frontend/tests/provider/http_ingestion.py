"""Local HTTP test publisher with signed fixture auth and real Kafka ACKs."""

from datetime import UTC, datetime

import jwt
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import default_contracts
from maintai_messaging.producer import KafkaPublisher


def _signing_key(jwks: dict, kid: str | None):
    for key in jwks.get("keys", []):
        if kid is None or key.get("kid") == kid:
            return jwt.PyJWK.from_dict(key).key
    raise HTTPException(status_code=401, detail="Unknown signing key")


def application(kafka, issuer, jwks, receipts):
    """Build a test-only FastAPI app that publishes sensor readings to Kafka."""
    publisher = KafkaPublisher(kafka, "ingestion")
    contracts = default_contracts()
    app = FastAPI()

    @app.post("/api/v1/sensor-readings")
    async def ingest(request: Request):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing bearer token")
        token = auth.removeprefix("Bearer ").strip()
        header = jwt.get_unverified_header(token)
        jwt.decode(
            token,
            _signing_key(jwks, header.get("kid")),
            algorithms=["RS256"],
            issuer=issuer,
            audience="ingestion",
            options={"require": ["exp", "iss", "aud", "sub"]},
        )
        payload = await request.json()
        payload.setdefault("server_accepted_at", datetime.now(UTC).isoformat())
        ack = publisher.publish("sensor.readings", payload)
        event = contracts.encode("sensor.readings", payload)
        receipts[event.message_id] = (
            event,
            Record(
                "sensor.readings", ack.partition, ack.offset, event.key.encode(), event.wire_bytes
            ),
        )
        return JSONResponse({"status": "ACCEPTED", "event_id": event.message_id}, status_code=202)

    @app.on_event("shutdown")
    def shutdown():
        publisher.close()

    return app
