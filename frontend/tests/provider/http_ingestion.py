"""Real HTTP Ingestion wrapper with signed fixture auth and real Kafka ACKs."""

from maintai_ingestion.app import create_app
from maintai_ingestion.auth import OidcIdentityVerifier
from maintai_ingestion.config import AppSettings
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import default_contracts
from maintai_messaging.producer import KafkaPublisher


def application(kafka, issuer, jwks, receipts):
    class CapturePublisher:
        def __init__(self, settings):
            self.settings = settings
            self.publisher = KafkaPublisher(settings, "ingestion")

        def publish(self, topic, payload):
            ack = self.publisher.publish(topic, payload)
            event = default_contracts().encode(topic, payload)
            if topic == "sensor.readings":
                receipts[event.message_id] = (
                    event,
                    Record(topic, ack.partition, ack.offset, event.key.encode(), event.wire_bytes),
                )
            return ack

        def close(self):
            self.publisher.close()

    settings = AppSettings.from_env(
        {
            "INGESTION_ENV": "test",
            "INGESTION_HOST": "127.0.0.1",
            "AUTH_MODE": "oidc",
            "BUSINESS_PUBLIC_ORIGIN": "http://127.0.0.1:18031",
            "OIDC_ISSUER": issuer,
            "OIDC_JWKS_URL": issuer + "/jwks",
            "OIDC_AUDIENCE": "ingestion",
            "OIDC_ROLES_CLAIM": "roles",
            "OIDC_MACHINES_CLAIM": "machines",
            "KAFKA_LOCAL_MODE": "true",
            "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            "KAFKA_BOOTSTRAP_SERVERS": kafka.bootstrap_servers,
        }
    )
    verifier = OidcIdentityVerifier(
        issuer=issuer,
        audience="ingestion",
        algorithms=("RS256",),
        roles_claim="roles",
        machines_claim="machines",
        fetch_jwks=lambda: jwks,
    )
    return create_app(settings, publisher_factory=CapturePublisher, verifier=verifier)
