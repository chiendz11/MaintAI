"""Framework-neutral HTTP ingestion boundary; caller supplies verified identity."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote, urlparse

from .contracts import ContractRegistry, canonical_json, default_contracts, strict_json
from .errors import (
    ConfigurationError,
    ContractViolation,
    MalformedJSON,
    PublishUnavailable,
    PublishUncertain,
)


@dataclass(frozen=True)
class SourceIdentity:
    """Construct only after JWT/credential verification and source authorization."""

    subject: str
    allowed_machine_ids: frozenset[str]


@dataclass(frozen=True)
class IngestResponse:
    status: int
    body: dict
    headers: dict[str, str]


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


class IngestionGateway:
    def __init__(
        self, publisher, business_origin: str, *, contracts: ContractRegistry | None = None
    ):
        origin = urlparse(business_origin)
        if (
            origin.scheme not in {"https", "http"}
            or not origin.netloc
            or origin.query
            or origin.fragment
        ):
            raise ConfigurationError("Business origin must be a configured absolute HTTP(S) URL")
        if origin.username or origin.password or origin.path not in {"", "/"}:
            raise ConfigurationError("Business origin must not contain credentials or a path")
        self.publisher = publisher
        self.business_origin = business_origin.rstrip("/")
        self.contracts = contracts or default_contracts()

    def handle(
        self,
        raw: bytes,
        *,
        identity: SourceIdentity | None,
        request_id: str,
        correlation_id: str,
    ) -> IngestResponse:
        def error(status, code, retryable=False):
            return IngestResponse(
                status,
                {"error": {"code": code, "retryable": retryable}, "correlation_id": correlation_id},
                {},
            )

        if identity is None:
            return error(401, "UNAUTHORIZED")
        if not identity.subject or not identity.allowed_machine_ids:
            return error(403, "FORBIDDEN")
        if (
            not request_id
            or not correlation_id
            or max(len(request_id), len(correlation_id), len(identity.subject)) > 256
        ):
            return error(400, "INVALID_REQUEST_IDENTITY")
        if len(raw) > self.publisher.settings.max_payload_bytes:
            return error(413, "PAYLOAD_TOO_LARGE")
        try:
            payload = strict_json(raw)
            issues = self.contracts.request_issues(payload)
            reason = "VALIDATION_FAILED" if issues else None
        except ContractViolation as exc:
            reason = "MALFORMED_JSON" if isinstance(exc, MalformedJSON) else "VALIDATION_FAILED"
            issues = [{"field": "", "code": reason}]
            payload = {}
        if not issues and payload["machine_id"] not in identity.allowed_machine_ids:
            return error(403, "FORBIDDEN")
        if issues:
            digest = hashlib.sha256(raw).hexdigest()
            rejection_id = (
                "rej_"
                + hashlib.sha256(
                    (identity.subject + "\0" + request_id + "\0" + digest).encode()
                ).hexdigest()
            )
            rejected = {
                "schema_version": "1.4.0",
                "rejection_id": rejection_id,
                "source_identity": identity.subject,
                "request_id": request_id,
                "correlation_id": correlation_id,
                "rejected_at": utc_now(),
                "reason": reason,
                "payload_disposition": "HASH_ONLY",
                "payload_sha256": digest,
                "payload_size_bytes": len(raw),
                "validation_errors": issues,
            }
            if len(canonical_json(rejected)) > self.publisher.settings.max_payload_bytes:
                return error(413, "QUARANTINE_METADATA_TOO_LARGE")
            try:
                self.publisher.publish("sensor.rejected", rejected)
            except PublishUncertain:
                return error(504, "KAFKA_PUBLISH_UNCERTAIN", True)
            except PublishUnavailable:
                return error(503, "KAFKA_UNAVAILABLE", True)
            response = error(400 if reason == "MALFORMED_JSON" else 422, reason)
            return IngestResponse(
                response.status, {**response.body, "rejection_id": rejection_id}, {}
            )
        event = {**payload, "correlation_id": correlation_id, "server_accepted_at": utc_now()}
        if len(canonical_json(event)) > self.publisher.settings.max_payload_bytes:
            return error(413, "PAYLOAD_TOO_LARGE")
        try:
            self.publisher.publish("sensor.readings", event)
        except PublishUncertain:
            return error(504, "KAFKA_PUBLISH_UNCERTAIN", True)
        except PublishUnavailable:
            return error(503, "KAFKA_UNAVAILABLE", True)
        url = (
            self.business_origin
            + "/api/v1/sensor-events/"
            + quote(event["event_id"], safe="")
            + "/status"
        )
        return IngestResponse(
            202,
            {
                "schema_version": "1.3.0",
                "event_id": event["event_id"],
                "machine_id": event["machine_id"],
                "ingestion_status": "ACCEPTED",
                "processing_status": "PENDING",
                "server_accepted_at": event["server_accepted_at"],
                "status_url": url,
                "correlation_id": correlation_id,
            },
            {"Location": url},
        )
