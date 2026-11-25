"""Framework-neutral HTTP ingestion boundary; caller supplies verified identity."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
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


@dataclass(frozen=True)
class BatchIngestItem:
    raw: bytes
    identity: SourceIdentity | None
    request_id: str
    correlation_id: str


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

    @staticmethod
    def _error(
        status: int, code: str, correlation_id: str, retryable: bool = False
    ) -> IngestResponse:
        return IngestResponse(
            status,
            {"error": {"code": code, "retryable": retryable}, "correlation_id": correlation_id},
            {},
        )

    def _prepare_item(
        self,
        raw: bytes,
        *,
        identity: SourceIdentity | None,
        request_id: str,
        correlation_id: str,
    ):
        if identity is None:
            return self._error(401, "UNAUTHORIZED", correlation_id), None
        if not identity.subject or not identity.allowed_machine_ids:
            return self._error(403, "FORBIDDEN", correlation_id), None
        if (
            not request_id
            or not correlation_id
            or max(len(request_id), len(correlation_id), len(identity.subject)) > 256
        ):
            return self._error(400, "INVALID_REQUEST_IDENTITY", correlation_id), None
        if len(raw) > self.publisher.settings.max_payload_bytes:
            return self._error(413, "PAYLOAD_TOO_LARGE", correlation_id), None
        try:
            payload = strict_json(raw)
            issues = self.contracts.request_issues(payload)
            reason = "VALIDATION_FAILED" if issues else None
        except ContractViolation as exc:
            reason = "MALFORMED_JSON" if isinstance(exc, MalformedJSON) else "VALIDATION_FAILED"
            issues = [{"field": "", "code": reason}]
            payload = {}
        if not issues and payload["machine_id"] not in identity.allowed_machine_ids:
            return self._error(403, "FORBIDDEN", correlation_id), None
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
                return self._error(413, "QUARANTINE_METADATA_TOO_LARGE", correlation_id), None
            status = 400 if reason == "MALFORMED_JSON" else 422
            response = self._error(status, reason, correlation_id)
            success = IngestResponse(
                response.status, {**response.body, "rejection_id": rejection_id}, {}
            )
            return None, ("sensor.rejected", rejected, correlation_id, success)
        event = {**payload, "correlation_id": correlation_id, "server_accepted_at": utc_now()}
        if len(canonical_json(event)) > self.publisher.settings.max_payload_bytes:
            return self._error(413, "PAYLOAD_TOO_LARGE", correlation_id), None
        url = (
            self.business_origin
            + "/api/v1/sensor-events/"
            + quote(event["event_id"], safe="")
            + "/status"
        )
        success = IngestResponse(
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
        return None, ("sensor.readings", event, correlation_id, success)

    def _result_for_publish(
        self, correlation_id: str, success: IngestResponse, outcome
    ) -> IngestResponse:
        if isinstance(outcome, PublishUncertain):
            return self._error(504, "KAFKA_PUBLISH_UNCERTAIN", correlation_id, True)
        if isinstance(outcome, PublishUnavailable):
            return self._error(503, "KAFKA_UNAVAILABLE", correlation_id, True)
        if isinstance(outcome, Exception):
            raise outcome
        return success

    def handle(
        self,
        raw: bytes,
        *,
        identity: SourceIdentity | None,
        request_id: str,
        correlation_id: str,
    ) -> IngestResponse:
        immediate, plan = self._prepare_item(
            raw, identity=identity, request_id=request_id, correlation_id=correlation_id
        )
        if immediate is not None:
            return immediate
        topic, payload, item_corr, success = plan
        try:
            self.publisher.publish(topic, payload)
        except (PublishUncertain, PublishUnavailable) as exc:
            return self._result_for_publish(item_corr, success, exc)
        return success

    def handle_batch(self, items: Sequence[BatchIngestItem]) -> list[IngestResponse]:
        if not items:
            return []
        responses: list[IngestResponse | None] = [None] * len(items)
        pending_indices: list[int] = []
        pending_plans: list[tuple[str, dict, str, IngestResponse]] = []
        for idx, item in enumerate(items):
            immediate, plan = self._prepare_item(
                item.raw,
                identity=item.identity,
                request_id=item.request_id,
                correlation_id=item.correlation_id,
            )
            if immediate is not None:
                responses[idx] = immediate
            else:
                pending_indices.append(idx)
                pending_plans.append(plan)
        if not pending_plans:
            return responses
        if hasattr(self.publisher, "publish_batch"):
            events = [
                self.contracts.encode(topic, payload) for topic, payload, _, _ in pending_plans
            ]
            try:
                outcomes = self.publisher.publish_batch(events)
            except (PublishUncertain, PublishUnavailable) as exc:
                outcomes = [exc] * len(pending_plans)
        else:
            outcomes = []
            for topic, payload, _, _ in pending_plans:
                try:
                    outcomes.append(self.publisher.publish(topic, payload))
                except (PublishUncertain, PublishUnavailable) as exc:
                    outcomes.append(exc)
        for idx, plan, outcome in zip(pending_indices, pending_plans, outcomes, strict=True):
            _, _, item_corr, success = plan
            responses[idx] = self._result_for_publish(item_corr, success, outcome)
        return responses
