"""B1 ingestion orchestration; persistence and downstream processing are ports."""

from contextlib import AbstractAsyncContextManager
from copy import deepcopy
from dataclasses import dataclass
import json
from typing import Any, Literal, Protocol

from app.schemas.errors import ApiError
from app.schemas.sensor import (
    SensorCompletedResponse,
    SensorDependencyFailure,
    SensorInvalidResponse,
    SensorReadingRequest,
    ValidationIssue,
)
from app.services.data_quality import VALIDATOR_VERSION, DataQualityValidator


class EventConflict(Exception):
    pass


class MachineNotFound(Exception):
    pass


class PendingEvent(Exception):
    """A previous attempt was recorded without a final outcome."""


class MLUnavailable(Exception):
    pass


class MLTimeout(Exception):
    pass


@dataclass(frozen=True)
class HttpOutcome:
    status_code: int
    body: dict[str, Any]


@dataclass
class StoredEvent:
    event_id: str
    machine_id: str
    canonical_payload: str
    raw_payload: dict[str, Any]
    validation_status: str
    validation_errors: list[dict[str, str]]
    validator_version: str
    correlation_id: str
    outcome: HttpOutcome | None = None


class SensorEventStore(Protocol):
    """Implement with an atomic, cross-worker lock and durable writes in production."""

    def lock(self, event_id: str) -> AbstractAsyncContextManager[Any]: ...

    async def get(self, event_id: str) -> StoredEvent | None: ...

    async def machine_exists(self, machine_id: str) -> bool: ...

    async def record(self, event: StoredEvent) -> None: ...

    async def save_outcome(self, event_id: str, outcome: HttpOutcome) -> None: ...


class ValidEventProcessor(Protocol):
    """B3+ downstream path. It must never be invoked for invalid events."""

    async def process(
        self, event: SensorReadingRequest, correlation_id: str
    ) -> SensorCompletedResponse | dict[str, Any]: ...


def _canonical_payload(payload: dict[str, Any]) -> str:
    # Sorting object keys makes wire-order changes idempotent. The contract does
    # not yet prescribe an external canonicalization algorithm.
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class SensorIngestionService:
    def __init__(
        self,
        store: SensorEventStore,
        processor: ValidEventProcessor,
        validator: DataQualityValidator | None = None,
    ):
        self.store = store
        self.processor = processor
        self.validator = validator or DataQualityValidator()

    async def ingest(
        self, event: SensorReadingRequest, correlation_id: str, request_id: str
    ) -> HttpOutcome:
        raw = event.model_dump(mode="json")
        canonical = _canonical_payload(raw)
        async with self.store.lock(event.event_id):
            previous = await self._existing(event.event_id, canonical, correlation_id, request_id)
            if previous is not None:
                return previous
            if not await self.store.machine_exists(event.machine_id):
                raise MachineNotFound(event.machine_id)

            validation = self.validator.validate(event)
            if validation.status == "INVALID":
                return await self._record_invalid(
                    raw, event.event_id, event.machine_id, canonical,
                    list(validation.errors), validation.validator_version, correlation_id,
                )

            await self.store.record(
                StoredEvent(
                    event_id=event.event_id,
                    machine_id=event.machine_id,
                    canonical_payload=canonical,
                    raw_payload=raw,
                    validation_status="VALID",
                    validation_errors=[],
                    validator_version=validation.validator_version,
                    correlation_id=correlation_id,
                )
            )
            try:
                processed = await self.processor.process(event, correlation_id)
            except MLTimeout:
                outcome = self._dependency_failure(
                    event, "ML_TIMEOUT", 504, correlation_id, request_id
                )
            except MLUnavailable:
                outcome = self._dependency_failure(
                    event, "ML_UNAVAILABLE", 503, correlation_id, request_id
                )
            else:
                completed = SensorCompletedResponse.model_validate(processed)
                if (
                    completed.event_id != event.event_id
                    or completed.machine_id != event.machine_id
                    or completed.correlation_id != correlation_id
                ):
                    raise ValueError("downstream response identity mismatch")
                outcome = HttpOutcome(200, completed.model_dump())

            await self.store.save_outcome(event.event_id, outcome)
            return outcome

    async def reject_invalid(
        self,
        raw: dict[str, Any],
        issues: list[ValidationIssue],
        correlation_id: str,
        request_id: str,
    ) -> HttpOutcome:
        event_id = raw["event_id"]
        machine_id = raw["machine_id"]
        canonical = _canonical_payload(raw)
        async with self.store.lock(event_id):
            previous = await self._existing(event_id, canonical, correlation_id, request_id)
            if previous is not None:
                return previous
            if not await self.store.machine_exists(machine_id):
                raise MachineNotFound(machine_id)
            return await self._record_invalid(
                raw, event_id, machine_id, canonical, issues,
                VALIDATOR_VERSION, correlation_id,
            )

    async def _existing(
        self, event_id: str, canonical: str, correlation_id: str, request_id: str
    ) -> HttpOutcome | None:
        stored = await self.store.get(event_id)
        if stored is None:
            return None
        if stored.canonical_payload != canonical:
            raise EventConflict(event_id)
        if stored.outcome is None:
            # Do not repeat inference after an interrupted write; recovery needs
            # a durable store/downstream reconciliation implementation.
            raise PendingEvent(event_id)
        # The stored result belongs to the first attempt; trace fields in this
        # response must describe the current HTTP request instead.
        body = deepcopy(stored.outcome.body)
        body["correlation_id"] = correlation_id
        if isinstance(body.get("error"), dict):
            body["error"]["correlation_id"] = correlation_id
            body["error"]["request_id"] = request_id
        return HttpOutcome(stored.outcome.status_code, body)

    async def _record_invalid(
        self,
        raw: dict[str, Any],
        event_id: str,
        machine_id: str,
        canonical: str,
        issues: list[ValidationIssue],
        validator_version: str,
        correlation_id: str,
    ) -> HttpOutcome:
        response = SensorInvalidResponse(
            event_id=event_id,
            machine_id=machine_id,
            validation_status="INVALID",
            machine_state="INVALID",
            errors=issues,
            prediction_id=None,
            alert_id=None,
            correlation_id=correlation_id,
        )
        outcome = HttpOutcome(422, response.model_dump())
        await self.store.record(
            StoredEvent(
                event_id=event_id,
                machine_id=machine_id,
                canonical_payload=canonical,
                raw_payload=raw,
                validation_status="INVALID",
                validation_errors=[issue.model_dump() for issue in issues],
                validator_version=validator_version,
                correlation_id=correlation_id,
            )
        )
        await self.store.save_outcome(event_id, outcome)
        return outcome

    @staticmethod
    def _dependency_failure(
        event: SensorReadingRequest,
        code: Literal["ML_UNAVAILABLE", "ML_TIMEOUT"],
        status_code: int,
        correlation_id: str,
        request_id: str,
    ) -> HttpOutcome:
        error = ApiError(
            code=code,
            message="ML inference service is unavailable" if code == "ML_UNAVAILABLE"
            else "ML inference timed out",
            retryable=True,
            details={},
            request_id=request_id,
            correlation_id=correlation_id,
        )
        response = SensorDependencyFailure(
            error=error,
            event_id=event.event_id,
            machine_id=event.machine_id,
            validation_status="VALID",
            processing_status="DEPENDENCY_FAILED",
            machine_state="UNKNOWN",
            prediction_id=None,
            alert_id=None,
            correlation_id=correlation_id,
        )
        return HttpOutcome(status_code, response.model_dump())
