"""Durable Processing callback and conflict audit, without direct Kafka publish."""

import hashlib

from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import IdentityConflict
from maintai_messaging.outcomes import OutcomeFactory
from maintai_messaging.postgres import TransactionalHandler
from processing.shared.events.outcomes import completed_outcome
from processing.shared.outbox.adapter import enqueue_outcome_and_signal
from processing.shared.persistence.repository import ProcessingRepository


class RegistryUnavailable(RuntimeError):
    """Cannot classify the event; roll back and leave its offset uncommitted."""


class ProcessingHandler:
    def __init__(
        self,
        connection_factory,
        *,
        consumer_group,
        validator,
        registry_lookup,
        predict,
        identity_resolver,
        policy,
        producer_git_sha,
        registry_cache_seconds=0,
        contracts=None,
        validation_observer=None,
    ):
        self.connection_factory, self.consumer_group = connection_factory, consumer_group
        self.validator, self.registry_lookup, self.predict = validator, registry_lookup, predict
        self.identity_resolver, self.policy = identity_resolver, policy
        self.producer_git_sha = producer_git_sha
        self.validation_observer = validation_observer
        self.contracts = contracts or default_contracts()
        self.repository = ProcessingRepository(self.contracts)
        # Identify the complete rule composition, including the mandatory registry
        # check and configured cache semantics, rather than claiming feature-only DQ.
        self.ruleset_sha256 = hashlib.sha256(
            canonical_json(
                {
                    "feature_ruleset_sha256": validator.ruleset_sha256,
                    "registry_required_for_valid_features": True,
                    "registry_not_found": "MACHINE_NOT_FOUND",
                    "registry_unavailable": "ROLLBACK_WITHOUT_OFFSET_COMMIT",
                    "registry_cache_seconds": registry_cache_seconds,
                }
            )
        ).hexdigest()
        self.transactional = TransactionalHandler(
            connection_factory,
            consumer_group,
            self._apply,
            contracts=self.contracts,
        )

    def __call__(self, event, record):
        try:
            return self.transactional(event, record)
        except IdentityConflict:
            # The SDK may reject before _apply. Its transaction has rolled back;
            # audit in a fresh owner transaction, then keep the offending offset.
            with self.connection_factory() as connection, connection.transaction():
                self.repository.record_conflict(
                    connection, event, record, consumer_group=self.consumer_group
                )
            raise

    def _observe_validation(self, status, reasons=()):
        if self.validation_observer is not None:
            try:
                self.validation_observer(status, reasons)
            except Exception:
                # Metrics are best effort and must not control durable processing.
                pass

    def _apply(self, connection, event, record, outbox):
        factory = OutcomeFactory(
            event,
            record,
            dq_ruleset_version=self.validator.version,
            dq_ruleset_sha256=self.ruleset_sha256,
            producer_git_sha=self.producer_git_sha,
            contracts=self.contracts,
        )
        if not self.repository.claim(
            connection,
            event,
            record,
            ruleset_version=self.validator.version,
            ruleset_sha256=self.ruleset_sha256,
        ):
            return
        dq = self.validator.validate(event.payload["features"])
        risk, inference_ms = None, None
        if dq.status == "INVALID":
            self._observe_validation("INVALID", tuple(issue.code for issue in dq.errors))
            outcome = factory.invalid(
                [{"field": issue.field[:256], "code": issue.code} for issue in dq.errors[:128]]
            )
        else:
            machine = self.registry_lookup(
                event.key, correlation_id=event.payload["correlation_id"]
            )
            if machine.status == "UNAVAILABLE":
                self._observe_validation("UNKNOWN", ("REGISTRY_UNAVAILABLE",))
                raise RegistryUnavailable("Business registry could not validate the machine")
            if machine.status == "NOT_FOUND":
                self._observe_validation("INVALID", ("MACHINE_NOT_FOUND",))
                outcome = factory.invalid([{"field": "machine_id", "code": "MACHINE_NOT_FOUND"}])
            elif machine.status == "FOUND":
                self._observe_validation("VALID")
                result = self.predict(
                    event_id=event.message_id,
                    machine_id=event.key,
                    features=dq.normalized_features,
                    correlation_id=event.payload["correlation_id"],
                )
                if result.response is None:
                    if not result.failure_code or result.attempts not in (1, 2):
                        raise ValueError("ML attempt is missing its typed failure")
                    outcome = factory.unavailable(result.failure_code, result.attempts)
                else:
                    if result.response.event_id != event.message_id:
                        outcome = factory.unavailable("ML_INVALID_RESPONSE", result.attempts)
                    else:
                        identity = self.identity_resolver(result.response)
                        outcome, risk = completed_outcome(
                            factory, result.response, verified_identity=identity, policy=self.policy
                        )
                        inference_ms = result.response.inference_ms
            else:
                self._observe_validation("UNKNOWN", ("REGISTRY_UNAVAILABLE",))
                raise RegistryUnavailable("Registry returned an unknown lookup state")
        self.repository.persist(connection, event, outcome, risk=risk, inference_ms=inference_ms)
        enqueue_outcome_and_signal(connection, outbox, factory, outcome, risk=risk)
