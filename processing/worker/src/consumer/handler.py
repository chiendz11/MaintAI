"""Durable Processing callback and conflict audit, without direct Kafka publish."""

import hashlib
import time
from dataclasses import dataclass

from maintai_messaging.consumer import DurableReceipt
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import ConfigurationError, IdentityConflict
from maintai_messaging.metrics import observe_safely
from maintai_messaging.ordering import KeyedTransactionalHandler, OrderedOutbox
from maintai_messaging.outcomes import OutcomeFactory
from maintai_messaging.postgres import TransactionalHandler
from processing.shared.events.outcomes import completed_outcome
from processing.shared.outbox.adapter import enqueue_outcome_and_signal
from processing.shared.persistence.repository import ProcessingRepository
from processing.shared.persistence.resequencer import AlertResequencer


class RegistryUnavailable(RuntimeError):
    """Cannot classify the event; roll back and leave its offset uncommitted."""


@dataclass(frozen=True)
class PreparedOutcome:
    outcome: dict
    risk: object = None
    inference_ms: float | None = None
    cached: bool = False
    should_alert: bool = False


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
        hybrid_ordering_enabled=False,
        unavailable_alert_disposition="BLOCKED",
    ):
        self.connection_factory, self.consumer_group = connection_factory, consumer_group
        self.validator, self.registry_lookup, self.predict = validator, registry_lookup, predict
        self.identity_resolver, self.policy = identity_resolver, policy
        self.producer_git_sha = producer_git_sha
        self.validation_observer = validation_observer
        self.contracts = contracts or default_contracts()
        self.hybrid_ordering_enabled = bool(hybrid_ordering_enabled)
        if unavailable_alert_disposition not in {"BLOCKED", "NO_SIGNAL"}:
            raise ValueError("unavailable_alert_disposition must be BLOCKED or NO_SIGNAL")
        self.unavailable_alert_disposition = unavailable_alert_disposition
        self.repository = ProcessingRepository(self.contracts)
        self.resequencer = AlertResequencer()
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
        prepared = self._prepare_business(event, record)
        self.repository.persist(
            connection,
            event,
            prepared.outcome,
            risk=prepared.risk,
            inference_ms=prepared.inference_ms,
        )
        enqueue_outcome_and_signal(
            connection, outbox, factory, prepared.outcome, risk=prepared.risk
        )

    def _factory(self, event, record):
        return OutcomeFactory(
            event,
            record,
            dq_ruleset_version=self.validator.version,
            dq_ruleset_sha256=self.ruleset_sha256,
            producer_git_sha=self.producer_git_sha,
            contracts=self.contracts,
        )

    def _prepare_business(self, event, record):
        factory = self._factory(event, record)
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
        return PreparedOutcome(
            outcome, risk, inference_ms, should_alert=bool(risk and risk.should_create_alert)
        )

    def _terminal_core(self, connection, event, record, *, allow_ledger_signal=False):
        row = connection.execute(
            """SELECT e.payload_hash,e.processing_status,p.outcome,
            p.outcome_sha256,r.should_alert FROM sensor_events e
            LEFT JOIN processing_results p ON p.event_id=e.event_id
            LEFT JOIN risk_decisions r ON r.event_id=e.event_id WHERE e.event_id=%s""",
            (event.message_id,),
        ).fetchone()
        if row is None:
            return None
        if row["payload_hash"] != event.identity_sha256:
            raise IdentityConflict("Event identity has different canonical content")
        if row["outcome"] is None or row["processing_status"] == "PROCESSING":
            raise RuntimeError("Existing event has no durable terminal outcome")
        raw = bytes(row["outcome"])
        if hashlib.sha256(raw).hexdigest() != row["outcome_sha256"]:
            raise IdentityConflict("Stored outcome checksum differs")
        outcome = self.contracts.decode("inference.results", raw, writer=True).payload
        res_encoded = self.contracts.encode("inference.results", outcome)
        res_saved = connection.execute(
            "SELECT payload,payload_sha256 FROM maintai_kafka_outbox "
            "WHERE topic=%s AND message_id=%s",
            ("inference.results", res_encoded.message_id),
        ).fetchone()
        if (
            res_saved is None
            or bytes(res_saved["payload"]) != res_encoded.wire_bytes
            or res_saved["payload_sha256"] != hashlib.sha256(res_encoded.wire_bytes).hexdigest()
        ):
            raise IdentityConflict("Terminal result lacks immutable required outbox evidence")
        if row["should_alert"]:
            signal = self._factory(event, record).alert_signal(outcome)
            if signal is None:
                raise IdentityConflict("Durable alert eligibility has no valid signal")
            sig_encoded = self.contracts.encode("alert.signals", signal)
            sig_sha = hashlib.sha256(sig_encoded.wire_bytes).hexdigest()
            sig_saved = connection.execute(
                "SELECT payload,payload_sha256 FROM maintai_kafka_outbox "
                "WHERE topic=%s AND message_id=%s",
                ("alert.signals", sig_encoded.message_id),
            ).fetchone()
            outbox_ok = (
                sig_saved is not None
                and bytes(sig_saved["payload"]) == sig_encoded.wire_bytes
                and sig_saved["payload_sha256"] == sig_sha
            )
            if not outbox_ok:
                if not allow_ledger_signal:
                    raise IdentityConflict("Terminal result lacks immutable required outbox evidence")
                ledger_sig = connection.execute(
                    "SELECT signal_payload,signal_sha256 FROM processing_alert_ledger "
                    "WHERE topic=%s AND message_id=%s AND signal_id=%s "
                    "AND disposition IN ('SIGNAL_READY','EMITTED') LIMIT 1",
                    (event.topic, event.message_id, sig_encoded.message_id),
                ).fetchone()
                if (
                    ledger_sig is None
                    or ledger_sig["signal_payload"] is None
                    or bytes(ledger_sig["signal_payload"]) != sig_encoded.wire_bytes
                    or ledger_sig["signal_sha256"] != sig_sha
                ):
                    raise IdentityConflict(
                        "Terminal alert eligibility lacks outbox or ledger evidence"
                    )
        return PreparedOutcome(outcome, cached=True, should_alert=bool(row["should_alert"]))

    def _terminal(self, connection, event, record):
        return self._terminal_core(connection, event, record, allow_ledger_signal=False)

    def _terminal_hybrid(self, connection, event, record):
        return self._terminal_core(connection, event, record, allow_ledger_signal=True)

    def prepare(self, event, record, context):
        context.check()
        with self.connection_factory() as connection:
            cached = (
                self._terminal_hybrid(connection, event, record)
                if self.hybrid_ordering_enabled
                else self._terminal(connection, event, record)
            )
        prepared = cached if cached is not None else self._prepare_business(event, record)
        context.check()
        return prepared

    def persist_prepared(self, connection, event, record, outbox, prepared):
        inserted = connection.execute(
            """INSERT INTO processing_source_cursor
            (machine_id,topic,source_partition) VALUES (%s,%s,%s)
            ON CONFLICT DO NOTHING RETURNING machine_id""",
            (event.key, record.topic, record.partition),
        ).fetchone()
        if (
            inserted
            and connection.execute(
                "SELECT 1 FROM sensor_events WHERE machine_id=%s LIMIT 1", (event.key,)
            ).fetchone()
        ):
            raise ConfigurationError(
                "Legacy owner history requires reviewed source cursor bootstrap"
            )
        cursor = connection.execute(
            "SELECT * FROM processing_source_cursor WHERE machine_id=%s FOR UPDATE", (event.key,)
        ).fetchone()
        claimed = self.repository.claim(
            connection,
            event,
            record,
            ruleset_version=self.validator.version,
            ruleset_sha256=self.ruleset_sha256,
        )
        if not claimed:
            # A different group/owner can win while HTTP preparation is running.
            # Reuse its original bytes/IDs/timestamps, not this attempt's candidate.
            prepared = self._terminal(connection, event, record)
        elif prepared.cached:
            raise IdentityConflict("A previously durable owner result disappeared")
        else:
            if event.payload["sequence_number"] <= cursor["last_sequence"] or (
                record.topic,
                record.partition,
            ) != (cursor["topic"], cursor["source_partition"]):
                raise IdentityConflict(
                    "Owner-global source order/partition requires reconciliation"
                )
            self.repository.persist(
                connection,
                event,
                prepared.outcome,
                risk=prepared.risk,
                inference_ms=prepared.inference_ms,
            )
            connection.execute(
                "UPDATE processing_source_cursor SET last_sequence=%s,last_event_id=%s "
                "WHERE machine_id=%s",
                (event.payload["sequence_number"], event.message_id, event.key),
            )
        outbox.enqueue(connection, "inference.results", prepared.outcome)
        if prepared.should_alert:
            signal = self._factory(event, record).alert_signal(prepared.outcome)
            if signal is None:
                raise ValueError("Cached policy requests an invalid signal")
            outbox.enqueue(connection, "alert.signals", signal)

    def persist_hybrid(
        self,
        connection,
        event,
        record,
        outbox,
        prepared,
        *,
        admission,
        admission_cursor=None,
        already_fenced=False,
        unavailable_alert_disposition=None,
    ):
        sequence = event.payload["sequence_number"]
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
            (event.key, str(sequence)),
        )
        conflict_seq = connection.execute(
            "SELECT event_id FROM sensor_events WHERE machine_id=%s AND sequence_number=%s "
            "AND event_id <> %s LIMIT 1",
            (event.key, sequence, event.message_id),
        ).fetchone()
        if conflict_seq is not None:
            raise IdentityConflict("Source sequence already belongs to a different event")
        claimed = self.repository.claim(
            connection,
            event,
            record,
            ruleset_version=self.validator.version,
            ruleset_sha256=self.ruleset_sha256,
        )
        if not claimed:
            prepared = self._terminal_hybrid(connection, event, record)
        elif prepared.cached:
            raise IdentityConflict("A previously durable owner result disappeared")
        else:
            self.repository.persist(
                connection,
                event,
                prepared.outcome,
                risk=prepared.risk,
                inference_ms=prepared.inference_ms,
            )
        disposition = None
        drain_summary = None
        if admission is not None and admission.admission_seq is not None:
            disposition = self.resequencer.record_disposition(
                connection,
                consumer_group=self.consumer_group,
                machine_id=event.key,
                admission_seq=admission.admission_seq,
                outcome=prepared.outcome,
                should_alert=prepared.should_alert,
                factory=self._factory(event, record),
                contracts=self.contracts,
                unavailable_alert_disposition=(
                    unavailable_alert_disposition or self.unavailable_alert_disposition
                ),
            )
            drain_summary = self.resequencer.drain_frontier(
                connection,
                consumer_group=self.consumer_group,
                machine_id=event.key,
                topic=record.topic,
                source_partition=record.partition,
                contracts=self.contracts,
                already_fenced=already_fenced,
                cursor=admission_cursor,
            )
        outbox.enqueue(connection, "inference.results", prepared.outcome)
        return disposition, drain_summary


class KeyedProcessingHandler(KeyedTransactionalHandler):
    """Owner preparation/persistence plugged into the SDK fencing protocol."""

    def __init__(
        self,
        application,
        *,
        on_receipt=None,
        on_error=None,
        hybrid_ordering_enabled=None,
        unavailable_alert_disposition=None,
        metrics=None,
    ):
        self.application, self.on_receipt, self.on_error = application, on_receipt, on_error
        self.hybrid_ordering_enabled = (
            application.hybrid_ordering_enabled
            if hybrid_ordering_enabled is None
            else bool(hybrid_ordering_enabled)
        )
        self.unavailable_alert_disposition = (
            unavailable_alert_disposition or application.unavailable_alert_disposition
        )
        self.metrics = metrics
        super().__init__(
            application.connection_factory,
            application.consumer_group,
            application.prepare,
            application.persist_prepared,
            contracts=application.contracts,
        )

    def _validate_coordinates(self, event, record, context):
        token = context.assignment
        if (
            event.topic != "sensor.readings"
            or event.wire_bytes != record.value
            or event.topic != record.topic
            or record.key != event.key.encode()
            or (token.group, token.topic, token.partition)
            != (self.consumer_group, record.topic, record.partition)
        ):
            raise IdentityConflict("Work/fence coordinates do not identify this reading")

    def admit(self, event, record, context):
        try:
            self._validate_coordinates(event, record, context)
            with self.connection_factory() as connection, connection.transaction():
                self._fence(connection, context)
                ticket = self.application.resequencer.admit(
                    connection,
                    consumer_group=self.consumer_group,
                    event=event,
                    record=record,
                    already_fenced=True,
                )
            return ticket
        except Exception as exc:
            if isinstance(exc, IdentityConflict):
                with self.application.connection_factory() as connection, connection.transaction():
                    self.application.repository.record_conflict(
                        connection, event, record, consumer_group=self.consumer_group
                    )
            if self.on_error:
                observe_safely(self.on_error)
            raise

    def _durable_replay(self, event, record, context, *, allow_ledger_signal=False):
        # Verify exact owner identity BEFORE the SDK's per-group sequence gate.
        # The pinned SDK fence is retained in the same receipt transaction.
        with self.connection_factory() as connection, connection.transaction():
            self._fence(connection, context)
            terminal = (
                self.application._terminal_hybrid(connection, event, record)
                if allow_ledger_signal
                else self.application._terminal(connection, event, record)
            )
            if terminal is None:
                if self._duplicate(connection, event):
                    raise IdentityConflict("SDK receipt has no owner terminal result")
                return None
            if not self._duplicate(connection, event):
                connection.execute(
                    """INSERT INTO maintai_kafka_inbox
                    (consumer_group,topic,message_id,identity_sha256,source_partition,source_offset,schema_version)
                    VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (
                        self.consumer_group,
                        event.topic,
                        event.message_id,
                        event.identity_sha256,
                        record.partition,
                        record.offset,
                        event.schema_version,
                    ),
                )
                self._duplicate(connection, event)
            context.check()
        return DurableReceipt.for_record(record)

    def _handle_hybrid(self, event, record, context):
        source_identity = canonical_json(event.payload)
        ticket = context.admission
        if ticket is None:
            ticket = self.admit(event, record, context)
        if ticket.legacy_terminal:
            receipt = self._durable_replay(event, record, context, allow_ledger_signal=True)
            if receipt is None:
                raise IdentityConflict("Legacy terminal ticket lacks owner evidence")
            return receipt

        if not ticket.replayed and not ticket.legacy_terminal:
            context.check()
            prepared = self.application._prepare_business(event, record)
            context.check()
        else:
            prepared = self.prepare(event, record, context)
            context.check()
        if canonical_json(event.payload) != source_identity:
            raise IdentityConflict("Prepare mutated the immutable source reading")

        persist_started = time.monotonic()
        disposition = None
        with self.connection_factory() as connection, connection.transaction():
            self._fence(connection, context)
            admission_cursor = self.application.resequencer._ensure_admission_cursor(
                connection,
                consumer_group=self.consumer_group,
                machine_id=event.key,
                topic=record.topic,
                partition=record.partition,
            )
            if self._duplicate(connection, event):
                self.application._terminal_hybrid(connection, event, record)
                if ticket.admission_seq is not None:
                    self.application.resequencer.drain_frontier(
                        connection,
                        consumer_group=self.consumer_group,
                        machine_id=event.key,
                        topic=record.topic,
                        source_partition=record.partition,
                        contracts=self.contracts,
                        already_fenced=True,
                        cursor=admission_cursor,
                    )
                context.check()
            else:
                connection.execute(
                    "INSERT INTO maintai_kafka_inbox "
                    "(consumer_group,topic,message_id,identity_sha256,source_partition,"
                    "source_offset,schema_version) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (
                        self.consumer_group,
                        event.topic,
                        event.message_id,
                        event.identity_sha256,
                        record.partition,
                        record.offset,
                        event.schema_version,
                    ),
                )
                outbox = OrderedOutbox(event, self.contracts)
                disposition, _ = self.application.persist_hybrid(
                    connection,
                    event,
                    record,
                    outbox,
                    prepared,
                    admission=ticket,
                    admission_cursor=admission_cursor,
                    already_fenced=True,
                    unavailable_alert_disposition=self.unavailable_alert_disposition,
                )
                if canonical_json(event.payload) != source_identity:
                    raise IdentityConflict("Persist mutated the immutable source reading")
                outbox.validate()
                context.check()

        persist_elapsed = time.monotonic() - persist_started
        if self.metrics:
            if hasattr(self.metrics, "persist_seconds"):
                observe_safely(
                    lambda: self.metrics.persist_seconds.observe(persist_elapsed)
                )
            if disposition and hasattr(self.metrics, "alert_dispositions"):
                observe_safely(
                    lambda d=disposition: self.metrics.alert_dispositions.labels(d).inc()
                )
        return DurableReceipt.for_record(record)

    def handle(self, event, record, context):
        try:
            self._validate_coordinates(event, record, context)
            if self.hybrid_ordering_enabled:
                receipt = self._handle_hybrid(event, record, context)
            else:
                receipt = self._durable_replay(event, record, context)
                if receipt is None:
                    receipt = super().handle(event, record, context)
        except Exception as exc:
            if isinstance(exc, IdentityConflict):
                with self.application.connection_factory() as connection, connection.transaction():
                    self.application.repository.record_conflict(
                        connection, event, record, consumer_group=self.consumer_group
                    )
            if self.on_error:
                observe_safely(self.on_error)
            raise
        if self.on_receipt:
            observe_safely(lambda: self.on_receipt(event, record))
        return receipt
