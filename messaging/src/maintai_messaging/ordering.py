"""Processing-owned fences, two-phase handlers and per-key ordered publication.

Migration/activation is always explicit. Prepared network work runs outside SQL;
the final owner transaction fences stale generations and advances source order.
"""

from __future__ import annotations

import hashlib
import random
import threading
import time
import uuid
from dataclasses import dataclass, field

from .consumer import DurableReceipt
from .contracts import canonical_json, default_contracts
from .errors import (
    ConfigurationError,
    ConsumerFailed,
    IdentityConflict,
    LeaseLost,
    PublishUnavailable,
)
from .metrics import observe_safely
from .postgres import OutboxRelay


def apply_processing_ordering_schema(connection, contracts=None):
    registry = contracts or default_contracts()
    connection.execute((registry.directory / "migrations/002_processing_ordering.sql").read_text())


def activate_processing_ordering(connection, *, legacy_writers_drained=False):
    if not legacy_writers_drained:
        raise ConfigurationError("Stop and drain legacy writers/relays before activation")
    state = connection.execute(
        "SELECT enabled FROM maintai_processing_capabilities "
        "WHERE capability='proc-order-1' FOR UPDATE"
    ).fetchone()
    if state is None:
        raise ConfigurationError("Apply Processing ordering migration first")
    if state["enabled"]:
        require_ordering(connection)
        return
    connection.execute(
        "LOCK TABLE maintai_kafka_inbox, maintai_kafka_outbox IN SHARE ROW EXCLUSIVE MODE"
    )
    pending = connection.execute(
        "SELECT 1 FROM maintai_kafka_outbox WHERE status <> 'PUBLISHED' "
        "AND dispatch_seq IS NULL LIMIT 1"
    ).fetchone()
    if pending:
        raise ConfigurationError(
            "Legacy pending outbox must be drained; timestamp backfill is unsafe"
        )
    if connection.execute(
        "SELECT 1 FROM maintai_kafka_inbox WHERE topic='sensor.readings' LIMIT 1"
    ).fetchone():
        raise ConfigurationError(
            "Legacy source inbox needs an evidence-based cursor bootstrap; no guessed backfill"
        )
    connection.execute(
        "UPDATE maintai_processing_capabilities SET enabled=TRUE WHERE capability='proc-order-1'"
    )
    require_ordering(connection)


def require_ordering(connection):
    row = connection.execute(
        "SELECT enabled FROM maintai_processing_capabilities "
        "WHERE capability='proc-order-1' FOR SHARE"
    ).fetchone()
    if row is None or not row["enabled"]:
        raise ConfigurationError("Processing ordering migration/activation is required")
    connection.execute("SELECT set_config('maintai.processing_writer', 'proc-order-1', TRUE)")


@dataclass(frozen=True)
class Assignment:
    group: str
    topic: str
    partition: int
    owner: str
    generation: int
    cancelled: threading.Event = field(default_factory=threading.Event, compare=False)


@dataclass(frozen=True)
class WorkContext:
    assignment: Assignment
    deadline: float

    def check(self):
        if self.assignment.cancelled.is_set():
            raise LeaseLost("Assignment no longer owns this work")
        if time.monotonic() >= self.deadline:
            raise ConsumerFailed("Processing handler deadline exceeded")


class OrderedOutbox:
    """Bound to one source reading; called inside its fenced owner transaction."""

    def __init__(self, source, contracts):
        self.source, self.contracts = source, contracts
        self.result_count = 0
        self.result_payload = None
        self.signals = []

    def validate(self):
        if self.result_count != 1:
            raise ConsumerFailed("Persist must enqueue exactly one terminal result")
        result = self.result_payload
        if self.signals and result["kind"] != "PREDICTION_COMPLETED":
            raise ConsumerFailed("Invalid/unavailable outcomes cannot emit alert signals")
        for signal in self.signals:
            pairs = {
                "prediction_id": "prediction_id",
                "model_version": "model_version",
                "policy_version": "policy_version",
                "risk_score": "risk_score",
                "severity": "machine_state",
            }
            if result["machine_state"] == "NORMAL" or any(
                signal[name] != result[target] for name, target in pairs.items()
            ):
                raise IdentityConflict("Signal does not match its completed risk decision")

    def enqueue(self, connection, topic, payload):
        if topic not in {"inference.results", "alert.signals"}:
            raise ConfigurationError("Processing outbox only publishes results/signals")
        event = self.contracts.encode(topic, payload)
        payload = event.payload  # Wire snapshot, not the caller's mutable dictionary.
        if event.key != self.source.key or payload["event_id"] != self.source.message_id:
            raise IdentityConflict("Outbox does not belong to this source reading")
        seq = self.source.payload["sequence_number"]
        if topic == "inference.results" and payload["sequence_number"] != seq:
            raise IdentityConflict("Result source sequence differs from the reading")
        connection.execute(
            "INSERT INTO maintai_outbox_keys(topic,message_key) VALUES (%s,%s) "
            "ON CONFLICT DO NOTHING",
            (topic, event.key),
        )
        connection.execute(
            "SELECT last_allocated FROM maintai_outbox_keys "
            "WHERE topic=%s AND message_key=%s FOR UPDATE",
            (topic, event.key),
        ).fetchone()
        digest = hashlib.sha256(event.wire_bytes).hexdigest()
        old = connection.execute(
            "SELECT payload_sha256 FROM maintai_kafka_outbox WHERE topic=%s AND message_id=%s",
            (topic, event.message_id),
        ).fetchone()
        if old:
            if old["payload_sha256"] != digest:
                raise IdentityConflict("Outbox identity has different immutable content")
        else:
            order = connection.execute(
                "UPDATE maintai_outbox_keys SET last_allocated=last_allocated+1 "
                "WHERE topic=%s AND message_key=%s RETURNING last_allocated",
                (topic, event.key),
            ).fetchone()["last_allocated"]
            connection.execute(
                "INSERT INTO maintai_kafka_outbox "
                "(topic,message_id,message_key,payload,payload_sha256,dispatch_seq,"
                "source_sequence_number) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (topic, event.message_id, event.key, event.wire_bytes, digest, order, seq),
            )
        if topic == "inference.results":
            self.result_count += 1
            self.result_payload = payload
        else:
            self.signals.append(payload)
        return event.message_id


class KeyedTransactionalHandler:
    """prepare(event,record,context); persist(conn,event,record,outbox,prepared).

    prepare must bound network I/O and honor context cancellation/deadline.
    persist writes DQ/history/risk AND the terminal result outbox in this connection.
    The SDK never substitutes an invented ML result or business callback.
    """

    def __init__(
        self,
        connection_factory,
        consumer_group,
        prepare,
        persist,
        *,
        prepare_batch=None,
        contracts=None,
    ):
        self.connection_factory, self.consumer_group = connection_factory, consumer_group
        self.prepare, self.persist = prepare, persist
        self.prepare_batch = prepare_batch
        self.contracts = contracts or default_contracts()
        self.owner = uuid.uuid4().hex
        with self.connection_factory() as connection:
            require_ordering(connection)

    def acquire(self, partitions):
        tokens = {}
        with self.connection_factory() as connection, connection.transaction():
            require_ordering(connection)
            for p in sorted(partitions, key=lambda p: (p.topic, p.partition)):
                row = connection.execute(
                    "INSERT INTO maintai_processing_assignments "
                    "(consumer_group,topic,source_partition,owner,generation,active) "
                    "VALUES (%s,%s,%s,%s,1,TRUE) "
                    "ON CONFLICT (consumer_group,topic,source_partition) DO UPDATE "
                    "SET owner=EXCLUDED.owner, "
                    "generation=maintai_processing_assignments.generation+1,"
                    " active=TRUE RETURNING generation",
                    (self.consumer_group, p.topic, p.partition, self.owner),
                ).fetchone()
                tokens[(p.topic, p.partition)] = Assignment(
                    self.consumer_group, p.topic, p.partition, self.owner, row["generation"]
                )
        return tokens

    def release(self, tokens):
        for token in tokens:
            token.cancelled.set()
        with self.connection_factory() as connection, connection.transaction():
            for token in tokens:
                connection.execute(
                    "UPDATE maintai_processing_assignments SET active=FALSE "
                    "WHERE consumer_group=%s AND topic=%s AND source_partition=%s "
                    "AND owner=%s AND generation=%s",
                    (token.group, token.topic, token.partition, token.owner, token.generation),
                )

    def _fence(self, connection, context):
        context.check()
        require_ordering(connection)
        token = context.assignment
        row = connection.execute(
            "SELECT owner,generation,active FROM maintai_processing_assignments "
            "WHERE consumer_group=%s AND topic=%s AND source_partition=%s FOR SHARE",
            (token.group, token.topic, token.partition),
        ).fetchone()
        if (
            not row
            or not row["active"]
            or (row["owner"], row["generation"]) != (token.owner, token.generation)
        ):
            raise LeaseLost("Stale Processing DB writer was fenced")
        context.check()

    def _duplicate(self, connection, event):
        row = connection.execute(
            "SELECT identity_sha256 FROM maintai_kafka_inbox "
            "WHERE consumer_group=%s AND topic=%s AND message_id=%s",
            (self.consumer_group, event.topic, event.message_id),
        ).fetchone()
        if row and row["identity_sha256"] != event.identity_sha256:
            raise IdentityConflict("Reading identity has different canonical content")
        return row is not None

    def handle(self, event, record, context):
        if event.topic != "sensor.readings":
            raise ConfigurationError("Keyed Processing only handles sensor.readings")
        source_identity = canonical_json(event.payload)
        token = context.assignment
        if (
            event.wire_bytes != record.value
            or event.topic != record.topic
            or (token.group, token.topic, token.partition)
            != (self.consumer_group, record.topic, record.partition)
        ):
            raise IdentityConflict("Work/fence coordinates do not identify this reading")
        with self.connection_factory() as connection, connection.transaction():
            self._fence(connection, context)
            if self._duplicate(connection, event):
                return DurableReceipt.for_record(record)
        prepared = self.prepare(event, record, context)
        context.check()
        if canonical_json(event.payload) != source_identity:
            raise IdentityConflict("Prepare mutated the immutable source reading")
        with self.connection_factory() as connection, connection.transaction():
            self._fence(connection, context)
            connection.execute(
                "INSERT INTO maintai_processing_machine_cursor "
                "(consumer_group,machine_id,topic,source_partition) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (self.consumer_group, event.key, record.topic, record.partition),
            )
            cursor = connection.execute(
                "SELECT * FROM maintai_processing_machine_cursor "
                "WHERE consumer_group=%s AND machine_id=%s FOR UPDATE",
                (self.consumer_group, event.key),
            ).fetchone()
            # A duplicate may have committed while prepare was running.
            if self._duplicate(connection, event):
                return DurableReceipt.for_record(record)
            sequence = event.payload["sequence_number"]
            if sequence <= cursor["last_sequence"] or (
                cursor["topic"],
                cursor["source_partition"],
            ) != (record.topic, record.partition):
                raise IdentityConflict("Machine source order/partition requires reconciliation")
            connection.execute(
                "INSERT INTO maintai_kafka_inbox "
                "(consumer_group,topic,message_id,identity_sha256,source_partition,source_offset,"
                "schema_version) VALUES (%s,%s,%s,%s,%s,%s,%s)",
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
            self.persist(connection, event, record, outbox, prepared)
            if canonical_json(event.payload) != source_identity:
                raise IdentityConflict("Persist mutated the immutable source reading")
            outbox.validate()
            context.check()
            connection.execute(
                "UPDATE maintai_processing_machine_cursor SET last_sequence=%s,last_event_id=%s "
                "WHERE consumer_group=%s AND machine_id=%s",
                (sequence, event.message_id, self.consumer_group, event.key),
            )
        return DurableReceipt.for_record(record)

    def handle_batch(self, items, context):
        if not items:
            return []
        if len(items) == 1:
            event, record = items[0]
            return [self.handle(event, record, context)]
        token = context.assignment
        source_identities = []
        machine_key = items[0][0].key
        for event, record in items:
            if event.topic != "sensor.readings":
                raise ConfigurationError("Keyed Processing only handles sensor.readings")
            if event.key != machine_key:
                raise IdentityConflict("Per-key micro-batch mixed machine keys")
            if (
                event.wire_bytes != record.value
                or event.topic != record.topic
                or (token.group, token.topic, token.partition)
                != (self.consumer_group, record.topic, record.partition)
            ):
                raise IdentityConflict("Work/fence coordinates do not identify this reading")
            source_identities.append(canonical_json(event.payload))

        pending_indices = []
        pending_items = []
        with self.connection_factory() as connection, connection.transaction():
            self._fence(connection, context)
            for idx, (event, _record) in enumerate(items):
                if not self._duplicate(connection, event):
                    pending_indices.append(idx)
                    pending_items.append((event, _record))
        if not pending_items:
            return [DurableReceipt.for_record(record) for _event, record in items]

        if self.prepare_batch is not None:
            prepared_pending = list(self.prepare_batch(pending_items, context))
        else:
            prepared_pending = [self.prepare(e, r, context) for e, r in pending_items]
        if len(prepared_pending) != len(pending_items):
            raise ConsumerFailed("Batch preparation returned mismatched outcome count")
        prepared_by_idx = dict(zip(pending_indices, prepared_pending, strict=True))
        context.check()
        for (event, _record), source_identity in zip(items, source_identities, strict=True):
            if canonical_json(event.payload) != source_identity:
                raise IdentityConflict("Prepare mutated the immutable source reading")

        with self.connection_factory() as connection, connection.transaction():
            self._fence(connection, context)
            connection.execute(
                "INSERT INTO maintai_processing_machine_cursor "
                "(consumer_group,machine_id,topic,source_partition) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (self.consumer_group, machine_key, items[0][1].topic, items[0][1].partition),
            )
            cursor = connection.execute(
                "SELECT * FROM maintai_processing_machine_cursor "
                "WHERE consumer_group=%s AND machine_id=%s FOR UPDATE",
                (self.consumer_group, machine_key),
            ).fetchone()
            last_sequence = cursor["last_sequence"]
            last_event_id = cursor["last_event_id"]
            cursor_updated = False
            for idx, ((event, record), source_identity) in enumerate(
                zip(items, source_identities, strict=True)
            ):
                if self._duplicate(connection, event):
                    continue
                prepared = prepared_by_idx.get(idx)
                if prepared is None:
                    prepared = self.prepare(event, record, context)
                sequence = event.payload["sequence_number"]
                if sequence <= last_sequence or (
                    cursor["topic"],
                    cursor["source_partition"],
                ) != (record.topic, record.partition):
                    raise IdentityConflict("Machine source order/partition requires reconciliation")
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
                self.persist(connection, event, record, outbox, prepared)
                if canonical_json(event.payload) != source_identity:
                    raise IdentityConflict("Persist mutated the immutable source reading")
                outbox.validate()
                last_sequence = sequence
                last_event_id = event.message_id
                cursor_updated = True
            context.check()
            if cursor_updated:
                connection.execute(
                    "UPDATE maintai_processing_machine_cursor "
                    "SET last_sequence=%s,last_event_id=%s "
                    "WHERE consumer_group=%s AND machine_id=%s",
                    (last_sequence, last_event_id, self.consumer_group, machine_key),
                )
        return [DurableReceipt.for_record(record) for _event, record in items]


class OrderedOutboxRelay(OutboxRelay):
    """Claim one head immediately before each bounded publish, never a later row."""

    def __init__(self, *args, metrics=None, **kwargs):
        self.metrics = metrics
        super().__init__(*args, **kwargs)
        with self.connection_factory() as connection:
            require_ordering(connection)

    def _claim(self):
        rows = self._claim_batch(limit=1)
        return rows[0] if rows else None

    def _claim_batch(self, limit: int | None = None):
        target = min(self.batch_size, 16) if limit is None else min(limit, self.batch_size, 16)
        if target < 1:
            return []
        with self.connection_factory() as connection, connection.transaction():
            require_ordering(connection)
            keys = connection.execute(
                "SELECT k.topic,k.message_key FROM maintai_outbox_keys k "
                "CROSS JOIN LATERAL ("
                "SELECT o.created_at,o.status,o.next_attempt_at,o.lease_until "
                "FROM maintai_kafka_outbox o "
                "WHERE o.topic=k.topic AND o.message_key=k.message_key "
                "AND o.dispatch_seq=k.last_published+1 "
                "AND o.dispatch_seq IS NOT NULL "
                "LIMIT 1"
                ") o "
                "WHERE k.last_published<k.last_allocated "
                "AND (k.owner IS NULL OR k.lease_until<clock_timestamp()) "
                "AND ((o.status='PENDING' AND o.next_attempt_at<=clock_timestamp()) "
                "OR (o.status='LEASED' AND o.lease_until<clock_timestamp())) "
                "ORDER BY o.created_at,k.topic,k.message_key "
                "LIMIT %s FOR UPDATE OF k SKIP LOCKED",
                (target,),
            ).fetchall()
            if not keys:
                return []
            claimed = []
            for key in keys:
                lease = connection.execute(
                    "UPDATE maintai_outbox_keys SET owner=%s,generation=generation+1,"
                    "lease_until=clock_timestamp()+(%s * interval '1 second') "
                    "WHERE topic=%s AND message_key=%s RETURNING *",
                    (self.owner, self.lease_seconds, key["topic"], key["message_key"]),
                ).fetchone()
                row = connection.execute(
                    "UPDATE maintai_kafka_outbox SET status='LEASED',lease_owner=%s,"
                    "lease_until=%s,fencing_token=%s,attempts=attempts+1 "
                    "WHERE topic=%s AND message_key=%s AND dispatch_seq=%s RETURNING *",
                    (
                        self.owner,
                        lease["lease_until"],
                        lease["generation"],
                        key["topic"],
                        key["message_key"],
                        lease["last_published"] + 1,
                    ),
                ).fetchone()
                if row is not None:
                    claimed.append(row)
            return claimed

    def _finish(self, row, success, delay):
        self._finish_batch([(row, success, delay)])

    def _finish_batch(self, items):
        if not items:
            return
        with self.connection_factory() as connection, connection.transaction():
            require_ordering(connection)
            for row, success, delay in items:
                key = connection.execute(
                    "SELECT * FROM maintai_outbox_keys WHERE topic=%s AND message_key=%s "
                    "AND owner=%s AND generation=%s AND lease_until>=clock_timestamp() FOR UPDATE",
                    (row["topic"], row["message_key"], self.owner, row["fencing_token"]),
                ).fetchone()
                if key is None or key["last_published"] + 1 != row["dispatch_seq"]:
                    raise LeaseLost("Ordered key lease was lost; successor remains blocked")
                super()._finish_in_connection(connection, row, success, delay)
                connection.execute(
                    "UPDATE maintai_outbox_keys SET owner=NULL,lease_until=NULL,last_published=%s "
                    "WHERE topic=%s AND message_key=%s",
                    (
                        row["dispatch_seq"] if success else key["last_published"],
                        row["topic"],
                        row["message_key"],
                    ),
                )

    def run_once(self):
        try:
            return self._run_once()
        except (IdentityConflict, LeaseLost) as exc:
            if self.metrics and getattr(self, "current_topic", None):
                reason = "FENCE" if isinstance(exc, LeaseLost) else "IDENTITY"
                observe_safely(
                    lambda: self.metrics.conflicts.labels(
                        topic=self.current_topic, reason=reason
                    ).inc()
                )
            raise

    def _run_once(self):
        published = 0
        remaining = self.batch_size
        while remaining > 0:
            rows = self._claim_batch(limit=remaining)
            if not rows:
                break
            events = []
            for row in rows:
                self.current_topic = row["topic"]
                raw = bytes(row["payload"])
                if hashlib.sha256(raw).hexdigest() != row["payload_sha256"]:
                    raise IdentityConflict("Outbox payload checksum mismatch")
                event = self.publisher.contracts.decode(row["topic"], raw, writer=True)
                if event.key != row["message_key"] or event.message_id != row["message_id"]:
                    raise IdentityConflict("Outbox row and immutable message identity differ")
                events.append(event)
            if hasattr(self.publisher, "publish_batch"):
                try:
                    outcomes = self.publisher.publish_batch(events)
                except PublishUnavailable as exc:
                    outcomes = [exc] * len(rows)
            else:
                outcomes = []
                for event in events:
                    try:
                        outcomes.append(self.publisher.publish_event(event))
                    except PublishUnavailable as exc:
                        outcomes.append(exc)
            finishes = []
            for row, outcome in zip(rows, outcomes, strict=True):
                if isinstance(outcome, PublishUnavailable):
                    delay = min(300, 2 ** min(row["attempts"], 8)) + random.random()
                    finishes.append((row, False, delay))
                elif isinstance(outcome, Exception):
                    raise outcome
                else:
                    finishes.append((row, True, 0))
                    published += 1
            self._finish_batch(finishes)
            remaining -= len(rows)
        return published
