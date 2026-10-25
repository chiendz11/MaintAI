"""DB-owner-local inbox/outbox with transaction boundaries and leased publication."""

from __future__ import annotations

import hashlib
import math
import random
import threading
import uuid
from contextlib import contextmanager

from .consumer import DurableReceipt
from .contracts import ContractRegistry, DecodedEvent, default_contracts
from .errors import IdentityConflict, LeaseLost, PublishUnavailable


@contextmanager
def connect(dsn):
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(
        dsn,
        row_factory=dict_row,
        connect_timeout=10,
        options=(
            "-c statement_timeout=15000 -c lock_timeout=5000 "
            "-c idle_in_transaction_session_timeout=60000"
        ),
    ) as connection:
        yield connection


def apply_messaging_schema(connection, contracts: ContractRegistry | None = None):
    """An explicit migration operation, never called by a worker/relay constructor."""
    registry = contracts or default_contracts()
    connection.execute((registry.directory / "migrations/001_messaging.sql").read_text())


class Outbox:
    def __init__(self, contracts: ContractRegistry | None = None):
        self.contracts = contracts or default_contracts()

    def enqueue(self, connection, topic: str, payload: dict):
        if topic not in {"inference.results", "alert.signals"}:
            raise ValueError("The Processing outbox publishes only outcomes and alert signals")
        event = self.contracts.encode(topic, payload)
        digest = hashlib.sha256(event.wire_bytes).hexdigest()
        row = connection.execute(
            """INSERT INTO maintai_kafka_outbox
               (topic, message_id, message_key, payload, payload_sha256)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (topic, message_id) DO NOTHING RETURNING message_id""",
            (topic, event.message_id, event.key, event.wire_bytes, digest),
        ).fetchone()
        if row is None:
            old = connection.execute(
                "SELECT payload_sha256 FROM maintai_kafka_outbox WHERE topic=%s AND message_id=%s",
                (topic, event.message_id),
            ).fetchone()
            if old["payload_sha256"] != digest:
                raise IdentityConflict("Outbox identity has different immutable content")
        return event.message_id


class TransactionalHandler:
    """The application callback writes its rows and outbox using this same connection.

    callback(connection, event, record, outbox) -> None. Receipt is returned only
    after the entire context commits. Network/DQ/ML work can be prepared separately
    by the application; never acknowledge it before durable canonical persistence.
    """

    def __init__(self, connection_factory, consumer_group: str, callback, *, contracts=None):
        self.connection_factory = connection_factory
        self.consumer_group, self.callback = consumer_group, callback
        self.outbox = Outbox(contracts)

    def __call__(self, event: DecodedEvent, record):
        with self.connection_factory() as connection, connection.transaction():
            row = connection.execute(
                """INSERT INTO maintai_kafka_inbox
                   (consumer_group, topic, message_id, identity_sha256,
                    source_partition, source_offset, schema_version)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (consumer_group, topic, message_id) DO NOTHING
                   RETURNING message_id""",
                (
                    self.consumer_group,
                    event.topic,
                    event.message_id,
                    event.identity_sha256,
                    record.partition,
                    record.offset,
                    event.schema_version,
                ),
            ).fetchone()
            if row is None:
                existing = connection.execute(
                    """SELECT identity_sha256 FROM maintai_kafka_inbox
                       WHERE consumer_group=%s AND topic=%s AND message_id=%s""",
                    (self.consumer_group, event.topic, event.message_id),
                ).fetchone()
                if existing["identity_sha256"] != event.identity_sha256:
                    raise IdentityConflict("Inbox identity has different canonical content")
            else:
                self.callback(connection, event, record, self.outbox)
        return DurableReceipt.for_record(record)


class OutboxRelay:
    def __init__(
        self,
        connection_factory,
        publisher,
        *,
        lease_seconds=120,
        batch_size=10,
        poll_interval_seconds=0.25,
    ):
        self.connection_factory, self.publisher = connection_factory, publisher
        self.owner = str(uuid.uuid4())
        if not math.isfinite(lease_seconds) or lease_seconds <= (
            publisher.settings.delivery_timeout_ms / 1000
        ):
            raise ValueError("Outbox lease must exceed the delivery timeout")
        if not 1 <= batch_size <= 100:
            raise ValueError("Outbox batch size must be between 1 and 100")
        if not math.isfinite(poll_interval_seconds) or not 0.01 <= poll_interval_seconds <= 10:
            raise ValueError("Outbox polling interval must be between 0.01 and 10 seconds")
        self.lease_seconds, self.batch_size = lease_seconds, batch_size
        self.poll_interval_seconds = poll_interval_seconds

    def run_once(self):
        with self.connection_factory() as connection, connection.transaction():
            rows = connection.execute(
                """WITH due AS (
                    SELECT topic, message_id FROM maintai_kafka_outbox
                    WHERE (status='PENDING' AND next_attempt_at<=clock_timestamp())
                       OR (status='LEASED' AND lease_until<clock_timestamp())
                    ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED
                ) UPDATE maintai_kafka_outbox AS target
                  SET status='LEASED', lease_owner=%s,
                      lease_until=clock_timestamp()+(%s * interval '1 second'),
                      fencing_token=target.fencing_token+1, attempts=target.attempts+1
                  FROM due WHERE target.topic=due.topic AND target.message_id=due.message_id
                  RETURNING target.*""",
                (self.batch_size, self.owner, self.lease_seconds),
            ).fetchall()
        published = 0
        for row in rows:
            raw = bytes(row["payload"])
            if hashlib.sha256(raw).hexdigest() != row["payload_sha256"]:
                raise IdentityConflict("Outbox payload checksum mismatch")
            event = self.publisher.contracts.decode(row["topic"], raw, writer=True)
            try:
                self.publisher.publish_event(event)
            except PublishUnavailable:
                delay = min(300, 2 ** min(row["attempts"], 8)) + random.random()
                self._finish(row, False, delay)
                continue
            self._finish(row, True, 0)
            published += 1
        return published

    def _finish(self, row, success, delay):
        with self.connection_factory() as connection, connection.transaction():
            self._finish_in_connection(connection, row, success, delay)

    def _finish_in_connection(self, connection, row, success, delay):
        """Allow the ordered relay to atomically finish its row AND key cursor."""
        updated = connection.execute(
            """UPDATE maintai_kafka_outbox
                   SET status=%s, published_at=CASE WHEN %s THEN clock_timestamp() ELSE NULL END,
                       next_attempt_at=clock_timestamp()+(%s * interval '1 second'),
                       lease_owner=NULL, lease_until=NULL, last_error=%s
                   WHERE topic=%s AND message_id=%s AND status='LEASED'
                     AND lease_owner=%s AND fencing_token=%s
                     AND lease_until>=clock_timestamp() RETURNING message_id""",
            (
                "PUBLISHED" if success else "PENDING",
                success,
                delay,
                None if success else "PUBLISH_UNACKNOWLEDGED",
                row["topic"],
                row["message_id"],
                self.owner,
                row["fencing_token"],
            ),
        ).fetchone()
        if updated is None:
            raise LeaseLost("Outbox lease was lost; no stale acknowledgement recorded")

    def run(self, stop: threading.Event):
        while not stop.is_set():
            self.run_once()
            stop.wait(self.poll_interval_seconds)
