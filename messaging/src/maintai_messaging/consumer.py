"""Poll-thread ownership, durable receipts, and explicit contiguous offset progress."""

from __future__ import annotations

import hashlib
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaError, TopicPartition

from .config import KafkaSettings
from .contracts import ContractRegistry, default_contracts
from .errors import ConfigurationError, ConsumerFailed


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes | None], ...] = ()
    timestamp_ms: int | None = None

    @classmethod
    def from_message(cls, message):
        timestamp = message.timestamp()[1]
        return cls(
            message.topic(),
            message.partition(),
            message.offset(),
            message.key(),
            message.value(),
            tuple(message.headers() or ()),
            timestamp if timestamp is not None and timestamp >= 0 else None,
        )


@dataclass(frozen=True)
class DurableReceipt:
    """Returned by the DB/S3 adapter only after its transaction or manifest commits."""

    topic: str
    partition: int
    offset: int
    value_sha256: str

    @classmethod
    def for_record(cls, record: Record):
        return cls(
            record.topic,
            record.partition,
            record.offset,
            hashlib.sha256(record.value or b"").hexdigest(),
        )


class ConsumerRunner:
    """One in-flight record per runner; scale with replicas/partitions first.

    Polls and rebalances continue while a bounded handler executes in its own
    thread. No automatic skipping or commit-on-close. Handler failure stops the
    runner and preserves broker redelivery. PostgreSQL/S3 adapters supply receipts.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        role: str,
        handler,
        *,
        contracts: ContractRegistry | None = None,
        client=None,
        max_buffered_records: int = 64,
    ):
        if type(max_buffered_records) is not int or not 2 <= max_buffered_records <= 1024:
            raise ConfigurationError("Serial buffer must contain between 2 and 1024 records")
        self.settings = settings
        self.contracts = contracts or default_contracts()
        profiles = self.contracts.topology["profiles"]
        if role not in profiles:
            raise ConfigurationError("Unknown Kafka consumer role")
        self.role, self.handler = role, handler
        self.topic = profiles[role]["topic"]
        self.group_id = settings.group_prefix + profiles[role]["group_id"]
        self.client = client or Consumer(settings.consumer_config(profiles[role]["group_id"]))
        self._generation = 0
        self._busy = False
        self._backpressured = False
        self._buffer_limit = max_buffered_records
        self._pending = deque()
        self.processed = 0
        self.rebalances = 0

    def _assigned(self, consumer, partitions):
        self._generation += 1
        self.rebalances += 1
        consumer.assign(partitions)
        if self._backpressured:
            consumer.pause(partitions)

    def _revoked(self, _consumer, _partitions):
        self._generation += 1
        self._pending.clear()

    def _execute(self, record):
        if self.role.startswith("storage-"):
            # Archive every Kafka receipt, even if an upstream producer violated schema.
            event = None
        else:
            if record.value is None:
                raise ConsumerFailed("Tombstone on append-only application topic")
            event = self.contracts.decode(record.topic, record.value)
            if record.key != event.key.encode():
                raise ConsumerFailed("Kafka partition key disagrees with event identity")
        receipt = self.handler(event, record)
        if not isinstance(receipt, DurableReceipt) or receipt != DurableReceipt.for_record(record):
            raise ConsumerFailed("Handler did not return this record's durable receipt")
        return receipt

    def _poll(self, timeout):
        message = self.client.poll(timeout)
        if message is None:
            return None
        error = message.error()
        if error:
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            raise ConsumerFailed("Kafka poll failed; offsets remain explicit")
        return Record.from_message(message)

    def _flow_control(self):
        buffered = len(self._pending) + int(self._busy)
        if buffered >= self._buffer_limit and not self._backpressured:
            self._backpressured = True
            assigned = self.client.assignment()
            if assigned:
                self.client.pause(assigned)
        elif self._backpressured and buffered <= self._buffer_limit // 2:
            self._backpressured = False
            assigned = self.client.assignment()
            if assigned:
                self.client.resume(assigned)

    def run(self, stop: threading.Event | None = None, *, max_records: int | None = None):
        stop = stop or threading.Event()
        if max_records is not None and max_records < 1:
            raise ConfigurationError("max_records must be positive")
        self.client.subscribe(
            [self.topic],
            on_assign=self._assigned,
            on_revoke=self._revoked,
            on_lost=self._revoked,
        )
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="maintai-handler")
        future = record = None
        generation = None
        try:
            while True:
                if future is not None:
                    if not future.done():
                        extra = self._poll(self.settings.poll_interval_seconds)
                        if extra is not None:
                            if len(self._pending) + 1 >= self._buffer_limit:
                                raise ConsumerFailed("Serial consumer exceeded its bounded buffer")
                            self._pending.append((self._generation, extra))
                            self._flow_control()
                        continue
                    try:
                        future.result()
                    except Exception as exc:
                        raise ConsumerFailed("Handler failed; no offset advanced") from exc
                    owned = {(p.topic, p.partition) for p in self.client.assignment()}
                    if generation == self._generation and (record.topic, record.partition) in owned:
                        positions = self.client.commit(
                            offsets=[
                                TopicPartition(record.topic, record.partition, record.offset + 1)
                            ],
                            asynchronous=False,
                        )
                        if any(getattr(partition, "error", None) for partition in positions or ()):
                            raise ConsumerFailed("Broker did not acknowledge the offset commit")
                        self.processed += 1
                    self._busy = False
                    future = None
                    self._flow_control()
                if stop.is_set() or (max_records is not None and self.processed >= max_records):
                    break
                if self._pending:
                    pending_generation, record = self._pending.popleft()
                    if pending_generation != self._generation:
                        continue
                else:
                    record = self._poll(self.settings.poll_interval_seconds)
                if record is None:
                    continue
                generation = self._generation
                self._busy = True
                self._flow_control()
                future = executor.submit(self._execute, record)
        finally:
            # Auto commit and auto offset storage are both disabled. A handler
            # still completing during shutdown can only cause safe redelivery.
            self.client.close()
            executor.shutdown(wait=True, cancel_futures=True)
        return self.processed
