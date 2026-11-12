"""Bounded keyed Processing. Only the poll thread owns Kafka assignment/commit.

Completion order is independent of commit order. Daemon workers permit a bounded
shutdown after cancellation; the required durable handler fences late SQL writes.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from concurrent.futures import Future
from dataclasses import dataclass
from queue import Empty, Queue

from confluent_kafka import TopicPartition

from .config import KeyedSettings
from .consumer import ConsumerRunner, DurableReceipt
from .errors import ConfigurationError, ConsumerFailed, LeaseLost
from .metrics import observe_safely
from .ordering import WorkContext


class DaemonExecutor:
    def __init__(self, count):
        self.queue = Queue(maxsize=count)
        self.threads = [
            threading.Thread(target=self._work, daemon=True, name=f"maintai-keyed-{i}")
            for i in range(count)
        ]
        for thread in self.threads:
            thread.start()

    def _work(self):
        while (item := self.queue.get()) is not None:
            future, callback, args = item
            if future.set_running_or_notify_cancel():
                try:
                    future.set_result(callback(*args))
                except BaseException as exc:
                    future.set_exception(exc)

    def submit(self, callback, *args):
        future = Future()
        self.queue.put_nowait((future, callback, args))
        return future

    def close(self, timeout):
        while True:
            try:
                item = self.queue.get_nowait()
                if item is not None:
                    item[0].cancel()
            except Empty:
                break
        for _ in self.threads:
            self.queue.put_nowait(None)
        deadline = time.monotonic() + timeout
        for thread in self.threads:
            thread.join(max(0, deadline - time.monotonic()))


@dataclass
class Entry:
    record: object
    event: object
    assignment: object
    admitted_at: float
    size: int
    done: bool = False

    @property
    def key(self):
        return self.event.key


class OffsetCoordinator:
    """Ordered ledger of delivered records (offset gaps are allowed)."""

    def __init__(self):
        self.partitions = defaultdict(deque)

    def admit(self, entry):
        key = (entry.record.topic, entry.record.partition)
        queue = self.partitions[key]
        if queue and entry.record.offset <= queue[-1].record.offset:
            raise ConsumerFailed("Offsets went backwards inside an assignment")
        queue.append(entry)

    def candidates(self):
        candidates = []
        for (topic, partition), entries in self.partitions.items():
            prefix = []
            for entry in entries:
                if not entry.done:
                    break
                prefix.append(entry)
            if prefix:
                candidates.append(
                    (TopicPartition(topic, partition, prefix[-1].record.offset + 1), prefix)
                )
        return candidates

    def acknowledge(self, partition, entries):
        queue = self.partitions[(partition.topic, partition.partition)]
        for expected in entries:
            if queue.popleft() is not expected:
                raise ConsumerFailed("Offset coordinator prefix changed")

    def drop(self, partitions):
        for key in partitions:
            self.partitions.pop(key, None)

    def entries(self):
        return [entry for queue in self.partitions.values() for entry in queue]


class KeyedConsumerRunner(ConsumerRunner):
    """Processing-only opt-in requiring acquire/release/handle durable fencing."""

    def __init__(self, settings, role, handler, *, limits=None, metrics=None, **kwargs):
        if role != "processing":
            raise ConfigurationError("Keyed mode is Processing-only")
        if any(
            not callable(getattr(handler, name, None)) for name in ("acquire", "release", "handle")
        ):
            raise ConfigurationError("Keyed mode requires a fenced two-phase durable handler")
        self.limits = limits or KeyedSettings()
        # One already-fetched message fits even at the admission watermark.
        self.max_receipt_bytes = settings.max_payload_bytes + 4096
        if self.limits.max_pending_bytes < 2 * self.max_receipt_bytes:
            raise ConfigurationError("Byte budget must reserve room for a fetched record")
        super().__init__(settings, role, handler, **kwargs)
        if handler.consumer_group != self.group_id:
            self.client.close()
            raise ConfigurationError("Handler fence/inbox group must match the Kafka group")
        self.metrics = metrics
        self.tokens = {}
        self.ledger = OffsetCoordinator()
        self.queues = defaultdict(deque)
        self.ready = deque()
        self.active = {}
        self.paused = set()
        self.snapshot = {}

    def _assigned(self, consumer, partitions):
        self._generation += 1
        self.rebalances += 1
        self.tokens.update(self.handler.acquire(partitions))
        consumer.assign(partitions)

    def _revoked(self, _consumer, partitions):
        keys = {(p.topic, p.partition) for p in partitions}
        tokens = [self.tokens.pop(key) for key in keys if key in self.tokens]
        # Cancel locally BEFORE DB I/O; acquire in the new owner increments its
        # durable generation even if the old owner's release loses connectivity.
        for token in tokens:
            token.cancelled.set()
        self.ledger.drop(keys)
        for key, entries in list(self.queues.items()):
            remaining = deque(
                e for e in entries if (e.record.topic, e.record.partition) not in keys
            )
            if remaining:
                self.queues[key] = remaining
            else:
                del self.queues[key]
        self.ready = deque(key for key in self.ready if self.queues.get(key))
        self.paused -= keys
        self.handler.release(tokens)

    def _admit(self, record):
        token = self.tokens.get((record.topic, record.partition))
        if token is None or token.cancelled.is_set():
            return  # Revoked fetched receipt: never ACK it; new owner replays.
        if record.value is None or len(record.value) > self.settings.max_payload_bytes:
            raise ConsumerFailed("Invalid or oversized Processing receipt")
        event = self.contracts.decode(record.topic, record.value)
        if record.key != event.key.encode():
            raise ConsumerFailed("Kafka key differs from machine identity")
        size = (
            len(record.value)
            + len(record.key or b"")
            + 256
            + sum(len(name.encode()) + len(value or b"") for name, value in record.headers)
        )
        if size > self.max_receipt_bytes:
            raise ConsumerFailed("Processing receipt headers/key exceed the bounded reserve")
        pending = self.ledger.entries()
        if len(pending) >= self.limits.max_pending_records or (
            sum(e.size for e in pending) + size > self.limits.max_pending_bytes
        ):
            raise ConsumerFailed("Fetched reserve exhausted; stop safely without skipping")
        entry = Entry(record, event, token, time.monotonic(), size)
        self.ledger.admit(entry)
        queue = self.queues[event.key]
        queue.append(entry)
        if len(queue) == 1 and event.key not in self.active:
            self.ready.append(event.key)

    def _collect(self):
        for key, (future, entry, context) in list(self.active.items()):
            if not future.done():
                if time.monotonic() >= context.deadline:
                    raise ConsumerFailed("Handler deadline exceeded; fence and replay")
                continue
            del self.active[key]
            try:
                receipt = future.result()
            except Exception as exc:
                if not entry.assignment.cancelled.is_set():
                    if self.metrics and isinstance(exc, LeaseLost):
                        observe_safely(lambda: self.metrics.stale.labels(stage="DB_FENCE").inc())
                    raise ConsumerFailed(
                        "Keyed handler failed; no unsafe prefix committed"
                    ) from exc
                if self.metrics:
                    observe_safely(lambda: self.metrics.stale.labels(stage="HANDLER").inc())
            else:
                if entry.assignment.cancelled.is_set():
                    if self.metrics:
                        observe_safely(lambda: self.metrics.stale.labels(stage="HANDLER").inc())
                elif receipt != DurableReceipt.for_record(entry.record):
                    raise ConsumerFailed("Keyed handler did not provide durable receipt")
                else:
                    entry.done = True
            if self.queues.get(key):
                self.ready.append(key)

    def _commit(self):
        for partition, entries in self.ledger.candidates():
            token = self.tokens.get((partition.topic, partition.partition))
            if (
                token is None
                or token.cancelled.is_set()
                or any(e.assignment is not token for e in entries)
            ):
                continue
            result = self.client.commit(offsets=[partition], asynchronous=False)
            if any(getattr(p, "error", None) for p in result or ()):
                raise ConsumerFailed("Offset prefix commit was not acknowledged")
            self.ledger.acknowledge(partition, entries)
            self.processed += len(entries)

    def _dispatch(self, executor):
        while self.ready and len(self.active) < self.limits.max_active_keys:
            key = self.ready.popleft()
            if key in self.active or not self.queues.get(key):
                continue
            entry = self.queues[key].popleft()
            if not self.queues[key]:
                del self.queues[key]
            if entry.assignment.cancelled.is_set():
                continue
            context = WorkContext(
                entry.assignment, time.monotonic() + self.limits.handler_timeout_seconds
            )
            self.active[key] = (
                executor.submit(self.handler.handle, entry.event, entry.record, context),
                entry,
                context,
            )
            if self.metrics:
                observe_safely(
                    lambda entry=entry: self.metrics.queue_wait.observe(
                        time.monotonic() - entry.admitted_at
                    )
                )

    def _backpressure(self, *, draining=False, max_records=None):
        entries = self.ledger.entries()
        count, size = len(entries), sum(e.size for e in entries)
        counts = defaultdict(int)
        for entry in entries:
            counts[entry.key] += 1
        reserve = self.max_receipt_bytes
        for p in self.client.assignment():
            key = (p.topic, p.partition)
            local = self.ledger.partitions[key]
            factor = self.limits.resume_low_watermark if key in self.paused else 1
            blocked = (
                draining
                or (max_records is not None and self.processed + count >= max_records)
                or count >= max(1, self.limits.max_pending_records * factor)
                or (size >= max(0, (self.limits.max_pending_bytes - reserve) * factor))
                or len(local) >= max(1, self.limits.max_uncommitted_per_partition * factor)
                or any(
                    counts[e.key] >= max(1, self.limits.max_pending_per_key * factor) for e in local
                )
            )
            if blocked and key not in self.paused:
                self.client.pause([p])
                self.paused.add(key)
            elif not blocked and key in self.paused:
                self.client.resume([p])
                self.paused.remove(key)
        self.snapshot = {
            "active_keys": len(self.active),
            "pending_records": count,
            "pending_bytes": size,
            "paused_partitions": len(self.paused),
        }
        if self.metrics:
            observe_safely(lambda: self.metrics.update(self, entries, draining))

    def run(self, stop=None, *, max_records=None):
        stop = stop or threading.Event()
        if max_records is not None and max_records < 1:
            raise ConfigurationError("max_records must be positive")
        executor = DaemonExecutor(self.limits.max_active_keys)
        draining_at = None
        try:
            self.client.subscribe(
                [self.topic],
                on_assign=self._assigned,
                on_revoke=self._revoked,
                on_lost=self._revoked,
            )
            while True:
                if stop.is_set() and draining_at is None:
                    draining_at = time.monotonic()
                self._collect()
                self._commit()
                if max_records is not None and self.processed >= max_records:
                    break
                if draining_at is not None:
                    if not self.active:
                        break
                    if time.monotonic() - draining_at >= self.limits.drain_timeout_seconds:
                        raise ConsumerFailed("Drain expired; unfinished receipts will replay")
                else:
                    self._dispatch(executor)
                self._backpressure(draining=draining_at is not None, max_records=max_records)
                record = self._poll(self.settings.poll_interval_seconds)
                if record is not None and draining_at is None:
                    self._admit(record)
        finally:
            tokens = list(self.tokens.values())
            for token in tokens:
                token.cancelled.set()
            try:
                self.handler.release(tokens)
            finally:
                try:
                    self.client.close()
                finally:
                    executor.close(0.1)
                    self.tokens.clear()
                    self.ledger.drop(list(self.ledger.partitions))
                    self.queues.clear()
                    self.active.clear()
                    self.paused.clear()
                    if self.metrics:
                        observe_safely(lambda: self.metrics.update(self, [], True))
        return self.processed
