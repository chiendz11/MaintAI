"""Bounded publish, explicit broker acknowledgement and stable wire identity."""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass

from confluent_kafka import KafkaException, Producer

from .config import KafkaSettings
from .contracts import ContractRegistry, DecodedEvent, default_contracts
from .errors import ConfigurationError, ContractViolation, PublishUnavailable, PublishUncertain


@dataclass(frozen=True)
class Delivery:
    topic: str
    partition: int
    offset: int


class KafkaPublisher:
    def __init__(
        self,
        settings: KafkaSettings,
        role: str,
        *,
        contracts: ContractRegistry | None = None,
        client=None,
    ):
        if role not in {"ingestion", "outbox"}:
            raise ConfigurationError("Publisher role must be ingestion or outbox")
        if not settings.local_mode and not settings.producer_git_sha:
            raise ConfigurationError("Production publishers require PRODUCER_GIT_SHA")
        self.settings, self.role = settings, role
        self.contracts = contracts or default_contracts()
        self.client = client or Producer(settings.producer_config())
        self._lock = threading.Lock()
        self._closed = False

    def publish(self, topic: str, payload: dict) -> Delivery:
        return self.publish_event(self.contracts.encode(topic, payload))

    def _prepare_event(self, event: DecodedEvent) -> DecodedEvent:
        entry = self.contracts.topology["topics"].get(event.topic, {})
        if self.role not in entry.get("publishers", []):
            raise ContractViolation("This publisher role cannot write the selected topic")
        decoded = self.contracts.decode(event.topic, event.wire_bytes, writer=True)
        if len(decoded.wire_bytes) > self.settings.max_payload_bytes:
            raise ContractViolation("Kafka payload exceeds the configured bound")
        return decoded

    def _headers(self, event: DecodedEvent) -> list[tuple[str, bytes]]:
        headers = [
            ("schema_version", event.schema_version.encode()),
            ("message_id", event.message_id.encode()),
            ("correlation_id", event.payload.get("correlation_id", "").encode()),
        ]
        if self.settings.producer_git_sha:
            headers.append(("producer_git_sha", self.settings.producer_git_sha.encode()))
        return headers

    def publish_event(self, event: DecodedEvent) -> Delivery:
        result = self.publish_batch([event])[0]
        if isinstance(result, Exception):
            raise result
        return result

    def publish_batch(self, events: Sequence[DecodedEvent]) -> list[Delivery | Exception]:
        if not events:
            return []
        prepared = [self._prepare_event(event) for event in events]
        deadline = time.monotonic() + self.settings.delivery_timeout_ms / 1000
        callbacks: list[list] = [[] for _ in prepared]
        enqueue_errors: list[Exception | None] = [None] * len(prepared)
        enqueued_indices: list[int] = []
        if not self._lock.acquire(timeout=max(0, deadline - time.monotonic())):
            raise PublishUnavailable("Local producer queue is busy; retain the original event")
        try:
            if self._closed:
                raise PublishUnavailable("Producer is closed")
            for idx, item in enumerate(prepared):
                slot = callbacks[idx]

                def delivered(error, message, _slot=slot):
                    _slot.append((error, message))

                headers = self._headers(item)
                enqueued = False
                while time.monotonic() < deadline:
                    try:
                        self.client.produce(
                            item.topic,
                            value=item.wire_bytes,
                            key=item.key.encode(),
                            headers=headers,
                            on_delivery=delivered,
                        )
                        enqueued = True
                        break
                    except BufferError:
                        self.client.poll(min(0.1, max(0, deadline - time.monotonic())))
                    except KafkaException as exc:
                        enqueue_errors[idx] = PublishUnavailable("Kafka rejected local enqueue")
                        enqueue_errors[idx].__cause__ = exc
                        break
                if enqueued:
                    enqueued_indices.append(idx)
                elif enqueue_errors[idx] is None:
                    enqueue_errors[idx] = PublishUnavailable("Producer queue remained full")
        finally:
            self._lock.release()

        # Poll ACKs without holding _lock so concurrent callers can enqueue into librdkafka.
        while (
            enqueued_indices
            and any(not callbacks[idx] for idx in enqueued_indices)
            and time.monotonic() < deadline
        ):
            self.client.poll(min(0.05, max(0, deadline - time.monotonic())))

        results: list[Delivery | Exception] = []
        for idx in range(len(prepared)):
            err = enqueue_errors[idx]
            if err is not None:
                results.append(err)
                continue
            slot = callbacks[idx]
            if not slot:
                results.append(
                    PublishUncertain("Broker ACK not observed; retry the same ID and payload")
                )
                continue
            error, message = slot[0]
            if error:
                # Transport errors do not establish whether a timed-out record reached Kafka.
                results.append(PublishUncertain("Kafka delivery was not acknowledged"))
                continue
            results.append(Delivery(message.topic(), message.partition(), message.offset()))
        return results

    def close(self):
        with self._lock:
            self._closed = True
            if self.client.flush(self.settings.delivery_timeout_ms / 1000):
                raise PublishUncertain("Producer closed with unacknowledged messages")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
