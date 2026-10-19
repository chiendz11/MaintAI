"""Bounded publish, explicit broker acknowledgement and stable wire identity."""

from __future__ import annotations

import threading
import time
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

    def publish_event(self, event: DecodedEvent) -> Delivery:
        entry = self.contracts.topology["topics"].get(event.topic, {})
        if self.role not in entry.get("publishers", []):
            raise ContractViolation("This publisher role cannot write the selected topic")
        event = self.contracts.decode(event.topic, event.wire_bytes, writer=True)
        if len(event.wire_bytes) > self.settings.max_payload_bytes:
            raise ContractViolation("Kafka payload exceeds the configured bound")
        deadline = time.monotonic() + self.settings.delivery_timeout_ms / 1000
        if not self._lock.acquire(timeout=max(0, deadline - time.monotonic())):
            raise PublishUnavailable("Local producer queue is busy; retain the original event")
        try:
            if self._closed:
                raise PublishUnavailable("Producer is closed")
            result = []

            def delivered(error, message):
                result.append((error, message))

            headers = [
                ("schema_version", event.schema_version.encode()),
                ("message_id", event.message_id.encode()),
                ("correlation_id", event.payload.get("correlation_id", "").encode()),
            ]
            if self.settings.producer_git_sha:
                headers.append(("producer_git_sha", self.settings.producer_git_sha.encode()))
            enqueued = False
            while time.monotonic() < deadline:
                try:
                    self.client.produce(
                        event.topic,
                        value=event.wire_bytes,
                        key=event.key.encode(),
                        headers=headers,
                        on_delivery=delivered,
                    )
                    enqueued = True
                    break
                except BufferError:
                    self.client.poll(min(0.1, max(0, deadline - time.monotonic())))
                except KafkaException as exc:
                    raise PublishUnavailable("Kafka rejected local enqueue") from exc
            if not enqueued:
                raise PublishUnavailable("Producer queue remained full")
            while not result and time.monotonic() < deadline:
                self.client.poll(min(0.1, max(0, deadline - time.monotonic())))
            if not result:
                raise PublishUncertain("Broker ACK not observed; retry the same ID and payload")
            error, message = result[0]
            if error:
                # Transport errors do not establish whether a timed-out record reached Kafka.
                raise PublishUncertain("Kafka delivery was not acknowledged")
            return Delivery(message.topic(), message.partition(), message.offset())
        finally:
            self._lock.release()

    def close(self):
        with self._lock:
            self._closed = True
            if self.client.flush(self.settings.delivery_timeout_ms / 1000):
                raise PublishUncertain("Producer closed with unacknowledged messages")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
