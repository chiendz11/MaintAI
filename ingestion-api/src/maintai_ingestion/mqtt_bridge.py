"""Single-pod MQTT QoS 1 -> Kafka IngestionGateway micro-batch bridge."""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from uuid import uuid4

import paho.mqtt.client as mqtt
from maintai_messaging.ingestion import BatchIngestItem, SourceIdentity

LOG = logging.getLogger("maintai.ingestion.mqtt")


@dataclass(frozen=True)
class QueuedMqttMessage:
    mid: int
    qos: int
    topic: str
    payload: bytes
    received_at: float


class MqttKafkaBridge:
    """Consumes MQTT QoS 1 readings, publishes batched to Kafka, then ACKs MQTT.

    Never subscribes with $share so per-machine source order arrives on one pod.
    Never ACKs MQTT on retryable Kafka failures (503/504), leaving QoS 1 redelivery intact.
    """

    def __init__(self, settings, gateway, *, client=None):
        if settings.mqtt_topic_filter.startswith("$share/"):
            raise ValueError("Shared subscriptions break per-machine MQTT ingress ordering")
        self.settings = settings
        self.gateway = gateway
        self.identity = SourceIdentity(settings.mqtt_source_subject, settings.mqtt_allowed_machines)
        self._queue: queue.Queue[QueuedMqttMessage | None] = queue.Queue(
            maxsize=settings.mqtt_max_inflight
        )
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._worker: threading.Thread | None = None
        if client is None:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=settings.mqtt_client_id,
                clean_session=False,
                protocol=mqtt.MQTTv311,
                manual_ack=True,
            )
            client.max_inflight_messages_set(settings.mqtt_max_inflight)
            client.reconnect_delay_set(min_delay=1, max_delay=10)
        self.client = client
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.acked_count = 0
        self.deferred_count = 0

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def _on_connect(self, client, _userdata, _flags, reason_code, _properties=None):
        if getattr(reason_code, "is_failure", False) or (
            isinstance(reason_code, int) and reason_code != 0
        ):
            self._connected.clear()
            return
        self._connected.set()
        client.subscribe(self.settings.mqtt_topic_filter, qos=1)

    def _on_disconnect(self, _client, _userdata, _disconnect_flags, _reason_code, _properties=None):
        self._connected.clear()

    def _on_message(self, _client, _userdata, message):
        if self._stop.is_set():
            return
        # Exclude LWT/birth status messages if subscribed broadly.
        if not message.topic.endswith(("/telemetry", "/readings")):
            if message.qos > 0:
                self.client.ack(message.mid, message.qos)
            return
        item = QueuedMqttMessage(
            mid=message.mid,
            qos=message.qos,
            topic=message.topic,
            payload=bytes(message.payload),
            received_at=time.monotonic(),
        )
        try:
            self._queue.put(item, timeout=0.5)
        except queue.Full:
            # Leave un-ACKed so Mosquitto redelivers after inflight drains.
            self.deferred_count += 1
            LOG.warning("MQTT bridge ingress queue full; deferring PUBACK for %s", item.topic)

    def start(self):
        self.client.connect_async(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=30)
        self.client.loop_start()
        self._worker = threading.Thread(
            target=self._run_loop, name="maintai-mqtt-bridge", daemon=True
        )
        self._worker.start()

    def _drain_batch(self) -> list[QueuedMqttMessage]:
        try:
            first = self._queue.get(timeout=0.1)
        except queue.Empty:
            return []
        if first is None:
            return []
        batch = [first]
        deadline = time.monotonic() + (self.settings.mqtt_batch_wait_ms / 1000.0)
        while len(batch) < self.settings.mqtt_batch_size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                try:
                    nxt = self._queue.get_nowait()
                except queue.Empty:
                    break
            else:
                try:
                    nxt = self._queue.get(timeout=remaining)
                except queue.Empty:
                    break
            if nxt is None:
                break
            batch.append(nxt)
        return batch

    def flush_once(self) -> int:
        batch = self._drain_batch()
        if not batch:
            return 0
        return self.process_messages(batch)

    def process_messages(self, batch: list[QueuedMqttMessage]) -> int:
        items = [
            BatchIngestItem(
                raw=msg.payload,
                identity=self.identity,
                request_id=f"mqtt-{msg.mid}-{uuid4().hex[:12]}",
                correlation_id=f"mqtt-{uuid4().hex}",
            )
            for msg in batch
        ]
        responses = self.gateway.handle_batch(items)
        acked = 0
        for msg, resp in zip(batch, responses, strict=True):
            if resp.status in (503, 504):
                # Do NOT ACK MQTT so Mosquitto QoS 1 retains and redelivers the reading.
                self.deferred_count += 1
                continue
            if msg.qos > 0:
                self.client.ack(msg.mid, msg.qos)
            self.acked_count += 1
            acked += 1
        return acked

    def _run_loop(self):
        while not self._stop.is_set():
            try:
                self.flush_once()
            except Exception:
                LOG.exception("Unhandled error in MQTT->Kafka bridge batch loop")
                time.sleep(0.05)

    def stop(self):
        self._stop.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        if self._worker is not None:
            self._worker.join(timeout=5.0)
        try:
            self.client.disconnect()
        except Exception:
            pass
        try:
            self.client.loop_stop()
        except Exception:
            pass
        self._connected.clear()
