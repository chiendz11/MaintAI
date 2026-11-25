"""Nonpublishing Kafka metadata checks and expiring dependency snapshots."""

import time

from confluent_kafka.admin import AdminClient


class KafkaReadinessProbe:
    def __init__(self, settings, timeout):
        self.client = AdminClient(settings.client_config())
        self.timeout = timeout

    def check(self):
        try:
            metadata = self.client.list_topics(timeout=self.timeout)
            return all(
                topic in metadata.topics and metadata.topics[topic].error is None
                for topic in ("sensor.readings", "sensor.rejected")
            )
        except Exception:
            return False


class DependencyState:
    def __init__(self, *, mqtt_enabled: bool = False):
        self.kafka = self.auth_keys = False
        self.mqtt_enabled = mqtt_enabled
        self.mqtt = not mqtt_enabled
        self.checked_at = float("-inf")

    def response(self, *, draining=False):
        fresh = time.monotonic() - self.checked_at <= 10
        checks = {
            "config_loaded": True,
            "kafka": self.kafka and fresh,
            "auth_keys": self.auth_keys and fresh,
        }
        if self.mqtt_enabled:
            checks["mqtt"] = self.mqtt and fresh
        ready = all(checks.values()) and not draining
        return (200 if ready else 503), {"status": "UP" if ready else "DEGRADED", "checks": checks}
