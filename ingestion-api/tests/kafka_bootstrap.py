"""Test-only metadata boundary for an explicitly isolated Kafka broker."""

import time

from confluent_kafka import KafkaError, KafkaException
from maintai_messaging.errors import ConfigurationError

TRANSIENT_ERRORS = frozenset(
    {
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError._TIMED_OUT,
        KafkaError.REQUEST_TIMED_OUT,
    }
)


def pending_metadata(metadata, expected):
    pending = []
    for name, spec in expected.items():
        topic = metadata.topics.get(name)
        prefix = f"bootstrap: topic={name} expected={spec['partitions']}"
        if topic is None:
            pending.append(f"{prefix} observed=missing")
            continue
        if topic.error:
            detail = f"{prefix} error={topic.error.name()}"
            if topic.error.code() not in TRANSIENT_ERRORS:
                raise ConfigurationError(detail)
            pending.append(detail)
            continue
        count = len(topic.partitions)
        if count == 0:
            pending.append(f"{prefix} observed=0")
            continue
        if count != spec["partitions"]:
            raise ConfigurationError(f"{prefix} observed={count}")
        for partition, state in topic.partitions.items():
            detail = f"{prefix} observed={count} partition={partition} leader={state.leader}"
            if state.error:
                detail += f" error={state.error.name()}"
                if state.error.code() not in TRANSIENT_ERRORS:
                    raise ConfigurationError(detail)
                pending.append(detail)
            elif state.leader < 0:
                pending.append(detail)
    return "; ".join(pending)


class ReadyTopicAdmin:
    """Delegate SDK topic creation; bound the wait for complete metadata to 30s."""

    def __init__(self, client, expected, *, timeout=30):
        self.client, self.expected, self.timeout = client, expected, timeout

    def create_topics(self, *args, **kwargs):
        return self.client.create_topics(*args, **kwargs)

    def list_topics(self, *, timeout):
        deadline = time.monotonic() + self.timeout
        detail = "bootstrap: metadata unavailable"
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"bootstrap metadata deadline: {detail}")
            try:
                metadata = self.client.list_topics(timeout=min(timeout, 5, remaining))
                detail = pending_metadata(metadata, self.expected)
                if not detail:
                    return metadata
            except KafkaException as error:
                code = error.args[0]
                if code.code() not in TRANSIENT_ERRORS:
                    error.add_note("bootstrap: metadata call failed")
                    raise
                detail = f"bootstrap: metadata error={code.name()}"
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
