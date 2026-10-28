"""Bootstrap must tolerate propagation, while rejecting incompatible topology."""

from concurrent.futures import Future
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException
from maintai_messaging.admin import create_local_topics
from maintai_messaging.config import KafkaSettings
from maintai_messaging.errors import ConfigurationError

from ..kafka_bootstrap import ReadyTopicAdmin

EXPECTED = {
    "sensor.readings": {"partitions": 2, "retention_ms": 60000, "max_message_bytes": 262144}
}


def snapshot(*, missing=False, count=2, error=None, leader=1, partition_error=None):
    return SimpleNamespace(
        topics={}
        if missing
        else {
            "sensor.readings": SimpleNamespace(
                error=error,
                partitions={
                    i: SimpleNamespace(leader=leader, error=partition_error) for i in range(count)
                },
            )
        }
    )


class MetadataSnapshots:
    def __init__(self, values, *, creation_error=None):
        self.values = iter(values)
        self.last = None
        self.reads = 0
        self.creation_error = creation_error

    def create_topics(self, topics, **kwargs):
        futures = {}
        for topic in topics:
            future = Future()
            if self.creation_error:
                future.set_exception(self.creation_error)
            else:
                future.set_result(None)
            futures[topic.topic] = future
        return futures

    def list_topics(self, **kwargs):
        self.reads += 1
        self.last = next(self.values, self.last)
        if isinstance(self.last, Exception):
            raise self.last
        return self.last


def bootstrap(raw, *, timeout=1, expected=None):
    expected = expected or EXPECTED
    return create_local_topics(
        KafkaSettings(local_mode=True, security_protocol="PLAINTEXT"),
        contracts=SimpleNamespace(topology={"topics": expected}),
        client=ReadyTopicAdmin(raw, expected, timeout=timeout),
    )


@pytest.mark.parametrize(
    "initial",
    [
        snapshot(missing=True),
        snapshot(count=0),
        snapshot(leader=-1),
        snapshot(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
        snapshot(partition_error=KafkaError(KafkaError.LEADER_NOT_AVAILABLE)),
    ],
)
def test_bootstrap_waits_for_visible_topics_and_partition_leaders(initial):
    assert bootstrap(MetadataSnapshots([initial, snapshot()])) == ["sensor.readings"]


@pytest.mark.parametrize(
    "code",
    [
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError._TIMED_OUT,
        KafkaError.REQUEST_TIMED_OUT,
    ],
)
def test_bootstrap_retries_temporary_metadata_call_failure(code):
    assert bootstrap(MetadataSnapshots([KafkaException(KafkaError(code)), snapshot()])) == [
        "sensor.readings"
    ]


@pytest.mark.parametrize(
    "initial,detail",
    [
        (snapshot(count=1), "expected=2 observed=1"),
        (snapshot(error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)), "AUTHORIZATION_FAILED"),
        (
            snapshot(partition_error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)),
            "partition=0.*AUTHORIZATION_FAILED",
        ),
    ],
)
def test_bootstrap_rejects_permanent_metadata_error_without_waiting(initial, detail):
    raw = MetadataSnapshots([initial, snapshot()])
    with pytest.raises(ConfigurationError, match=detail):
        bootstrap(raw)
    assert raw.reads == 1


def test_bootstrap_does_not_hide_wrong_partition_count_behind_another_pending_topic():
    with pytest.raises(ConfigurationError, match="sensor.readings.*expected=2 observed=1"):
        bootstrap(
            MetadataSnapshots([snapshot(count=1)]),
            expected={"not-visible": EXPECTED["sensor.readings"], **EXPECTED},
        )


def test_metadata_deadline_identifies_the_topic_that_did_not_appear():
    with pytest.raises(TimeoutError, match="sensor.readings.*expected=2.*missing"):
        bootstrap(MetadataSnapshots([snapshot(missing=True)]), timeout=0.01)


def test_metadata_deadline_identifies_transient_call_error():
    with pytest.raises(TimeoutError, match="LEADER_NOT_AVAILABLE"):
        bootstrap(
            MetadataSnapshots([KafkaException(KafkaError(KafkaError.LEADER_NOT_AVAILABLE))]),
            timeout=0.01,
        )


def test_permanent_metadata_call_error_retains_original_exception():
    error = KafkaException(KafkaError(KafkaError.CLUSTER_AUTHORIZATION_FAILED))
    raw = MetadataSnapshots([error, snapshot()])
    with pytest.raises(KafkaException) as caught:
        bootstrap(raw)
    assert caught.value is error
    assert raw.reads == 1


def test_topic_creation_failure_is_not_retried_or_replaced():
    error = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    raw = MetadataSnapshots([snapshot()], creation_error=error)
    with pytest.raises(KafkaException) as caught:
        bootstrap(raw)
    assert caught.value is error
    assert raw.reads == 0


def test_existing_topics_still_require_complete_metadata():
    raw = MetadataSnapshots(
        [snapshot(missing=True), snapshot()],
        creation_error=KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)),
    )
    assert bootstrap(raw) == ["sensor.readings"]
