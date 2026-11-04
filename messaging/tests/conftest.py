import copy
import io
import json
import time
from collections import deque

import pytest
from confluent_kafka import TopicPartition
from maintai_messaging.config import KafkaSettings
from maintai_messaging.contracts import ContractRegistry


@pytest.fixture(scope="session")
def contracts():
    return ContractRegistry()


@pytest.fixture
def example(contracts):
    def load(name):
        return json.loads((contracts.directory / "examples" / f"{name}.json").read_text())

    return load


@pytest.fixture
def settings():
    return KafkaSettings(local_mode=True, security_protocol="PLAINTEXT", delivery_timeout_ms=1000)


class Message:
    def __init__(self, value, *, topic="sensor.readings", key=b"M-001", offset=0, partition=0):
        self._value, self._topic, self._key = value, topic, key
        self._offset, self._partition = offset, partition

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return [("correlation_id", b"fixture")]

    def timestamp(self):
        return (1, 1792368000000)

    def error(self):
        return None


class ProducerClient:
    def __init__(self, *, queue_failures=0, delivery_error=None, never_ack=False):
        self.queue_failures, self.delivery_error, self.never_ack = (
            queue_failures,
            delivery_error,
            never_ack,
        )
        self.calls, self.pending = [], []
        self.flushed = False

    def produce(self, topic, **kwargs):
        if self.queue_failures:
            self.queue_failures -= 1
            raise BufferError("full")
        self.calls.append((topic, kwargs))
        self.pending.append((topic, kwargs))

    def poll(self, _timeout):
        if self.pending and not self.never_ack:
            topic, kwargs = self.pending.pop(0)
            kwargs["on_delivery"](
                self.delivery_error, Message(kwargs["value"], topic=topic, offset=7)
            )
        return 1

    def flush(self, _timeout):
        self.flushed = True
        return len(self.pending)


class ConsumerClient:
    def __init__(self, messages, *, during_wait=None, commit_error=None):
        self.messages, self.during_wait = deque(messages), during_wait
        self.commit_error = commit_error
        self.commits, self.pauses = [], []
        self._assignment, self.callbacks = [], {}
        self.paused = self.closed = False
        self.poll_count = 0

    def subscribe(self, topics, **callbacks):
        self.topics, self.callbacks = topics, callbacks

    def assign(self, partitions):
        self._assignment = list(partitions)

    def assignment(self):
        return self._assignment

    def pause(self, partitions):
        self.paused = True
        self.pauses.append(list(partitions))

    def resume(self, _partitions):
        self.paused = False

    def poll(self, _timeout):
        self.poll_count += 1
        if not self._assignment and self.poll_count == 1:
            self.callbacks["on_assign"](self, [TopicPartition(self.topics[0], 0)])
        if self.paused or not self.messages:
            if self.during_wait:
                self.during_wait(self)
            time.sleep(0.001)
            return None
        if self.messages:
            return self.messages.popleft()
        time.sleep(0.001)
        return None

    def commit(self, *, offsets, asynchronous):
        assert asynchronous is False
        if self.commit_error:
            raise self.commit_error
        self.commits.extend(offsets)
        return offsets

    def close(self):
        self.closed = True


class ConditionalWriteError(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class ObjectStore:
    def __init__(self, *, versioned=True, fail_manifest=False):
        self.objects = {}
        self.versioned, self.fail_manifest = versioned, fail_manifest

    def put_object(self, **kwargs):
        assert kwargs["IfNoneMatch"] == "*"
        key = kwargs["Key"]
        if self.fail_manifest and key.endswith(".manifest.json"):
            raise OSError("S3 disconnected")
        if key in self.objects:
            raise ConditionalWriteError()
        self.objects[key] = copy.deepcopy(kwargs)
        self.objects[key]["VersionId"] = f"version-{len(self.objects)}" if self.versioned else None
        return {"VersionId": self.objects[key]["VersionId"]}

    def get_object(self, *, Bucket, Key):
        value = self.objects[Key]
        assert Bucket == value["Bucket"]
        return {"Body": io.BytesIO(value["Body"]), "VersionId": value["VersionId"]}
