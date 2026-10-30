import io
import threading
import time
from collections import deque
from dataclasses import replace
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from confluent_kafka import TopicPartition
from maintai_messaging.consumer import Record

from maintai_storage.settings import Settings


@pytest.fixture
def settings():
    return replace(
        Settings.from_env(
            {
                "KAFKA_LOCAL_MODE": "true",
                "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
                "S3_RAW_BUCKET": "fixture-bucket",
                "AWS_REGION": "us-east-1",
                "STORAGE_HTTP_HOST": "127.0.0.1",
                "STORAGE_RETRY_INITIAL_SECONDS": "0.01",
                "STORAGE_RETRY_MAX_SECONDS": "0.02",
                "STORAGE_PROBE_INTERVAL_SECONDS": "0.1",
            }
        ),
        port=0,
    )


@pytest.fixture
def record():
    return Record(
        "sensor.readings",
        0,
        7,
        b"machine",
        b'\xff\x00{"feature":"invalid"}',
        (("correlation_id", b"private-id"), ("duplicate", None)),
        123,
    )


def provider_error(code="SlowDown", status=503):
    return ClientError(
        {
            "Error": {"Code": code, "Message": "SECRET_DO_NOT_LOG"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "PutObject",
    )


class ObjectStore:
    """In-memory S3 fixture, not evidence of AWS/MinIO compatibility."""

    def __init__(self):
        self.objects = {}
        self.lock = threading.Lock()
        self.failure = None
        self.fail_suffix = ""
        self.remaining = 0
        self.versioning = "Enabled"
        self.calls = []
        self.closed = False

    def get_bucket_versioning(self, **_args):
        return {"Status": self.versioning}

    def put_object(self, **args):
        with self.lock:
            self.calls.append(args["Key"])
            if self.failure and args["Key"].endswith(self.fail_suffix) and self.remaining:
                self.remaining -= 1
                raise self.failure
            if args["Key"] in self.objects:
                raise provider_error("PreconditionFailed", 412)
            version = f"v{len(self.objects) + 1}" if self.versioning == "Enabled" else None
            self.objects[args["Key"]] = {**args, "VersionId": version}
            return {"VersionId": version}

    def get_object(self, *, Bucket, Key):
        with self.lock:
            obj = self.objects[Key]
            assert Bucket == obj["Bucket"]
            return {"Body": io.BytesIO(obj["Body"]), "VersionId": obj["VersionId"]}

    def close(self):
        self.closed = True


class Message:
    def __init__(self, record):
        self.record = record

    def topic(self):
        return self.record.topic

    def partition(self):
        return self.record.partition

    def offset(self):
        return self.record.offset

    def key(self):
        return self.record.key

    def value(self):
        return self.record.value

    def headers(self):
        return self.record.headers

    def timestamp(self):
        return 1, self.record.timestamp_ms

    def error(self):
        return None


class ConsumerClient:
    def __init__(self, records=(), *, commit_error=None, after_commit=None, during_wait=None):
        self.messages = deque(Message(record) for record in records)
        self.positions = []
        self.partitions = []
        self.callbacks = {}
        self.paused = self.closed = False
        self.polls = 0
        self.commit_error = commit_error
        self.after_commit, self.during_wait = after_commit, during_wait

    def subscribe(self, topics, **callbacks):
        self.topics, self.callbacks = topics, callbacks

    def assign(self, partitions):
        self.partitions = list(partitions)

    def assignment(self):
        return self.partitions

    def pause(self, _partitions):
        self.paused = True

    def resume(self, _partitions):
        self.paused = False

    def poll(self, _timeout):
        self.polls += 1
        if self.polls == 1:
            self.callbacks["on_assign"](self, [TopicPartition(self.topics[0], 0)])
        if (self.paused or not self.messages) and self.during_wait:
            self.during_wait(self)
        if not self.paused and self.messages:
            return self.messages.popleft()
        time.sleep(0.001)
        return None

    def commit(self, *, offsets, asynchronous):
        assert not asynchronous
        if self.commit_error:
            raise self.commit_error
        self.positions.extend(offsets)
        if self.after_commit:
            self.after_commit()
        return offsets

    def close(self):
        self.closed = True


class Admin:
    def __init__(self):
        self.fail = False
        self.topics = {
            name: SimpleNamespace(error=None, partitions={0: object()})
            for name in (
                "sensor.readings",
                "sensor.rejected",
                "inference.results",
            )
        }

    def list_topics(self, *, timeout):
        assert timeout > 0
        if self.fail:
            raise TimeoutError("SECRET_DO_NOT_LOG")
        return SimpleNamespace(topics=self.topics)
