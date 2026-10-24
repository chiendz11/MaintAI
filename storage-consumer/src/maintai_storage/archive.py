"""Bounded retries around the SDK's immutable raw microbatch writer."""

from __future__ import annotations

import json
import logging
import random
import threading
import time

from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from maintai_messaging.errors import ConfigurationError, IdentityConflict

LOGGER = logging.getLogger("maintai.storage")


class ShutdownRequested(Exception):
    """No receipt: the broker must redeliver the interrupted record."""


def classify(exc):
    if isinstance(exc, (ConnectTimeoutError, ReadTimeoutError, TimeoutError)):
        return True, "TIMEOUT"
    if isinstance(exc, (EndpointConnectionError, ConnectionClosedError, ConnectionError)):
        return True, "UNAVAILABLE"
    if isinstance(exc, IdentityConflict):
        return False, "CONFLICT"
    if isinstance(exc, ClientError):
        response = exc.response
        code = response.get("Error", {}).get("Code", "")
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if code in {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken"}:
            return False, "AUTH"
        if (
            status in {408, 409, 429}
            or 500 <= status <= 599
            or code
            in {
                "SlowDown",
                "RequestTimeout",
                "RequestTimeoutException",
                "InternalError",
                "ServiceUnavailable",
                "ConditionalRequestConflict",
            }
        ):
            return True, "UNAVAILABLE"
        return False, "OTHER"
    if isinstance(exc, ConfigurationError):
        return False, "INVALID_RESPONSE"
    return False, "OTHER"


class RetryingArchive:
    def __init__(
        self,
        archive,
        role,
        settings,
        state,
        metrics,
        stop: threading.Event,
        *,
        clock=time.monotonic,
        jitter=random.uniform,
    ):
        self.archive, self.role, self.settings = archive, role, settings
        self.state, self.metrics, self.stop = state, metrics, stop
        self.clock, self.jitter = clock, jitter

    def __call__(self, event, record):
        if self.stop.is_set():
            raise ShutdownRequested()
        for attempt in range(1, self.settings.retry_attempts + 1):
            if self.stop.is_set():
                raise ShutdownRequested()
            started = self.clock()
            try:
                receipt = self.archive(event, record)
            except Exception as exc:
                self.metrics.write(self.clock() - started, "ERROR")
                retryable, error_type = classify(exc)
                self.metrics.dependency_error("s3", error_type)
                self.state.dependency("s3", False)
                retry = retryable and attempt < self.settings.retry_attempts
                self.metrics.batch("RETRY" if retry else "FAILED")
                LOGGER.warning(
                    json.dumps(
                        {
                            "event": "archive_retry" if retry else "archive_failed",
                            "role": self.role,
                            "topic": record.topic,
                            "partition": record.partition,
                            "offset": record.offset,
                            "attempt": attempt,
                            "error_type": error_type,
                        }
                    )
                )
                if not retry:
                    raise
                ceiling = min(
                    self.settings.retry_max_seconds,
                    self.settings.retry_initial_seconds * 2 ** (attempt - 1),
                )
                if self.stop.wait(self.jitter(ceiling / 2, ceiling)):
                    raise ShutdownRequested() from None
            else:
                self.metrics.write(self.clock() - started, "SUCCESS")
                self.metrics.batch("COMMITTED")
                # COMMITTED is durable archive, not a Kafka commit or unique event count.
                LOGGER.info(
                    json.dumps(
                        {
                            "event": "archive_durable",
                            "role": self.role,
                            "topic": record.topic,
                            "partition": record.partition,
                            "offset": record.offset,
                        }
                    )
                )
                return receipt
