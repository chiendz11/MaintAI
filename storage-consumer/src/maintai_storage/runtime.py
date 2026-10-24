"""One process: independent accepted/rejected groups and internal management HTTP."""

from __future__ import annotations

import json
import logging
import threading
import time

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from maintai_messaging.archive import S3RecordArchive
from maintai_messaging.consumer import ConsumerRunner
from maintai_messaging.contracts import ContractRegistry
from maintai_messaging.errors import ConfigurationError

from .archive import RetryingArchive, classify
from .consumer import ObservedConsumer
from .health import ManagementServer
from .metrics import Metrics
from .state import State

LOGGER = logging.getLogger("maintai.storage")


class Runtime:
    def __init__(
        self,
        settings,
        *,
        s3=None,
        admin=None,
        consumer_factory=Consumer,
        contracts=None,
    ):
        self.settings = settings
        self.contracts = contracts or ContractRegistry()
        self.state = State(settings.roles)
        self.metrics = Metrics(self.state)
        self.s3 = s3 if s3 is not None else settings.create_s3_client()
        self.admin = (
            admin
            if admin is not None
            else AdminClient({**settings.kafka.client_config(), "log_level": 0})
        )
        self.consumer_factory = consumer_factory
        self.management = None

    def probe(self):
        self.state.tick("supervisor")
        available = True
        for dependency, check in (("s3", self._probe_s3), ("kafka", self._probe_kafka)):
            try:
                check()
            except Exception as exc:
                available = False
                self.state.dependency(dependency, False)
                _, error_type = classify(exc)
                self.metrics.dependency_error(dependency, error_type)
                LOGGER.warning(
                    json.dumps(
                        {
                            "event": "dependency_probe_failed",
                            "dependency": dependency,
                            "error_type": error_type,
                        }
                    )
                )
            else:
                self.state.dependency(dependency, True)
        self.state.tick("supervisor")
        return available

    def _probe_s3(self):
        versioning = self.s3.get_bucket_versioning(Bucket=self.settings.bucket)
        if versioning.get("Status") != "Enabled" and not self.settings.allow_unversioned:
            raise ConfigurationError("Archive bucket versioning must be enabled")

    def _probe_kafka(self):
        # No per-topic lookup or topic creation: list only metadata visible through ACLs.
        metadata = self.admin.list_topics(timeout=self.settings.probe_timeout_seconds)
        for role in self.settings.roles:
            topic = self.contracts.topology["profiles"][role]["topic"]
            entry = metadata.topics.get(topic)
            if entry is None or entry.error or not entry.partitions:
                raise ConfigurationError("Required archive topic is unavailable")

    def _consume(self, role, stop):
        self.state.tick(role)
        client = None
        try:
            profile = self.contracts.topology["profiles"][role]

            def error_callback(_error):
                # Never log broker error text (it can contain addresses or credentials).
                self.state.dependency("kafka", False)
                self.metrics.dependency_error("kafka", "UNAVAILABLE")

            config = {
                **self.settings.kafka.consumer_config(profile["group_id"]),
                "client.id": self.settings.kafka.client_id + "-" + role,
                "error_cb": error_callback,
                "log_level": 0,  # Errors go through the bounded callback, not native text dumps.
            }
            client = ObservedConsumer(self.consumer_factory(config), role, self.state)
            writer = S3RecordArchive(
                self.s3,
                self.settings.bucket,
                prefix=self.settings.prefix,
                allow_unversioned=self.settings.allow_unversioned,
            )
            handler = RetryingArchive(
                writer,
                role,
                self.settings,
                self.state,
                self.metrics,
                stop,
            )
            ConsumerRunner(
                self.settings.kafka,
                role,
                handler,
                contracts=self.contracts,
                client=client,
            ).run(stop)
            if not stop.is_set():
                raise RuntimeError("Unexpected consumer exit")
        except Exception as exc:
            if not stop.is_set():
                self.state.shutdown(failed=True)
                LOGGER.error(
                    json.dumps(
                        {
                            "event": "consumer_stopped",
                            "role": role,
                            "error_type": type(exc).__name__,
                        }
                    )
                )
                stop.set()
        finally:
            if client is not None:
                client.close()

    def run(self, stop=None):
        stop = stop if stop is not None else threading.Event()
        self.state.stop = stop
        workers = []
        try:
            self.management = ManagementServer(self.settings, self.state, self.metrics)
            self.management.start()
            # Do not consume/write any receipt until the versioning preflight succeeds.
            while not stop.is_set() and not self.probe():
                stop.wait(self.settings.probe_interval_seconds)
            if not stop.is_set():
                for role in self.settings.roles:
                    worker = threading.Thread(
                        target=self._consume,
                        args=(role, stop),
                        name=role,
                    )
                    workers.append(worker)
                    worker.start()
                next_probe = time.monotonic() + self.settings.probe_interval_seconds
                while not stop.wait(0.1):
                    self.state.tick("supervisor")
                    if not self.management.thread.is_alive():
                        raise RuntimeError("Management server stopped")
                    if any(not worker.is_alive() for worker in workers):
                        raise RuntimeError("Consumer thread stopped")
                    if time.monotonic() >= next_probe:
                        self.probe()
                        next_probe = time.monotonic() + self.settings.probe_interval_seconds
        finally:
            self.state.shutdown()
            stop.set()
            for worker in workers:
                worker.join()
            if self.management is not None:
                self.management.close()
            self.s3.close()
        return 1 if self.state.failed else 0
