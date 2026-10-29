"""Real Kafka consumer/relay around explicit ML, policy and registry fixtures."""

import threading
import time
from datetime import UTC, datetime
from types import SimpleNamespace

from maintai_messaging.consumer import ConsumerRunner
from maintai_messaging.postgres import OutboxRelay
from maintai_messaging.producer import KafkaPublisher

from processing.shared.inference_client.client import PredictionAttempt
from processing.worker.tests.test_worker_transaction import Providers, handler


class WorkerHarness:
    def __init__(self, kafka, factory, scenarios):
        self.kafka, self.factory, self.scenarios = kafka, factory, scenarios
        self.stop = threading.Event()
        self.failed = []
        self.threads = []

    def process(self, event, record):
        kind = self.scenarios[event.message_id]

        class CurrentProviders(Providers):
            def predict(self, **kwargs):
                time.sleep(0.1)
                result = super().predict(**kwargs)
                if result.response is None:
                    return result
                return PredictionAttempt(
                    result.response.model_copy(update={"predicted_at": datetime.now(UTC)}), None, 1
                )

        providers = CurrentProviders(
            probability=0.91 if kind == "alert" else 0.12,
            failure="ML_UNAVAILABLE" if kind == "unavailable" else None,
        )
        return handler(SimpleNamespace(factory=self.factory), providers)(event, record)

    def start(self):
        def consume():
            ConsumerRunner(self.kafka, "processing", self.process).run(self.stop)

        def relay():
            with KafkaPublisher(self.kafka, "outbox") as publisher:
                adapter = OutboxRelay(self.factory, publisher)
                while not self.stop.is_set():
                    adapter.run_once()
                    self.stop.wait(0.05)

        def guarded(target):
            try:
                target()
            except Exception as error:
                self.failed.append(type(error).__name__)
                self.stop.set()

        for target in (consume, relay):
            thread = threading.Thread(target=guarded, args=(target,), daemon=True)
            self.threads.append(thread)
            thread.start()

    def close(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=15)
