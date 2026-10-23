import threading

from maintai_messaging.contracts import default_contracts
from maintai_messaging.producer import Delivery


class FakePublisher:
    def __init__(self, settings):
        self.settings = settings
        self.records = []
        self.closed = 0
        self.failure = None
        self.started = threading.Event()
        self.barrier = None

    def publish(self, topic, payload):
        event = default_contracts().encode(topic, payload)
        self.started.set()
        if self.barrier and not self.barrier.wait(5):
            raise TimeoutError("test barrier")
        if self.failure:
            raise self.failure
        self.records.append(event)
        return Delivery(topic, 0, len(self.records) - 1)

    def close(self):
        self.closed += 1


class Probe:
    value = True

    def check(self):
        return self.value
