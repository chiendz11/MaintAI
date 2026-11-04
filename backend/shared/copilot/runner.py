"""Two independent leased workers, isolated from Kafka and Alert transactions."""

import logging
import random
import threading
from datetime import UTC, datetime

from rag_client import ProviderFailure


class BriefRunner:
    def __init__(self, repository, client, stop, *, metrics=None):
        self.repository, self.client, self.stop = repository, client, stop
        self.threads = []
        self.metrics = metrics

    def once(self, owner):
        claim = self.repository.claim(owner)
        if claim is None:
            return False
        done = threading.Event()

        def heartbeat():
            while not done.wait(15):
                try:
                    if not self.repository.heartbeat(claim):
                        return
                except Exception:
                    return

        pulse = threading.Thread(target=heartbeat, daemon=True, name="brief-lease")
        pulse.start()
        try:
            timeout = max(0.001, min(60, (claim["deadline"] - datetime.now(UTC)).total_seconds()))
            output = self.client.generate(claim["input"], job_id=claim["job_id"], timeout=timeout)
            committed = self.repository.complete(claim, output)
            if committed and self.metrics is not None:
                self.metrics.attempts.labels("SUCCESS").inc()
        except ProviderFailure as error:
            committed = self.repository.fail(
                claim,
                error.code,
                retryable=error.retryable,
                delay=min(60, 2 ** claim["attempts"]) + random.random(),
            )
            if committed and self.metrics is not None:
                self.metrics.attempts.labels("ERROR").inc()
        finally:
            done.set()
            pulse.join(timeout=1)
        return True

    def start(self):
        from uuid import uuid4

        def work():
            owner = uuid4().hex
            while not self.stop.is_set():
                try:
                    if not self.once(owner):
                        self.stop.wait(1)
                except Exception:
                    logging.getLogger(__name__).warning("Brief job attempt interrupted")
                    self.stop.wait(1)

        self.threads = [
            threading.Thread(target=work, daemon=True, name=f"brief-{i}") for i in range(2)
        ]
        for thread in self.threads:
            thread.start()

    def close(self):
        # The supervising Consumer sets stop before bounded drain. Lost leases expire.
        for thread in self.threads:
            thread.join(timeout=5)
        if not any(thread.is_alive() for thread in self.threads):
            self.client.close()
