"""Two independent leased workers, isolated from Kafka and Alert transactions."""

import logging
import random
import threading
import time
from datetime import UTC, datetime

from rag_client import ProviderFailure


class BriefRunner:
    def __init__(
        self,
        repository,
        client,
        stop,
        *,
        metrics=None,
        redis_bus=None,
        sweep_interval=2.0,
        reclaim_idle_ms=90000,
    ):
        self.repository, self.client, self.stop = repository, client, stop
        self.threads = []
        self.metrics = metrics
        self.redis_bus = redis_bus
        self.sweep_interval = sweep_interval
        self.reclaim_idle_ms = reclaim_idle_ms
        self._group_ready = False
        self._group_lock = threading.Lock()

    def _ensure_group(self):
        if self.redis_bus is None or self._group_ready:
            return
        with self._group_lock:
            if not self._group_ready:
                self.redis_bus.ensure_brief_group()
                self._group_ready = True

    def _execute_claim(self, claim):
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

    def once(self, owner):
        claim = self.repository.claim(owner)
        if claim is None:
            return False
        self._execute_claim(claim)
        return True

    def once_stream(self, owner, *, consumer=None, reclaim=False, block_ms=500):
        if self.redis_bus is None:
            return self.once(owner)
        self._ensure_group()
        consumer_id = consumer or owner
        entries = []
        if reclaim:
            entries = self.redis_bus.autoclaim_brief_jobs(
                consumer_id,
                min_idle_ms=self.reclaim_idle_ms,
                count=4,
            )
        if not entries:
            entries = self.redis_bus.read_brief_jobs(
                consumer_id,
                count=1,
                block_ms=block_ms,
            )
        if not entries:
            return False
        for entry_id, fields in entries:
            job_id = fields.get("job_id")
            if not job_id:
                self.redis_bus.ack_brief_job(entry_id)
                continue
            claim, disposition = self.repository.claim_by_id(owner, job_id)
            if claim is not None:
                self._execute_claim(claim)
                self.redis_bus.ack_brief_job(entry_id)
            elif disposition in {"DONE", "DEFER_BACKOFF"}:
                self.redis_bus.ack_brief_job(entry_id)
        return True

    def start(self):
        from uuid import uuid4

        def work():
            owner = uuid4().hex
            next_sweep = 0.0
            while not self.stop.is_set():
                if self.redis_bus is not None:
                    now_mono = time.monotonic()
                    reclaim = now_mono >= next_sweep
                    try:
                        self.once_stream(owner, consumer=owner, reclaim=reclaim, block_ms=500)
                        if reclaim:
                            # Drain delayed backoff retries without blocking new stream items.
                            while not self.stop.is_set() and self.once(owner):
                                pass
                            next_sweep = time.monotonic() + self.sweep_interval
                    except Exception:
                        logging.getLogger(__name__).warning(
                            "Redis Stream brief attempt interrupted; falling back to Postgres sweep"
                        )
                        try:
                            if not self.once(owner):
                                self.stop.wait(1)
                        except Exception:
                            self.stop.wait(1)
                else:
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
