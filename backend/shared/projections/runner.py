"""One recoverable rebuild loop, independent of Kafka callbacks and Copilot jobs."""

import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from backend.shared.projections.errors import retryable
from backend.shared.projections.sources import SourcePending


class RebuildRunner:
    def __init__(self, repository, raw, outcomes, stop):
        self.repository, self.raw, self.outcomes, self.stop = repository, raw, outcomes, stop
        self.owner = uuid4().hex
        self.thread = threading.Thread(
            target=self.run, name="business-projection-rebuild", daemon=True
        )

    def start(self):
        self.thread.start()

    def close(self):
        self.thread.join(timeout=10)

    def run(self):
        while not self.stop.is_set():
            lease = None
            heartbeat_stop = threading.Event()
            heartbeat = None
            try:
                lease = self.repository.claim(self.owner)
                if lease is None:
                    self.stop.wait(1)
                    continue

                def renew(lease=lease, heartbeat_stop=heartbeat_stop):
                    while not heartbeat_stop.wait(15) and not self.stop.is_set():
                        try:
                            self.repository.heartbeat(lease)
                        except Exception:
                            heartbeat_stop.set()
                            return

                heartbeat = threading.Thread(
                    target=renew, name="projection-rebuild-lease", daemon=True
                )
                heartbeat.start()
                self.execute(lease, heartbeat_stop)
            except SourcePending:
                # Preserve the original deadline, refs and partial progress for bounded retries.
                self.stop.wait(1)
            except Exception as exc:
                if lease is not None and not retryable(exc):
                    try:
                        self.repository.fail(lease["job_id"], "SOURCE_CORRUPT", lease=lease)
                    except Exception:
                        pass
                self.stop.wait(1)
            finally:
                heartbeat_stop.set()
                if heartbeat is not None:
                    heartbeat.join(timeout=3)
                if lease is not None:
                    self.repository.release(lease)

    def execute(self, lease, heartbeat_stop):
        def stopped():
            return self.stop.is_set() or heartbeat_stop.is_set()

        while not stopped() and not self.repository.status(lease["job_id"])["baseline_done"]:
            self.repository.scan_members(lease)
        if stopped():
            return
        # Complete bounded catalog traversal before interpreting a missing exact receipt.
        while not stopped() and self.raw.sync(lease):
            pass
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="projection-source") as executor:
            while not stopped():
                members = self.repository.pending(lease)
                if not members:
                    break
                # Two in-flight results, each <=1MiB; no queued batch of wire bodies.
                for index in range(0, len(members), 2):
                    pair = members[index : index + 2]
                    futures = [
                        executor.submit(self.raw.read, lease, member)
                        if member["topic"] == "sensor.readings"
                        else executor.submit(self.outcomes.read, member)
                        for member in pair
                    ]
                    results = [future.result() for future in futures]
                    if sum(len(event.wire_bytes) for event, _ in results) > 8 * 1024 * 1024:
                        raise ValueError("Source batch exceeds wire memory budget")
                    for member, (event, reference) in zip(pair, results, strict=True):
                        if stopped():
                            return
                        self.repository.apply_member(lease, member, event, reference)
        while not stopped() and self.repository.catch_up(lease):
            pass
        if not stopped():
            self.repository.verify(lease["job_id"], lease=lease)
