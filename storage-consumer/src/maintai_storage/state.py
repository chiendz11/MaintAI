"""Volatile health/observations only. Kafka offsets and S3 manifests are durable state."""

from __future__ import annotations

import threading
import time


class State:
    def __init__(self, roles, *, clock=time.monotonic):
        self.clock = clock
        self.lock = threading.RLock()
        self.roles = tuple(roles)
        self.ticks = {"supervisor": clock()}
        self.dependencies = {name: (False, None) for name in ("kafka", "s3")}
        self.pending = {}
        self.shutting_down = False
        self.failed = False
        self.stop = None

    def tick(self, role):
        with self.lock:
            self.ticks[role] = self.clock()

    def dependency(self, name, available):
        with self.lock:
            self.dependencies[name] = (available, self.clock())

    def begin(self, role, record):
        with self.lock:
            coordinate = (record.topic, record.partition, record.offset)
            self.pending.setdefault((role, coordinate), self.clock())

    def committed(self, role, positions):
        with self.lock:
            for entry in list(self.pending):
                owner, (topic, partition, offset) = entry
                if owner == role and any(
                    p.topic == topic and p.partition == partition and p.offset > offset
                    for p in positions
                ):
                    self.pending.pop(entry, None)

    def revoked(self, role):
        with self.lock:
            for entry in list(self.pending):
                if entry[0] == role:
                    self.pending.pop(entry, None)

    def oldest_age(self):
        with self.lock:
            return max((self.clock() - started for started in self.pending.values()), default=0)

    def shutdown(self, *, failed=False):
        with self.lock:
            self.failed = self.failed or failed
            self.shutting_down = True

    def snapshot(self, settings):
        with self.lock:
            now = self.clock()
            shutting_down = self.shutting_down or (self.stop is not None and self.stop.is_set())
            live = not self.failed and all(
                now - timestamp <= settings.liveness_max_age_seconds
                for timestamp in self.ticks.values()
            )
            dependencies = {
                name: available
                and checked is not None
                and now - checked <= settings.readiness_max_age_seconds
                for name, (available, checked) in self.dependencies.items()
            }
            return {
                "live": live,
                "ready": live
                and not shutting_down
                and all(role in self.ticks for role in self.roles)
                and all(dependencies.values()),
                "dependencies": dependencies,
                "shutting_down": shutting_down,
            }
