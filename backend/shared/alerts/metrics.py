"""OBS-01 counters update only after their owning transaction has committed."""

import math
import time
from datetime import datetime

from prometheus_client import Counter, Histogram


class AlertMetrics:
    def __init__(self, registry, *, dropped=None, wall_time=time.time):
        self.clock, self.dropped = wall_time, dropped
        self.created = Counter(
            "maintai_alerts_created_total", "Committed alerts", ["severity"], registry=registry
        )
        self.deduped = Counter(
            "maintai_alerts_deduplicated_total",
            "Committed grouped signals",
            ["severity"],
            registry=registry,
        )
        self.transitions = Counter(
            "maintai_alert_transitions_total",
            "Committed state changes",
            ["from_status", "to_status"],
            registry=registry,
        )
        self.visible = Histogram(
            "maintai_event_to_alert_visible_duration_seconds",
            "Accepted source to readable alert commit",
            registry=registry,
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
        )

    def transition(self, before, after):
        self.transitions.labels(before, after).inc()

    def signal(self, disposition, severity, accepted):
        (self.created if disposition == "CREATED" else self.deduped).labels(severity).inc()
        try:
            stamp = datetime.fromisoformat(accepted)
            if stamp.tzinfo is None:
                raise ValueError("Missing source timezone")
            elapsed = self.clock() - stamp.timestamp()
            reason = (
                "INVALID_VALUE"
                if not math.isfinite(elapsed)
                else "CLOCK_SKEW"
                if elapsed < 0
                else None
            )
        except (ValueError, TypeError, OverflowError):
            reason = "SOURCE_TIME_MISSING"
        if reason is None:
            self.visible.observe(elapsed)
        elif self.dropped is not None:
            self.dropped.labels(reason).inc()
