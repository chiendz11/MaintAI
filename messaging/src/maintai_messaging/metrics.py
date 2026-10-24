"""Optional OBS-01 collectors. Instrumentation errors never change durable effects."""

import logging
import time


def observe_safely(callback):
    try:
        callback()
    except Exception:
        logging.getLogger(__name__).error("Processing instrumentation unavailable")


class ProcessingMetrics:
    def __init__(self, registry):
        from prometheus_client import Counter, Gauge, Histogram

        self.active = Gauge("maintai_processing_active_keys", "Active keys", registry=registry)
        self.queue = Gauge(
            "maintai_processing_queue_records", "Retained receipts", ["state"], registry=registry
        )
        self.bytes = Gauge("maintai_processing_pending_bytes", "Retained bytes", registry=registry)
        self.uncommitted = Gauge(
            "maintai_processing_uncommitted_records", "Before offset ACK", registry=registry
        )
        self.stall = Gauge(
            "maintai_processing_frontier_stall_seconds", "Blocking hole age", registry=registry
        )
        self.paused = Gauge(
            "maintai_processing_paused_partitions",
            "Paused partitions",
            ["reason"],
            registry=registry,
        )
        self.stale = Counter(
            "maintai_processing_stale_completions_total",
            "Stale completion",
            ["stage"],
            registry=registry,
        )
        self.queue_wait = Histogram(
            "maintai_processing_queue_wait_seconds",
            "Poll to dispatch",
            registry=registry,
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
        )
        self.blocked_since = {}

    def update(self, runner, entries, draining):
        now = time.monotonic()
        active = {id(item[1]) for item in runner.active.values()}
        states = dict.fromkeys(("FETCHED", "QUEUED", "IN_FLIGHT", "COMPLETED_WAITING_COMMIT"), 0)
        for entry in entries:
            state = (
                "COMPLETED_WAITING_COMMIT"
                if entry.done
                else ("IN_FLIGHT" if id(entry) in active else "QUEUED")
            )
            states[state] += 1
        for state, count in states.items():
            self.queue.labels(state=state).set(count)
        self.active.set(len(runner.active))
        self.bytes.set(sum(e.size for e in entries))
        self.uncommitted.set(len(entries))
        holes = {}
        for partition, queue in runner.ledger.partitions.items():
            unfinished = next((e for e in queue if not e.done), None)
            if unfinished and any(
                e.done and e.record.offset > unfinished.record.offset for e in queue
            ):
                key = (*partition, unfinished.record.offset)
                holes[key] = self.blocked_since.get(key, now)
        self.blocked_since = holes
        self.stall.set(max((now - start for start in holes.values()), default=0))
        reasons = dict.fromkeys(("QUEUE_BUDGET", "FRONTIER_BUDGET", "DRAIN"), 0)
        for partition in runner.paused:
            reason = (
                "DRAIN"
                if draining
                else (
                    "FRONTIER_BUDGET"
                    if len(runner.ledger.partitions[partition])
                    >= runner.limits.max_uncommitted_per_partition
                    else "QUEUE_BUDGET"
                )
            )
            reasons[reason] += 1
        for reason, count in reasons.items():
            self.paused.labels(reason=reason).set(count)


class OutboxOrderMetrics:
    """Read-only, DB-wide gauges. Use max across replicas, never sum duplicates."""

    def __init__(self, connection_factory, registry):
        from prometheus_client import Counter

        self.connection_factory = connection_factory
        self.conflicts = Counter(
            "maintai_outbox_order_conflicts_total",
            "Rejected outbox order",
            ["topic", "reason"],
            registry=registry,
        )
        registry.register(self)

    def describe(self):
        from prometheus_client.core import GaugeMetricFamily

        yield GaugeMetricFamily(
            "maintai_outbox_blocked_keys", "Blocked keys", labels=["topic", "reason"]
        )
        yield GaugeMetricFamily(
            "maintai_outbox_head_blockage_age_seconds", "Blocked head age", labels=["topic"]
        )

    def collect(self):
        from prometheus_client.core import GaugeMetricFamily

        counts = GaugeMetricFamily(
            "maintai_outbox_blocked_keys", "Blocked keys", labels=["topic", "reason"]
        )
        age = GaugeMetricFamily(
            "maintai_outbox_head_blockage_age_seconds", "Blocked head age", labels=["topic"]
        )
        try:
            with self.connection_factory() as connection:
                rows = connection.execute(
                    "SELECT k.topic,CASE WHEN o.last_error='PERMANENT' THEN 'ERROR' "
                    "WHEN o.last_error='PUBLISH_UNACKNOWLEDGED' THEN 'UNKNOWN' "
                    "WHEN o.status='PENDING' AND o.next_attempt_at>clock_timestamp() "
                    "THEN 'BACKOFF' "
                    "WHEN k.owner IS NOT NULL AND k.lease_until>=clock_timestamp() THEN 'LEASED' "
                    "ELSE NULL END AS reason,"
                    "extract(epoch FROM clock_timestamp()-o.created_at)::double precision AS age "
                    "FROM maintai_outbox_keys k JOIN maintai_kafka_outbox o "
                    "ON o.topic=k.topic AND o.message_key=k.message_key "
                    "AND o.dispatch_seq=k.last_published+1 "
                    "WHERE k.last_allocated>k.last_published+1"
                ).fetchall()
        except Exception:
            logging.getLogger(__name__).error("Outbox order metric snapshot unavailable")
            return
        for topic in ("inference.results", "alert.signals"):
            for reason in ("ERROR", "UNKNOWN", "BACKOFF", "LEASED"):
                total = sum(r["topic"] == topic and r["reason"] == reason for r in rows)
                counts.add_metric([topic, reason], total)
            values = [max(0, r["age"]) for r in rows if r["topic"] == topic and r["reason"]]
            age.add_metric([topic], max(values, default=0))
        yield counts
        yield age
