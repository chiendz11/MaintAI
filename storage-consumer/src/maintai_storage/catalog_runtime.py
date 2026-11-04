"""Read-only Kafka audit + raw manifest reconciliation for the M6 catalog.

The audit client uses explicit assignment, never subscribes to a group and never
commits. Observing actual broker records/EOF distinguishes offset control gaps
from missing raw receipts. S3 remains the source of the catalog entry contents.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, TopicCollection, TopicPartition
from maintai_messaging.errors import ConfigurationError, IdentityConflict

from .catalog import TOPICS, CatalogPublisher, CatalogStore, _missing, now_utc

LOGGER = logging.getLogger("maintai.storage")


class KafkaAudit:
    def __init__(self, settings, admin, *, consumer_factory=Consumer):
        self.settings, self.admin, self.consumer_factory = settings, admin, consumer_factory
        self.clients = {}
        self.probes = {}
        self.buffers = {}
        self.positions = {}
        self.bases = {}

    def close(self):
        for consumer in [*self.clients.values(), *self.probes.values()]:
            consumer.close()
        self.clients.clear()
        self.probes.clear()
        self.buffers.clear()
        self.positions.clear()
        self.bases.clear()

    def describe(self):
        metadata = self.admin.list_topics(timeout=self.settings.probe_timeout_seconds)
        if not metadata.cluster_id:
            raise ConfigurationError("Catalog requires Kafka cluster identity")
        descriptions = self.admin.describe_topics(
            TopicCollection(list(TOPICS)),
            request_timeout=self.settings.probe_timeout_seconds,
        )
        partitions = []
        for topic in TOPICS:
            entry = metadata.topics.get(topic)
            if entry is None or entry.error:
                raise ConfigurationError("Catalog topic unavailable")
            description = descriptions[topic].result(timeout=self.settings.probe_timeout_seconds)
            identity = str(description.topic_id)
            if description.topic_id is None or not identity or identity == "AAAAAAAAAAAAAAAAAAAAAA":
                raise ConfigurationError("Catalog requires Kafka topic identity")
            partitions.extend(
                (topic, partition, identity) for partition in sorted(entry.partitions)
            )
        return metadata.cluster_id, partitions

    def scan(self, topic, partition, start, *, limit, stop):
        # Watermark probes are shared per topic; fetch assignment is persistent per
        # non-empty partition. Reassigning a client at every short poll deadline
        # can continuously cancel an in-flight fetch and starve catalog progress.
        group = "raw-archive-v1" if topic == "sensor.readings" else "raw-rejected-archive-v1"
        config = {
            **self.settings.kafka.consumer_config(group),
            "client.id": self.settings.kafka.client_id + "-catalog-audit",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
            "enable.partition.eof": True,
            "isolation.level": "read_committed",
            "fetch.wait.max.ms": 50,
            "log_level": 0,
        }
        coordinate = (topic, partition)
        try:
            if topic not in self.probes:
                self.probes[topic] = self.consumer_factory(config)
            low, high = self.probes[topic].get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=self.settings.probe_timeout_seconds,
                cached=False,
            )
            if start >= high:
                # The requested interval is empty. Never spend a poll budget on
                # empty partitions or discard a fetch before it has returned.
                self._close_partition(coordinate)
                return low, high, [], start
            base = max(start, low)
            if coordinate not in self.clients:
                if len(self.clients) >= 128:
                    raise ConfigurationError("Catalog active partition budget exceeded")
                client = self.consumer_factory(config)
                self.clients[coordinate] = client
                self.buffers[coordinate] = []
                self.positions[coordinate] = base
                self.bases[coordinate] = base
                client.assign([TopicPartition(topic, partition, base)])
            client = self.clients[coordinate]
            if base < self.bases[coordinate] or base > self.positions[coordinate]:
                client.assign([TopicPartition(topic, partition, base)])
                self.buffers[coordinate] = []
                self.positions[coordinate] = self.bases[coordinate] = base
            # Keep receipts whose raw manifests are pending. Their broker fetch
            # need not be repeated; at most `limit` offsets are buffered per key.
            pending = [o for o in self.buffers[coordinate] if o >= base]
            self.buffers[coordinate] = pending
            deadline = time.monotonic() + self.settings.catalog_poll_seconds
            while len(pending) < limit and time.monotonic() < deadline and not stop.is_set():
                message = client.poll(0.1)
                if message is None:
                    continue
                error = message.error()
                if error:
                    if error.code() == KafkaError._PARTITION_EOF:
                        self.positions[coordinate] = max(
                            self.positions[coordinate], message.offset()
                        )
                        break
                    raise ConfigurationError("Kafka catalog audit failed")
                offset = message.offset()
                if offset >= base and offset not in pending:
                    pending.append(offset)
                self.positions[coordinate] = offset + 1
                if offset >= high:
                    break
            records = [o for o in pending if base <= o < high]
            return low, high, records, min(high, self.positions[coordinate])
        except Exception:
            self._close_partition(coordinate)
            probe = self.probes.pop(topic, None)
            if probe is not None:
                probe.close()
            raise

    def _close_partition(self, coordinate):
        client = self.clients.pop(coordinate, None)
        if client is not None:
            client.close()
        self.buffers.pop(coordinate, None)
        self.positions.pop(coordinate, None)
        self.bases.pop(coordinate, None)


class CatalogRuntime:
    def __init__(self, settings, s3, audit, *, state=None, metrics=None):
        self.settings, self.audit, self.state, self.metrics = settings, audit, state, metrics
        self.store = CatalogStore(
            s3, settings.bucket, raw_prefix=settings.prefix, prefix=settings.catalog_prefix
        )
        self.publisher = CatalogPublisher(self.store)

    def sync(self, stop):
        current, _ = self.store.current()
        old = current[1] if current else None
        cluster_id, coordinates = self.audit.describe()
        if old and old["source_cluster_id"] != cluster_id:
            raise IdentityConflict("Catalog source Kafka cluster changed")
        prior = {(c["topic"], c["partition"]): c for c in old["partitions"]} if old else {}
        entries = []
        updates = [
            {
                "topic": topic,
                "partition": partition,
                "topic_id": topic_id,
                "start_offset": 0,
                "archived_next_offset": 0,
                "indexed_next_offset": 0,
                "captured_log_end_offset": None,
                "captured_at": now_utc(),
                "gaps": [],
                "status": "UNKNOWN",
            }
            for topic, partition, topic_id in coordinates
            if (topic, partition) not in prior
        ]
        for topic, partition, topic_id in coordinates:
            if stop.is_set():
                break
            if self.state:
                self.state.tick("catalog")
            previous = prior.get((topic, partition))
            if previous and previous["topic_id"] != topic_id:
                raise IdentityConflict("Catalog topic was recreated")
            start = previous["indexed_next_offset"] if previous else 0
            low, high, offsets, scanned = self.audit.scan(
                topic,
                partition,
                start,
                limit=self.settings.catalog_batch_records,
                stop=stop,
            )
            gaps = list(previous["gaps"]) if previous else []
            if low > start:
                gaps.append(
                    {"from_offset": start, "to_offset": low, "reason": "RETENTION_UNVERIFIABLE"}
                )
            if high < start:
                gaps.append({"from_offset": high, "to_offset": start, "reason": "LOG_TRUNCATED"})
            elif previous and high < (previous["captured_log_end_offset"] or 0):
                gaps.append(
                    {
                        "from_offset": high,
                        "to_offset": previous["captured_log_end_offset"],
                        "reason": "LOG_TRUNCATED",
                    }
                )
            frontier = max(start, low)
            pending = False
            for offset in offsets:
                if stop.is_set():
                    pending = True
                    break
                try:
                    entry = self.store.receipt(topic, partition, offset)
                except Exception as exc:
                    if not _missing(exc):
                        raise
                    # Expected broker receipt not archived yet: never advance over it.
                    frontier, pending = offset, True
                    break
                entries.append(entry)
                frontier = offset + 1
                if self.state:
                    self.state.tick("catalog")
            if not pending:
                frontier = max(frontier, scanned)
            coverage = {
                "topic": topic,
                "partition": partition,
                "topic_id": topic_id,
                "start_offset": previous["start_offset"] if previous else 0,
                "archived_next_offset": frontier,
                "indexed_next_offset": frontier,
                "captured_log_end_offset": high,
                "captured_at": now_utc(),
                "gaps": list({json.dumps(g, sort_keys=True): g for g in gaps}.values()),
                "status": "UNKNOWN" if gaps else ("COMPLETE" if frontier >= high else "LAGGING"),
            }
            # Do not create generations just to update the observation timestamp of idle partitions.
            meaningful = {k: v for k, v in coverage.items() if k != "captured_at"}
            old_meaningful = (
                {k: v for k, v in previous.items() if k != "captured_at"} if previous else None
            )
            heartbeat_due = (
                previous
                and (
                    datetime.now(UTC)
                    - datetime.fromisoformat(previous["captured_at"].replace("Z", "+00:00"))
                ).total_seconds()
                >= self.settings.catalog_heartbeat_seconds
            )
            if meaningful != old_meaningful or heartbeat_due:
                updates.append(coverage)
        pointer = self.publisher.publish(entries, updates, cluster_id)
        if self.metrics:
            self.metrics.catalog_result("SUCCESS", len(entries))
            current, _ = self.store.current()
            if current:
                self.metrics.catalog_coverage(current[1]["partitions"])
        return pointer

    def rebuild(self, stop):
        """Explicit operator recovery from durable manifests, including Kafka-expired receipts.

        Listing tokens are only used within this scan. This restores entries; it
        never clears retention gaps or manufactures a COMPLETE Kafka interval.
        """
        current, _ = self.store.current()
        if current is None:
            raise ConfigurationError("Run catalog-sync to establish source identity before rebuild")
        cluster_id = current[1]["source_cluster_id"]
        entries = []
        count = 0
        pages = self.store.s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.store.bucket,
            Prefix=self.settings.prefix + "/",
            PaginationConfig={"PageSize": self.settings.catalog_batch_records},
        )
        for page in pages:
            for item in page.get("Contents", []):
                if stop.is_set():
                    return count
                key = item["Key"]
                if not key.endswith(".manifest.json"):
                    continue
                # Parse the deterministic locator, never client-controlled object URLs.
                relative = key.removeprefix(self.settings.prefix + "/").split("/")
                if len(relative) != 3:
                    continue
                topic = relative[0].removeprefix("topic=")
                if topic not in TOPICS:
                    continue
                partition = int(relative[1].removeprefix("partition="))
                offset = int(relative[2].removeprefix("offset=").removesuffix(".manifest.json"))
                entries.append(self.store.receipt(topic, partition, offset))
                count += 1
                if len(entries) >= self.settings.catalog_batch_records:
                    self.publisher.publish(entries, [], cluster_id)
                    entries = []
            if self.state:
                self.state.tick("catalog")
        if entries:
            self.publisher.publish(entries, [], cluster_id)
        return count

    def run(self, stop):
        try:
            self._run(stop)
        finally:
            self.audit.close()

    def _run(self, stop):
        while not stop.is_set():
            if self.state:
                self.state.tick("catalog")
            try:
                self.sync(stop)
            except Exception as exc:
                if self.state:
                    self.state.dependency("catalog", False)
                if self.metrics:
                    self.metrics.catalog_result("ERROR", 0)
                LOGGER.warning(
                    json.dumps({"event": "catalog_sync_failed", "error_type": type(exc).__name__})
                )
            else:
                if self.state:
                    self.state.dependency("catalog", True)
            # Keep heartbeat fresh while dependencies are unavailable, without a busy retry loop.
            deadline = time.monotonic() + self.settings.catalog_interval_seconds
            while not stop.wait(0.1) and time.monotonic() < deadline:
                if self.state:
                    self.state.tick("catalog")
