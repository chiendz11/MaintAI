#!/usr/bin/env python3
"""Phase 3: Business Consumer (Prediction / Live / Alert) & PG NOTIFY bottleneck benchmark.

Instruments ConsumerRunner internals (poll wait vs worker execution vs synchronous Kafka
offset commit), ConnectionPool(size=8) contention, Projector SQL/PG NOTIFY latency, live
LISTEN SSE wakeup latency, per-partition consumer lag across 12 partitions, out-of-order
prediction arrival rate + Sequence Guard skips, and 1 vs 2 vs 4 Business Consumer replicas.
"""

from __future__ import annotations

import argparse
import contextvars
import json
import statistics
import sys
import threading
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from prometheus_client import CollectorRegistry
from psycopg import sql
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[3]
for rel in ("", "processing/worker/src", "processing/outbox-relay/src", "backend/business-consumer/src"):
    path_str = str(ROOT / rel) if rel else str(ROOT)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from backend.shared.alerts.handler import AlertHandler  # noqa: E402
from backend.shared.alerts.repository import AlertRepository  # noqa: E402
from backend.shared.inbox.handler import ProjectionHandler  # noqa: E402
from backend.shared.notifications import DEFAULT_CHANNEL, Notifier, parse_hint  # noqa: E402
from backend.shared.persistence.migration import upgrade as upgrade_business  # noqa: E402
from backend.shared.persistence.pool import ConnectionPool as BusinessPool  # noqa: E402
from backend.shared.persistence.registry import RegistryRepository  # noqa: E402
from backend.shared.projections.projector import Projector, load as proj_load, persist as proj_persist  # noqa: E402
from backend.shared.projections.reducer import reduce_projection  # noqa: E402
from backend.shared.ordering.identity import guard_source  # noqa: E402
from consumer.handler import KeyedProcessingHandler, ProcessingHandler  # noqa: E402
from maintai_messaging.config import KafkaSettings, KeyedSettings  # noqa: E402
from maintai_messaging.consumer import ConsumerRunner  # noqa: E402
from maintai_messaging.contracts import default_contracts  # noqa: E402
from maintai_messaging.keyed import KeyedConsumerRunner  # noqa: E402
from maintai_messaging.metrics import ProcessingMetrics  # noqa: E402
from maintai_messaging.ordering import OrderedOutboxRelay, activate_processing_ordering  # noqa: E402
from maintai_messaging.outcomes import ModelIdentity  # noqa: E402
from maintai_messaging.postgres import connect as pg_connect  # noqa: E402
from maintai_messaging.producer import KafkaPublisher  # noqa: E402
from processing.shared.data_quality.machine_registry import MachineLookupResult  # noqa: E402
from processing.shared.data_quality.validator import FeatureValidator  # noqa: E402
from processing.shared.persistence.pool import ConnectionPool as ProcessingPool  # noqa: E402
from processing.shared.risk_engine.policy import RiskPolicy  # noqa: E402
from scripts.benchmarks.hybrid_ordering.controlled_benchmark import (  # noqa: E402
    ControlledMLProvider,
    LocalEvidenceClient,
    RunTrace,
    ensure_topics_and_groups,
    summarize,
    upgrade_processing,
    workload_rates,
)

CURRENT_ROLE: contextvars.ContextVar[str] = contextvars.ContextVar("biz_role", default="other")


class BizMetricsCollector:
    def __init__(self):
        self.lock = threading.Lock()
        self.active = False
        self.tx_ms: dict[str, list[float]] = defaultdict(list)
        self.exec_ms: dict[str, list[float]] = defaultdict(list)
        self.commit_ms: dict[str, list[float]] = defaultdict(list)
        self.pool_wait_ms: dict[str, list[float]] = defaultdict(list)
        self.kafka_commit_ms: dict[str, list[float]] = defaultdict(list)
        self.handler_wall_ms: dict[str, list[float]] = defaultdict(list)
        self.poll_calls_while_busy: dict[str, int] = defaultdict(int)
        self.poll_empty_sleep_ms: dict[str, list[float]] = defaultdict(list)
        self.wake_delay_ms: dict[str, list[float]] = defaultdict(list)
        self.batch_queue_lens: dict[str, list[int]] = defaultdict(list)
        self.proj_substeps: dict[str, list[float]] = defaultdict(list)
        self.pred_consumed_ts: list[float] = []
        self.pred_committed_ts: list[float] = []
        self.out_of_order_arrivals: int = 0
        self.total_pred_arrivals: int = 0
        self.stale_pred_skipped: int = 0
        self.max_pred_seq_seen: dict[str, int] = {}
        self.notify_sent_by_machine: dict[str, list[float]] = defaultdict(list)
        self.notify_recv_lat_ms: list[float] = []
        self.notify_received_count: int = 0
        self.notify_emitted_count: int = 0


class TrackedTx:
    def __init__(self, inner, proxy):
        self._inner = inner
        self._proxy = proxy

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        t0 = time.perf_counter()
        try:
            return self._inner.__exit__(exc_type, exc, tb)
        finally:
            self._proxy.commit_ms += (time.perf_counter() - t0) * 1000.0

    def __getattr__(self, name):
        return getattr(self._inner, name)


class TrackedConn:
    def __init__(self, conn, pool_wait_ms: float):
        self._conn = conn
        self.pool_wait_ms = pool_wait_ms
        self.exec_ms = 0.0
        self.commit_ms = 0.0

    def execute(self, *args, **kwargs):
        t0 = time.perf_counter()
        try:
            return self._conn.execute(*args, **kwargs)
        finally:
            self.exec_ms += (time.perf_counter() - t0) * 1000.0

    def transaction(self, *args, **kwargs):
        return TrackedTx(self._conn.transaction(*args, **kwargs), self)

    def __getattr__(self, name):
        return getattr(self._conn, name)


@contextmanager
def tracked_biz_pool(pool: BusinessPool, collector: BizMetricsCollector):
    role = CURRENT_ROLE.get()
    t0 = time.perf_counter()
    with pool() as conn:
        t1 = time.perf_counter()
        proxy = TrackedConn(conn, (t1 - t0) * 1000.0)
        try:
            yield proxy
        finally:
            total_ms = (time.perf_counter() - t1) * 1000.0
            with collector.lock:
                if collector.active:
                    collector.pool_wait_ms[role].append(proxy.pool_wait_ms)
                    collector.tx_ms[role].append(total_ms)
                    collector.exec_ms[role].append(proxy.exec_ms)
                    collector.commit_ms[role].append(proxy.commit_ms)


class InstrumentedConsumerRunner(ConsumerRunner):
    """Exact copy of ConsumerRunner logic with non-invasive timing instrumentation."""

    def __init__(self, settings, role, handler, *, collector: BizMetricsCollector, replica_id: int, **kwargs):
        super().__init__(settings, role, handler, **kwargs)
        self.collector = collector
        self.replica_id = replica_id
        self._last_exec_finished_at = None

    def _poll(self, timeout):
        t0 = time.perf_counter()
        res = super()._poll(timeout)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        with self.collector.lock:
            if self.collector.active and self._busy:
                self.collector.poll_calls_while_busy[self.role] += 1
                if res is None:
                    self.collector.poll_empty_sleep_ms[self.role].append(elapsed_ms)
        return res

    def _execute(self, record):
        t0 = time.perf_counter()
        try:
            return super()._execute(record)
        finally:
            done_ts = time.perf_counter()
            self._last_exec_finished_at = done_ts
            with self.collector.lock:
                if self.collector.active:
                    self.collector.handler_wall_ms[self.role].append((done_ts - t0) * 1000.0)

    def run(self, stop=None, *, max_records=None):
        from concurrent.futures import ThreadPoolExecutor
        from maintai_messaging.errors import ConfigurationError, ConsumerFailed

        stop = stop or threading.Event()
        if max_records is not None and max_records < 1:
            raise ConfigurationError("max_records must be positive")
        self.client.subscribe(
            [self.topic],
            on_assign=self._assigned,
            on_revoke=self._revoked,
            on_lost=self._revoked,
        )
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"maintai-{self.role}-{self.replica_id}")
        future = record = None
        generation = None
        try:
            while True:
                if future is not None:
                    if not future.done():
                        extra = self._poll(self.settings.poll_interval_seconds)
                        if extra is not None:
                            if len(self._pending) + 1 >= self._buffer_limit:
                                raise ConsumerFailed("Serial consumer exceeded its bounded buffer")
                            self._pending.append((self._generation, extra))
                            self._flow_control()
                        continue
                    collected_ts = time.perf_counter()
                    fin_ts = self._last_exec_finished_at
                    if fin_ts is not None:
                        with self.collector.lock:
                            if self.collector.active:
                                self.collector.wake_delay_ms[self.role].append(max(0.0, (collected_ts - fin_ts) * 1000.0))
                                self.collector.batch_queue_lens[self.role].append(len(self._pending))
                    try:
                        future.result()
                    except Exception as exc:
                        raise ConsumerFailed("Handler failed; no offset advanced") from exc
                    owned = {(p.topic, p.partition) for p in self.client.assignment()}
                    if generation == self._generation and (record.topic, record.partition) in owned:
                        c0 = time.perf_counter()
                        positions = self.client.commit(
                            offsets=[TopicPartition(record.topic, record.partition, record.offset + 1)],
                            asynchronous=False,
                        )
                        c_ms = (time.perf_counter() - c0) * 1000.0
                        with self.collector.lock:
                            if self.collector.active:
                                self.collector.kafka_commit_ms[self.role].append(c_ms)
                        if any(getattr(partition, "error", None) for partition in positions or ()):
                            raise ConsumerFailed("Broker did not acknowledge the offset commit")
                        self.processed += 1
                    self._busy = False
                    future = None
                    self._flow_control()
                if stop.is_set() or (max_records is not None and self.processed >= max_records):
                    break
                if self._pending:
                    pending_generation, record = self._pending.popleft()
                    if pending_generation != self._generation:
                        continue
                else:
                    record = self._poll(self.settings.poll_interval_seconds)
                if record is None:
                    continue
                generation = self._generation
                self._busy = True
                self._last_exec_finished_at = None
                self._flow_control()
                future = executor.submit(self._execute, record)
        finally:
            self.client.close()
            executor.shutdown(wait=True, cancel_futures=True)
        return self.processed


def run_biz_trial(
    *,
    mode: str,
    workload: str,
    biz_replicas: int,
    run_idx: int,
    database_url: str,
    bootstrap_servers: str,
    warmup_seconds: int = 5,
    measure_seconds: int = 15,
) -> dict:
    run_id = uuid4().hex[:12]
    proc_db = f"bench3_proc_{run_id}"
    biz_db = f"bench3_biz_{run_id}"
    base_url = make_url(database_url)
    proc_dsn = base_url.set(database=proc_db).render_as_string(hide_password=False)
    biz_dsn = base_url.set(database=biz_db).render_as_string(hide_password=False)

    with pg_connect(database_url) as conn:
        conn.autocommit = True
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(proc_db)))
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(biz_db)))

    contracts = default_contracts()
    upgrade_processing(proc_dsn)
    with pg_connect(proc_dsn) as conn, conn.transaction():
        activate_processing_ordering(conn, legacy_writers_drained=True)

    upgrade_business(biz_dsn)
    rates = workload_rates(workload, 50, workload_c_bg=34)
    with pg_connect(biz_dsn) as conn:
        RegistryRepository(lambda: pg_connect(biz_dsn)).provision(
            [{"machine_id": m, "display_name": f"Benchmark {m}"} for m in sorted(rates)]
        )

    settings = KafkaSettings(
        local_mode=True,
        security_protocol="PLAINTEXT",
        bootstrap_servers=bootstrap_servers,
        group_prefix=f"bench3-{run_id}-",
        poll_interval_seconds=0.01,
        producer_git_sha="a" * 40,
    )
    ensure_topics_and_groups(settings, contracts)

    per_key = 2 if mode == "hybrid" else 1
    limits = KeyedSettings(
        max_active_keys=8,
        max_inflight_per_key=per_key,
        max_total_inflight=8,
        max_pending_records=512,
        max_pending_per_key=64,
        max_uncommitted_per_partition=256,
        handler_timeout_seconds=60.0,
        drain_timeout_seconds=60.0,
        hybrid_ordering_enabled=(mode == "hybrid"),
    )
    proc_pool = ProcessingPool(proc_dsn, size=12)
    biz_pools = [BusinessPool(biz_dsn, size=8) for _ in range(biz_replicas)]

    trace = RunTrace()
    collector = BizMetricsCollector()

    example_completed = json.loads((ROOT / "contracts/kafka/examples/completed.json").read_text())
    model_identity = ModelIdentity(
        model_version=example_completed["model_version"],
        preprocessor_version=example_completed["preprocessor_version"],
        dataset_version=example_completed["dataset_version"],
        dataset_manifest_sha256=example_completed["dataset_manifest_sha256"],
        split_sha256=example_completed["split_sha256"],
        mlflow_run_id=example_completed["mlflow_run_id"],
        model_artifact_sha256=example_completed["model_artifact_sha256"],
        preprocessor_sha256=example_completed["preprocessor_sha256"],
        feature_schema_version=example_completed["feature_schema_version"],
        feature_schema_sha256=example_completed["feature_schema_sha256"],
        release_id=example_completed["release_id"],
        inference_image_digest=example_completed["inference_image_digest"],
    )
    ml = ControlledMLProvider(
        trace=trace,
        model_identity=model_identity,
        base_ms=25.0,
        jitter_ms=5.0,
        slow_prob=0.1,
        slow_ms=300.0,
        max_concurrency=12,
        seed=3000 + run_idx,
    )
    validator = FeatureValidator.from_file(
        ROOT / "contracts/v1.3/schemas/features.schema.json", version="sensor-validator-1.0.0"
    )
    policy = RiskPolicy.from_file(ROOT / "processing/config/risk-policies/demo-risk-v1.json")
    proc_group = settings.group_prefix + contracts.topology["profiles"]["processing"]["group_id"]

    class InstApp(ProcessingHandler):
        def persist_prepared(self, connection, event, record, outbox, prepared):
            res = super().persist_prepared(connection, event, record, outbox, prepared)
            with trace.lock:
                trace.persist_end_at[event.message_id] = time.monotonic()
            return res

        def persist_hybrid(self, connection, event, record, outbox, prepared, **kwargs):
            res = super().persist_hybrid(connection, event, record, outbox, prepared, **kwargs)
            with trace.lock:
                trace.persist_end_at[event.message_id] = time.monotonic()
            return res

    app_handler = InstApp(
        lambda: proc_pool(),
        consumer_group=proc_group,
        validator=validator,
        registry_lookup=lambda _m, **_kw: MachineLookupResult("FOUND", None),
        predict=ml.predict,
        identity_resolver=lambda _r: model_identity,
        policy=policy,
        producer_git_sha="a" * 40,
        registry_cache_seconds=30,
        contracts=contracts,
        hybrid_ordering_enabled=(mode == "hybrid"),
    )

    class InstKeyed(KeyedProcessingHandler):
        def admit(self, event, record, context):
            with trace.lock:
                trace.admitted_at.setdefault(event.message_id, time.monotonic())
            return super().admit(event, record, context)

        def handle(self, event, record, context):
            with trace.lock:
                trace.admitted_at.setdefault(event.message_id, time.monotonic())
            return super().handle(event, record, context)

    worker_runner = KeyedConsumerRunner(
        settings,
        "processing",
        InstKeyed(
            app_handler,
            on_error=lambda: setattr(trace, "errors", trace.errors + 1),
            hybrid_ordering_enabled=(mode == "hybrid"),
        ),
        limits=limits,
        metrics=ProcessingMetrics(CollectorRegistry()),
        contracts=contracts,
    )

    class TrackingPublisher:
        def __init__(self, inner):
            self._inner = inner
            self.settings = inner.settings
            self.contracts = inner.contracts

        def publish_event(self, event):
            out = self._inner.publish_event(event)
            now = time.monotonic()
            src_id = event.payload["event_id"]
            with trace.lock:
                if event.topic == "inference.results":
                    trace.kafka_result_pub_at.setdefault(src_id, now)
                elif event.topic == "alert.signals":
                    trace.kafka_alert_pub_at.setdefault(src_id, now)
            return out

    notifier = Notifier(DEFAULT_CHANNEL)
    from backend.shared.notifications import machine_key

    class InstProjector(Projector):
        def __call__(self, connection, event, record, outbox):
            is_pred = event.topic == "inference.results"
            now_mono = time.monotonic()
            t0 = time.perf_counter()
            machine = connection.execute(
                "SELECT machine_id FROM machines WHERE machine_id=%s FOR UPDATE", (event.key,)
            ).fetchone()
            if machine is None:
                raise RuntimeError("Missing machine")
            t1 = time.perf_counter()
            validation = guard_source(connection, event)
            t2 = time.perf_counter()
            rows = proj_load(connection, event.key)
            t3 = time.perf_counter()
            prev_lvp_seq = (
                int(rows["latest_valid_prediction"]["sequence_number"])
                if rows.get("latest_valid_prediction") is not None
                else -1
            )
            result = reduce_projection(rows, event, source_validation=validation)
            t4 = time.perf_counter()
            proj_persist(connection, result)
            t5 = time.perf_counter()
            if result.changed:
                notifier(connection, event.key)
            t6 = time.perf_counter()

            if is_pred:
                seq = int(event.payload["sequence_number"])
                with collector.lock:
                    if collector.active:
                        collector.total_pred_arrivals += 1
                        prev_seen = collector.max_pred_seq_seen.get(event.key, -1)
                        if seq < prev_seen:
                            collector.out_of_order_arrivals += 1
                        if seq > prev_seen:
                            collector.max_pred_seq_seen[event.key] = seq
                        if event.kind == "PREDICTION_COMPLETED" and prev_lvp_seq > seq:
                            collector.stale_pred_skipped += 1
                        collector.pred_consumed_ts.append(now_mono)
                        collector.proj_substeps["lock_machine_ms"].append((t1 - t0) * 1000.0)
                        collector.proj_substeps["guard_source_ms"].append((t2 - t1) * 1000.0)
                        collector.proj_substeps["load_3tables_ms"].append((t3 - t2) * 1000.0)
                        collector.proj_substeps["reduce_ms"].append((t4 - t3) * 1000.0)
                        collector.proj_substeps["persist_upsert_ms"].append((t5 - t4) * 1000.0)
                        collector.proj_substeps["pg_notify_ms"].append((t6 - t5) * 1000.0)
                    if result.changed:
                        collector.notify_emitted_count += 1
                        collector.notify_sent_by_machine[machine_key(event.key)].append(time.monotonic())
                with trace.lock:
                    trace.biz_pred_commit_at.setdefault(event.source_event_id, time.monotonic())
            elif result.changed:
                with collector.lock:
                    collector.notify_emitted_count += 1
                    collector.notify_sent_by_machine[machine_key(event.key)].append(time.monotonic())
            return result.telemetry_visible

    class InstProjHandler(ProjectionHandler):
        def __call__(self, event, record):
            tok = CURRENT_ROLE.set("business-live" if self.topic == "sensor.readings" else "business-prediction")
            try:
                res = super().__call__(event, record)
                if self.topic == "inference.results":
                    with collector.lock:
                        if collector.active:
                            collector.pred_committed_ts.append(time.monotonic())
                return res
            finally:
                CURRENT_ROLE.reset(tok)

    class InstAlertHandler(AlertHandler):
        def __call__(self, event, record):
            tok = CURRENT_ROLE.set("business-alert")
            try:
                return super().__call__(event, record)
            finally:
                CURRENT_ROLE.reset(tok)

    biz_group_live = settings.group_prefix + contracts.topology["profiles"]["business-live"]["group_id"]
    biz_group_pred = settings.group_prefix + contracts.topology["profiles"]["business-prediction"]["group_id"]
    biz_group_alert = settings.group_prefix + contracts.topology["profiles"]["business-alert"]["group_id"]
    evidence_client = LocalEvidenceClient(proc_pool, contracts)

    stop = threading.Event()
    relay_pub = KafkaPublisher(settings, "outbox")
    from confluent_kafka import Producer as RawKafkaProducer

    producer = RawKafkaProducer({**settings.producer_config(), "linger.ms": 2})

    biz_runners: list[InstrumentedConsumerRunner] = []
    biz_threads: list[threading.Thread] = []
    lag_samples: list[dict] = []
    t_worker = t_relay = t_notify = t_lag = None

    try:
        for idx in range(biz_replicas):
            pool = biz_pools[idx]
            fac = lambda p=pool: tracked_biz_pool(p, collector)  # noqa: E731
            proj = InstProjector(notify=notifier)
            h_live = InstProjHandler(fac, biz_group_live, topic="sensor.readings", projector=proj)
            h_pred = InstProjHandler(fac, biz_group_pred, topic="inference.results", projector=proj)
            h_alert = InstAlertHandler(fac, biz_group_alert, evidence_client, repository=AlertRepository(fac))
            for role, h in (
                ("business-live", h_live),
                ("business-prediction", h_pred),
                ("business-alert", h_alert),
            ):
                r_obj = InstrumentedConsumerRunner(
                    settings,
                    role,
                    h,
                    collector=collector,
                    replica_id=idx,
                    contracts=contracts,
                )
                biz_runners.append(r_obj)
                th = threading.Thread(target=r_obj.run, args=(stop,), daemon=True, name=f"biz-{role}-{idx}")
                biz_threads.append(th)

        relay = OrderedOutboxRelay(
            lambda: proc_pool(),
            TrackingPublisher(relay_pub),
            lease_seconds=120,
            batch_size=64,
            poll_interval_seconds=0.01,
        )

        def relay_loop():
            while not stop.is_set():
                try:
                    n = relay.run_once()
                    if n == 0:
                        stop.wait(0.01)
                except Exception:
                    stop.wait(0.05)

        def notify_listener_clean():
            import psycopg

            with psycopg.connect(biz_dsn, autocommit=True) as nconn:
                nconn.execute(f"LISTEN {DEFAULT_CHANNEL}")
                while not stop.is_set():
                    for notify in nconn.notifies(timeout=0.2):
                        recv_ts = time.monotonic()
                        try:
                            mkey = parse_hint(notify.payload)
                        except Exception:
                            continue
                        with collector.lock:
                            collector.notify_received_count += 1
                            q = collector.notify_sent_by_machine.get(mkey)
                            if q:
                                sent_ts = q.pop(0)
                                if collector.active:
                                    collector.notify_recv_lat_ms.append((recv_ts - sent_ts) * 1000.0)
                        if stop.is_set():
                            break

        t_worker = threading.Thread(target=lambda: worker_runner.run(stop), daemon=True)
        t_relay = threading.Thread(target=relay_loop, daemon=True)
        t_notify = threading.Thread(target=notify_listener_clean, daemon=True)
        t_worker.start()
        t_relay.start()
        t_notify.start()
        for th in biz_threads:
            th.start()

        # Wait until worker and all business-prediction runners get partition assignments
        dl = time.monotonic() + 15
        while time.monotonic() < dl:
            if worker_runner.tokens:
                break
            time.sleep(0.05)
        time.sleep(0.5)

        t_start = time.monotonic()
        t_warmup_end = t_start + warmup_seconds
        t_measure_end = t_warmup_end + measure_seconds

        def lag_sampler():
            admin = AdminClient(settings.client_config())
            tps_pred = [TopicPartition("inference.results", p) for p in range(12)]
            tps_live = [TopicPartition("sensor.readings", p) for p in range(12)]
            from confluent_kafka import ConsumerGroupTopicPartitions

            while not stop.is_set():
                time.sleep(1.0)
                now_wall = time.monotonic()
                if now_wall < t_warmup_end or now_wall > t_measure_end + 0.5:
                    continue
                try:
                    hi_pred = admin.list_offsets({tp: OffsetSpec.latest() for tp in tps_pred})
                    hi_live = admin.list_offsets({tp: OffsetSpec.latest() for tp in tps_live})
                    cg_pred_res = admin.list_consumer_group_offsets(
                        [ConsumerGroupTopicPartitions(biz_group_pred, tps_pred)]
                    )
                    cg_live_res = admin.list_consumer_group_offsets(
                        [ConsumerGroupTopicPartitions(biz_group_live, tps_live)]
                    )
                    pred_committed = cg_pred_res[biz_group_pred].result(timeout=3)
                    live_committed = cg_live_res[biz_group_live].result(timeout=3)
                    per_part_pred = {}
                    for p_res in pred_committed.topic_partitions:
                        hi = hi_pred[TopicPartition("inference.results", p_res.partition)].result(timeout=3).offset
                        off = p_res.offset if p_res.offset is not None and p_res.offset >= 0 else hi
                        per_part_pred[p_res.partition] = max(0, hi - off)
                    live_lag = 0
                    for p_res in live_committed.topic_partitions:
                        hi = hi_live[TopicPartition("sensor.readings", p_res.partition)].result(timeout=3).offset
                        off = p_res.offset if p_res.offset is not None and p_res.offset >= 0 else hi
                        live_lag += max(0, hi - off)
                    lag_samples.append(
                        {
                            "t_rel": round(now_wall - t_warmup_end, 2),
                            "pred_total_lag": sum(per_part_pred.values()),
                            "pred_max_part_lag": max(per_part_pred.values(), default=0),
                            "pred_per_partition": per_part_pred,
                            "live_total_lag": live_lag,
                        }
                    )
                except Exception:
                    pass

        t_lag = threading.Thread(target=lag_sampler, daemon=True)
        t_lag.start()

        epoch = f"epoch-{run_id}"
        seq_counter = {m: 0 for m in rates}
        example_reading = json.loads((ROOT / "contracts/kafka/examples/sensor-reading.json").read_text())
        base_features = example_reading["features"]
        next_due = {m: t_start + (idx * 0.002) for idx, m in enumerate(sorted(rates))}
        window_event_ids: list[str] = []
        all_event_ids: list[str] = []

        while True:
            now = time.monotonic()
            if not collector.active and now >= t_warmup_end:
                with collector.lock:
                    collector.active = True
            if now >= t_measure_end:
                with collector.lock:
                    collector.active = False
                break
            due_machines = [m for m, ts in next_due.items() if ts <= now]
            if not due_machines:
                soonest = min(next_due.values())
                time.sleep(min(0.005, max(0.0005, soonest - now)))
                continue
            for m in sorted(due_machines):
                seq_counter[m] += 1
                seq = seq_counter[m]
                event_id = f"evt-{run_id}-{m}-{seq}"
                features = dict(base_features)
                if (seq + hash(m)) % 10 == 0:
                    features["torque_nm"] = 62.5
                iso_now = datetime.now(timezone.utc).isoformat()
                payload = {
                    **example_reading,
                    "event_id": event_id,
                    "machine_id": m,
                    "stream_epoch": epoch,
                    "sequence_number": seq,
                    "received_at": iso_now,
                    "server_accepted_at": iso_now,
                    "correlation_id": f"corr-{event_id}",
                    "features": features,
                }
                ev = contracts.encode("sensor.readings", payload)
                pub_ts = time.monotonic()
                with trace.lock:
                    trace.published_at[event_id] = pub_ts
                    trace.event_machine[event_id] = m
                    trace.event_seq[event_id] = seq
                producer.produce(
                    ev.topic,
                    value=ev.wire_bytes,
                    key=ev.key.encode(),
                    headers=[
                        ("schema_version", ev.schema_version.encode()),
                        ("message_id", ev.message_id.encode()),
                        ("correlation_id", ev.payload.get("correlation_id", "").encode()),
                        ("producer_git_sha", settings.producer_git_sha.encode()),
                    ],
                )
                producer.poll(0)
                all_event_ids.append(event_id)
                if t_warmup_end <= pub_ts <= t_measure_end:
                    window_event_ids.append(event_id)
                next_due[m] += 1.0 / rates[m]
                if next_due[m] < now - 0.5:
                    next_due[m] = now + (1.0 / rates[m])

        producer.flush(10)
        total_sent = len(all_event_ids)
        dl = time.monotonic() + 90.0
        while time.monotonic() < dl:
            with pg_connect(proc_dsn) as pc, pg_connect(biz_dsn) as bc:
                pr = pc.execute("SELECT count(*) AS n FROM processing_results").fetchone()["n"]
                ob = pc.execute("SELECT count(*) AS n FROM maintai_kafka_outbox WHERE status!='PUBLISHED'").fetchone()["n"]
                bd = bc.execute("SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='inference.results'").fetchone()["n"]
                al_exp = pc.execute("SELECT count(*) AS n FROM maintai_kafka_outbox WHERE topic='alert.signals'").fetchone()["n"]
                al_done = bc.execute("SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='alert.signals'").fetchone()["n"]
            if pr >= total_sent and ob == 0 and bd >= total_sent and al_done >= al_exp:
                break
            time.sleep(0.1)

        time.sleep(0.2)
        stop.set()
        t_worker.join(timeout=10)
        t_relay.join(timeout=5)
        t_lag.join(timeout=3)
        t_notify.join(timeout=3)
        for th in biz_threads:
            th.join(timeout=10)

        # Verify sequence correctness in latest_valid_prediction
        with pg_connect(proc_dsn) as pc, pg_connect(biz_dsn) as bc:
            lvp_rows = bc.execute("SELECT machine_id, sequence_number FROM latest_valid_prediction").fetchall()
            exp_rows = pc.execute(
                "SELECT se.machine_id, MAX(se.sequence_number) AS max_seq FROM sensor_events se "
                "JOIN processing_results pr ON pr.event_id = se.event_id "
                "WHERE pr.kind='PREDICTION_COMPLETED' GROUP BY se.machine_id"
            ).fetchall()
        lvp_map = {r["machine_id"]: int(r["sequence_number"]) for r in lvp_rows}
        exp_map = {r["machine_id"]: int(r["max_seq"]) for r in exp_rows}
        wrong_latest_seq = sum(1 for m, exp in exp_map.items() if lvp_map.get(m) != exp)

        win_admitted = sum(1 for ts in trace.admitted_at.values() if t_warmup_end <= ts <= t_measure_end)
        win_outcomes = sum(1 for ts in trace.persist_end_at.values() if t_warmup_end <= ts <= t_measure_end)
        win_outbox_pub = sum(1 for ts in trace.kafka_result_pub_at.values() if t_warmup_end <= ts <= t_measure_end)
        win_preds = sum(1 for ts in trace.biz_pred_commit_at.values() if t_warmup_end <= ts <= t_measure_end)

        kafka_to_biz_ms = [
            (trace.biz_pred_commit_at[eid] - trace.kafka_result_pub_at[eid]) * 1000.0
            for eid in window_event_ids
            if eid in trace.kafka_result_pub_at and eid in trace.biz_pred_commit_at
        ]
        hot_e2e = [
            (trace.biz_pred_commit_at[eid] - trace.published_at[eid]) * 1000.0
            for eid in window_event_ids
            if trace.event_machine[eid] == "M-001" and eid in trace.biz_pred_commit_at
        ]
        bg_e2e = [
            (trace.biz_pred_commit_at[eid] - trace.published_at[eid]) * 1000.0
            for eid in window_event_ids
            if trace.event_machine[eid] != "M-001" and eid in trace.biz_pred_commit_at
        ]

        role_breakdown = {}
        for role in ("business-prediction", "business-live", "business-alert"):
            role_breakdown[role] = {
                "handler_wall_ms": summarize(collector.handler_wall_ms[role]),
                "db_tx_ms": summarize(collector.tx_ms[role]),
                "db_exec_ms": summarize(collector.exec_ms[role]),
                "db_commit_ms": summarize(collector.commit_ms[role]),
                "pool_wait_ms": summarize(collector.pool_wait_ms[role]),
                "kafka_commit_ms": summarize(collector.kafka_commit_ms[role]),
                "wake_delay_ms": summarize(collector.wake_delay_ms[role]),
                "poll_empty_sleep_ms": summarize(collector.poll_empty_sleep_ms[role]),
                "poll_calls_while_busy": collector.poll_calls_while_busy[role],
                "pending_queue_len": summarize([float(x) for x in collector.batch_queue_lens[role]]),
            }

        return {
            "mode": mode,
            "workload": workload,
            "biz_replicas": biz_replicas,
            "run_index": run_idx,
            "funnel_rps": {
                "target_rps": round(sum(rates.values()), 2),
                "sent_rps": round(len(window_event_ids) / measure_seconds, 2),
                "admitted_rps": round(win_admitted / measure_seconds, 2),
                "outcomes_rps": round(win_outcomes / measure_seconds, 2),
                "outbox_pub_rps": round(win_outbox_pub / measure_seconds, 2),
                "pred_consumed_rps": round(len(collector.pred_consumed_ts) / measure_seconds, 2),
                "biz_pred_commit_rps": round(win_preds / measure_seconds, 2),
            },
            "latency_ms": {
                "kafka_pub_to_biz_commit": summarize(kafka_to_biz_ms),
                "hot_e2e": summarize(hot_e2e),
                "bg_e2e": summarize(bg_e2e),
                "pg_notify_delivery": summarize(collector.notify_recv_lat_ms),
            },
            "role_breakdown": role_breakdown,
            "projector_substeps_ms": {k: summarize(v) for k, v in collector.proj_substeps.items()},
            "out_of_order": {
                "total_arrivals": collector.total_pred_arrivals,
                "out_of_order_arrivals": collector.out_of_order_arrivals,
                "ooo_pct": round((collector.out_of_order_arrivals / max(1, collector.total_pred_arrivals)) * 100.0, 2),
                "stale_pred_skipped_by_guard": collector.stale_pred_skipped,
                "wrong_latest_sequence": wrong_latest_seq,
            },
            "notifications": {
                "emitted": collector.notify_emitted_count,
                "received": collector.notify_received_count,
            },
            "lag_samples": lag_samples,
            "max_pred_lag": max((s["pred_total_lag"] for s in lag_samples), default=0),
            "end_pred_lag": lag_samples[-1]["pred_total_lag"] if lag_samples else 0,
            "max_live_lag": max((s["live_total_lag"] for s in lag_samples), default=0),
        }
    finally:
        stop.set()
        for t in [t_worker, t_relay, t_lag, t_notify, *biz_threads]:
            if t is not None and t.is_alive():
                t.join(timeout=10)
        relay_pub.close()
        producer.flush(5)
        proc_pool.close()
        for p in biz_pools:
            p.close()
        try:
            admin = AdminClient(settings.client_config())
            hi_futs = admin.list_offsets(
                {TopicPartition(t, p): OffsetSpec.latest() for t in ("sensor.readings", "inference.results", "alert.signals") for p in range(12)}
            )
            del_tps = [TopicPartition(tp.topic, tp.partition, fut.result(timeout=5).offset) for tp, fut in hi_futs.items()]
            for fut in admin.delete_records(del_tps).values():
                fut.result(timeout=5)
        except Exception:
            pass
        with pg_connect(database_url) as conn:
            conn.autocommit = True
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(proc_db)))
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(biz_db)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default="postgresql://maintai_test:maintai_test@127.0.0.1:35432/maintai_processing_test")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:39092")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--matrix", default="", help="Optional filter e.g. baseline:A:1,hybrid:A:1,hybrid:A:2,hybrid:C:1,hybrid:C:2")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    matrix = [
        ("baseline", "A", 1),
        ("hybrid", "A", 1),
        ("hybrid", "A", 2),
        ("hybrid", "A", 4),
        ("hybrid", "C", 1),
        ("hybrid", "C", 2),
        ("hybrid", "C", 4),
    ]
    if args.matrix:
        wanted = {tuple(item.split(":")) for item in args.matrix.split(",") if item.strip()}
        matrix = [(m, w, r) for (m, w, r) in matrix if (m, w, str(r)) in wanted]
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    runs_out = []
    for mode, wl, reps in matrix:
        for r in range(1, args.runs + 1):
            print(f"[PHASE3] mode={mode} workload={wl} biz_replicas={reps} run={r}/{args.runs}", flush=True)
            res = run_biz_trial(
                mode=mode,
                workload=wl,
                biz_replicas=reps,
                run_idx=r,
                database_url=args.database_url,
                bootstrap_servers=args.bootstrap_servers,
            )
            runs_out.append(res)
            out_path.write_text(json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), "runs": runs_out}, indent=2) + "\n")
            f = res["funnel_rps"]
            bp = res["role_breakdown"]["business-prediction"]
            print(
                f"  -> out={f['outcomes_rps']} pred_commit={f['biz_pred_commit_rps']} "
                f"k2b_p95={res['latency_ms']['kafka_pub_to_biz_commit']['p95']}ms "
                f"db_tx_p50={bp['db_tx_ms']['p50']}ms kcommit_p50={bp['kafka_commit_ms']['p50']}ms "
                f"wake_p50={bp['wake_delay_ms']['p50']}ms pool_w_p95={bp['pool_wait_ms']['p95']}ms "
                f"max_lag={res['max_pred_lag']} ooo={res['out_of_order']['ooo_pct']}% wrong={res['out_of_order']['wrong_latest_sequence']}",
                flush=True,
            )
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
