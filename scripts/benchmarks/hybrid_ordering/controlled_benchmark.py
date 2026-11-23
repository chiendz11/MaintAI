#!/usr/bin/env python3
"""Controlled PostgreSQL 17 + Kafka 4.0.0 benchmark harness for Hybrid Ordering.

Runs real MaintAI Processing Worker (`KeyedConsumerRunner` + `KeyedProcessingHandler`),
`OrderedOutboxRelay`, and Business Consumers (`ProjectionHandler` + `AlertHandler` +
`AlertRepository`) against isolated PostgreSQL databases and real 12-partition Kafka topics.

Explicitly distinguishes controlled local harness metrics (single worker replica with
parameterized ML latency & concurrency) from full multi-pod k3d cluster benchmarks.
"""

from __future__ import annotations

import argparse
import csv
import inspect
import json
import os
import random
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from alembic import command
from alembic.config import Config
from confluent_kafka import ConsumerGroupTopicPartitions, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, OffsetSpec
from prometheus_client import CollectorRegistry
from psycopg import sql
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[3]
for rel in ("", "processing/worker/src", "processing/outbox-relay/src", "backend/business-consumer/src"):
    path_str = str(ROOT / rel) if rel else str(ROOT)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from backend.shared.alerts.domain import validate_evidence  # noqa: E402
from backend.shared.alerts.handler import AlertHandler  # noqa: E402
from backend.shared.alerts.repository import AlertRepository  # noqa: E402
from backend.shared.inbox.handler import ProjectionHandler  # noqa: E402
from backend.shared.persistence.migration import upgrade as upgrade_business  # noqa: E402
from backend.shared.persistence.pool import ConnectionPool as BusinessPool  # noqa: E402
from backend.shared.persistence.registry import RegistryRepository  # noqa: E402
from backend.shared.projections.projector import Projector  # noqa: E402
from consumer.handler import KeyedProcessingHandler, ProcessingHandler  # noqa: E402
from consumer.observability import RuntimeObservability  # noqa: E402
from maintai_messaging.config import KafkaSettings, KeyedSettings  # noqa: E402
from maintai_messaging.contracts import canonical_json, default_contracts  # noqa: E402
from maintai_messaging.keyed import KeyedConsumerRunner  # noqa: E402
from maintai_messaging.metrics import OutboxOrderMetrics, ProcessingMetrics  # noqa: E402
from maintai_messaging.ordering import OrderedOutboxRelay, activate_processing_ordering  # noqa: E402
from maintai_messaging.outcomes import ModelIdentity  # noqa: E402
from maintai_messaging.postgres import connect as pg_connect  # noqa: E402
from maintai_messaging.producer import KafkaPublisher  # noqa: E402
from maintai_messaging.service import serve_consumers  # noqa: E402
from processing.shared.data_quality.machine_registry import MachineLookupResult  # noqa: E402
from processing.shared.data_quality.validator import FeatureValidator  # noqa: E402
from processing.shared.inference_client.client import PredictionAttempt, PredictionResponse  # noqa: E402
from processing.shared.persistence.pool import ConnectionPool as ProcessingPool  # noqa: E402
from processing.shared.risk_engine.policy import RiskPolicy  # noqa: E402


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    idx = (pct / 100.0) * (len(ordered) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return round(ordered[lo] * (1.0 - frac) + ordered[hi] * frac, 3)


def summarize(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None}
    return {
        "count": len(values),
        "mean": round(statistics.fmean(values), 3),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": round(max(values), 3),
    }


def rss_mb() -> float:
    with open("/proc/self/statm", encoding="utf-8") as fh:
        pages = int(fh.read().split()[1])
    return round((pages * os.sysconf("SC_PAGE_SIZE")) / (1024 * 1024), 2)


def upgrade_processing(dsn: str):
    url = make_url(dsn)
    config = Config()
    config.set_main_option("script_location", str(ROOT / "processing/migrations"))
    engine = create_engine(url.set(drivername="postgresql+psycopg"))
    try:
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()


def ensure_topics_and_groups(settings: KafkaSettings, contracts):
    admin = AdminClient(settings.client_config())
    names = ("sensor.readings", "inference.results", "alert.signals")
    for fut in admin.create_topics(
        [
            NewTopic(
                name,
                num_partitions=contracts.topology["topics"][name]["partitions"],
                replication_factor=1,
                config={"min.insync.replicas": "1"},
            )
            for name in names
        ]
    ).values():
        try:
            fut.result(timeout=15)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    deadline = time.monotonic() + 30
    while True:
        md = admin.list_topics(timeout=3)
        ready = True
        for name in names:
            t = md.topics.get(name)
            if t is None or t.error is not None or len(t.partitions) != 12:
                ready = False
                break
            if not all(p.leader >= 0 and p.error is None for p in t.partitions.values()):
                ready = False
                break
        if ready:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("Kafka 12-partition topics did not become ready")
        time.sleep(0.1)

    requested = {(name, p) for name in names for p in range(12)}
    futures = admin.list_offsets(
        {TopicPartition(name, p): OffsetSpec.latest() for name, p in sorted(requested)}
    )
    captured = {tp: fut.result(timeout=10).offset for tp, fut in futures.items()}
    for role in ("processing", "business-live", "business-prediction", "business-alert"):
        prof = contracts.topology["profiles"][role]
        topic = prof["topic"]
        parts = [TopicPartition(topic, p, captured[TopicPartition(topic, p)]) for p in range(12)]
        req = ConsumerGroupTopicPartitions(settings.group_prefix + prof["group_id"], parts)
        for fut in admin.alter_consumer_group_offsets([req]).values():
            reply = fut.result(timeout=10)
            for p in reply.topic_partitions or []:
                if p.error is not None:
                    raise KafkaException(p.error)
    return captured


@dataclass
class RunTrace:
    published_at: dict[str, float] = field(default_factory=dict)
    event_machine: dict[str, str] = field(default_factory=dict)
    event_seq: dict[str, int] = field(default_factory=dict)
    admitted_at: dict[str, float] = field(default_factory=dict)
    inf_start_at: dict[str, float] = field(default_factory=dict)
    inf_end_at: dict[str, float] = field(default_factory=dict)
    persist_start_at: dict[str, float] = field(default_factory=dict)
    persist_end_at: dict[str, float] = field(default_factory=dict)
    kafka_result_pub_at: dict[str, float] = field(default_factory=dict)
    kafka_alert_pub_at: dict[str, float] = field(default_factory=dict)
    alert_pub_sequence: list[tuple[str, int, str]] = field(default_factory=list)
    biz_pred_commit_at: dict[str, float] = field(default_factory=dict)
    biz_alert_commit_at: dict[str, float] = field(default_factory=dict)
    stale_pred_updates: int = 0
    offset_commit_violations: int = 0
    admitted_coords: dict[tuple[int, int], str] = field(default_factory=dict)
    latest_seen_pred_seq: dict[str, int] = field(default_factory=dict)
    ml_active: int = 0
    ml_peak: int = 0
    errors: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class ControlledMLProvider:
    def __init__(
        self,
        *,
        trace: RunTrace,
        model_identity: ModelIdentity,
        base_ms: float,
        jitter_ms: float,
        slow_prob: float,
        slow_ms: float,
        max_concurrency: int,
        seed: int,
    ):
        self.trace = trace
        self.identity = model_identity
        self.base_ms = base_ms
        self.jitter_ms = jitter_ms
        self.slow_prob = slow_prob
        self.slow_ms = slow_ms
        self.semaphore = threading.BoundedSemaphore(max(1, max_concurrency))
        self.rng = random.Random(seed)
        self.rng_lock = threading.Lock()

    def predict(self, *, event_id: str, machine_id: str, features: dict, correlation_id: str):
        started = time.monotonic()
        seq = self.trace.event_seq.get(event_id, 0)
        with self.rng_lock:
            is_slow = (machine_id == "M-001" and seq % 5 == 1 and self.slow_prob > 0) or (
                self.rng.random() < self.slow_prob
            )
            jitter = self.rng.uniform(-self.jitter_ms, self.jitter_ms)
        target_ms = max(1.0, (self.slow_ms if is_slow else self.base_ms) + jitter)

        with self.semaphore:
            with self.trace.lock:
                self.trace.inf_start_at[event_id] = time.monotonic()
                self.trace.ml_active += 1
                if self.trace.ml_active > self.trace.ml_peak:
                    self.trace.ml_peak = self.trace.ml_active
            try:
                time.sleep(target_ms / 1000.0)
                is_high = float(features.get("torque_nm", 40.0)) >= 60.0
                prob = 0.88 if is_high else 0.12
                pred = "FAILURE" if prob >= 0.5 else "NORMAL"
                elapsed_ms = (time.monotonic() - started) * 1000.0
                response = PredictionResponse.model_validate_json(
                    json.dumps(
                        {
                            "prediction_id": "pred-" + event_id,
                            "event_id": event_id,
                            "prediction": pred,
                            "failure_probability": prob,
                            "model_threshold": 0.5,
                            "model_version": self.identity.model_version,
                            "preprocessor_version": self.identity.preprocessor_version,
                            "inference_ms": round(elapsed_ms, 3),
                            "predicted_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                )
                return PredictionAttempt(response, None, 1)
            finally:
                with self.trace.lock:
                    self.trace.ml_active -= 1
                    self.trace.inf_end_at[event_id] = time.monotonic()


class LocalEvidenceClient:
    """Fetches canonical HistoryRecord directly from isolated processing_db for AlertRepository."""

    def __init__(self, proc_pool: ProcessingPool, contracts):
        self.proc_pool = proc_pool
        self.contracts = contracts

    def fetch(self, signal_payload: dict) -> dict:
        event_id = signal_payload["event_id"]
        with self.proc_pool() as conn:
            row = conn.execute(
                """SELECT se.event_id, se.machine_id, se.source_server_accepted_at,
                          se.raw_features_json, pr.result_id, pr.outcome,
                          rd.risk_decision_id, rd.machine_state, rd.policy_version, rd.risk_score
                   FROM sensor_events se
                   JOIN processing_results pr ON pr.event_id = se.event_id
                   JOIN risk_decisions rd ON rd.event_id = se.event_id
                   WHERE se.event_id = %s""",
                (event_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError(f"Evidence missing for {event_id}")
        outcome = self.contracts.decode("inference.results", bytes(row["outcome"]), writer=True).payload
        value = {
            "schema_version": "1.0.0",
            "result_id": row["result_id"],
            "source_event_id": row["event_id"],
            "machine_id": row["machine_id"],
            "source_server_accepted_at": row["source_server_accepted_at"].isoformat(),
            "outcome": outcome,
            "features_raw": row["raw_features_json"],
            "raw_ref": None,
            "risk_snapshot": {
                "risk_decision_id": row["risk_decision_id"],
                "machine_state": row["machine_state"],
                "policy_version": row["policy_version"],
                "risk_score": row["risk_score"],
            },
        }
        return validate_evidence(signal_payload, value)

    def close(self):
        return None


def build_keyed_limits(args) -> KeyedSettings:
    sig = inspect.signature(KeyedSettings)
    kwargs = {
        "max_active_keys": args.max_active_keys,
        "max_pending_records": args.max_pending_records,
        "max_pending_per_key": args.max_pending_per_key,
        "max_uncommitted_per_partition": args.max_uncommitted_per_partition,
        "handler_timeout_seconds": 60.0,
        "drain_timeout_seconds": 60.0,
    }
    if "hybrid_ordering_enabled" in sig.parameters:
        kwargs["hybrid_ordering_enabled"] = args.mode == "hybrid"
    if "max_inflight_per_key" in sig.parameters:
        kwargs["max_inflight_per_key"] = args.max_inflight_per_key
    if "max_total_inflight" in sig.parameters and args.max_total_inflight is not None:
        kwargs["max_total_inflight"] = args.max_total_inflight
    return KeyedSettings(**kwargs)


def workload_rates(workload: str, machines: int) -> dict[str, float]:
    ids = [f"M-{i:03d}" for i in range(1, machines + 1)]
    if workload == "A":
        return {m: 1.0 for m in ids}
    if workload == "B":
        return {m: 2.0 for m in ids}
    if workload == "C":
        rates = {m: 1.0 for m in ids}
        rates["M-001"] = 16.0
        return rates
    raise ValueError(f"Unsupported workload {workload}")


def run_single(args, workload: str, run_idx: int) -> dict:
    run_id = uuid4().hex[:12]
    proc_db = f"bench_proc_{run_id}"
    biz_db = f"bench_biz_{run_id}"
    base_url = make_url(args.database_url)
    proc_dsn = base_url.set(database=proc_db).render_as_string(hide_password=False)
    biz_dsn = base_url.set(database=biz_db).render_as_string(hide_password=False)

    with pg_connect(args.database_url) as conn:
        conn.autocommit = True
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(proc_db)))
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(biz_db)))

    contracts = default_contracts()
    upgrade_processing(proc_dsn)
    with pg_connect(proc_dsn) as conn, conn.transaction():
        activate_processing_ordering(conn, legacy_writers_drained=True)

    upgrade_business(biz_dsn)
    rates = workload_rates(workload, args.machines)
    with pg_connect(biz_dsn) as conn:
        RegistryRepository(lambda: pg_connect(biz_dsn)).provision(
            [{"machine_id": m, "display_name": f"Benchmark {m}"} for m in sorted(rates)]
        )

    settings = KafkaSettings(
        local_mode=True,
        security_protocol="PLAINTEXT",
        bootstrap_servers=args.bootstrap_servers,
        group_prefix=f"bench-{run_id}-",
        poll_interval_seconds=0.01,
        producer_git_sha="a" * 40,
    )
    captured_offsets = ensure_topics_and_groups(settings, contracts)

    limits = build_keyed_limits(args)
    inflight_budget = getattr(limits, "max_total_inflight", None) or (
        limits.max_active_keys * getattr(limits, "max_inflight_per_key", 1)
    )
    proc_pool = ProcessingPool(proc_dsn, size=max(12, min(32, inflight_budget + 4)))
    biz_pool = BusinessPool(biz_dsn, size=12)

    trace = RunTrace()
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
        base_ms=args.ml_latency_ms,
        jitter_ms=args.ml_jitter_ms,
        slow_prob=args.ml_slow_prob,
        slow_ms=args.ml_slow_ms,
        max_concurrency=args.ml_max_concurrency,
        seed=1000 + run_idx,
    )
    validator = FeatureValidator.from_file(
        ROOT / "contracts/v1.3/schemas/features.schema.json", version="sensor-validator-1.0.0"
    )
    policy = RiskPolicy.from_file(ROOT / "processing/config/risk-policies/demo-risk-v1.json")

    proc_group = settings.group_prefix + contracts.topology["profiles"]["processing"]["group_id"]

    class InstrumentedProcessingHandler(ProcessingHandler):
        def persist_prepared(self, connection, event, record, outbox, prepared):
            with trace.lock:
                trace.persist_start_at[event.message_id] = time.monotonic()
            res = super().persist_prepared(connection, event, record, outbox, prepared)
            with trace.lock:
                trace.persist_end_at[event.message_id] = time.monotonic()
            return res

    app_handler = InstrumentedProcessingHandler(
        lambda: proc_pool(),
        consumer_group=proc_group,
        validator=validator,
        registry_lookup=lambda _machine_id, **_kw: MachineLookupResult("FOUND", None),
        predict=ml.predict,
        identity_resolver=lambda _resp: model_identity,
        policy=policy,
        producer_git_sha="a" * 40,
        registry_cache_seconds=30,
        contracts=contracts,
    )

    class BenchKeyedHandler(KeyedProcessingHandler):
        def handle(self, event, record, context):
            with trace.lock:
                if event.message_id not in trace.admitted_at:
                    trace.admitted_at[event.message_id] = time.monotonic()
                trace.admitted_coords[(record.partition, record.offset)] = event.message_id
            return super().handle(event, record, context)

    def on_err():
        with trace.lock:
            trace.errors += 1

    keyed_handler = BenchKeyedHandler(app_handler, on_error=on_err)
    proc_registry = CollectorRegistry()
    proc_metrics = ProcessingMetrics(proc_registry)

    class VerifiedKeyedRunner(KeyedConsumerRunner):
        def _commit(self):
            for partition, entries in self.ledger.candidates():
                with trace.lock:
                    for off in range(entries[0].record.offset, entries[-1].record.offset + 1):
                        eid = trace.admitted_coords.get((partition.partition, off))
                        if eid is not None and eid not in trace.persist_end_at:
                            trace.offset_commit_violations += 1
            return super()._commit()

    runner = VerifiedKeyedRunner(
        settings,
        "processing",
        keyed_handler,
        limits=limits,
        metrics=proc_metrics,
        contracts=contracts,
    )

    # Relay with publish timestamp tracking & alert source-order capture
    class TrackingPublisher:
        def __init__(self, inner: KafkaPublisher):
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
                    seq = trace.event_seq.get(src_id, -1)
                    trace.alert_pub_sequence.append((event.key, seq, src_id))
            return out

    # Instrumented Business handlers
    class TrackingProjector(Projector):
        def __call__(self, connection, event, record, outbox):
            res = super().__call__(connection, event, record, outbox)
            if event.topic == "inference.results":
                now = time.monotonic()
                src_id = event.source_event_id
                seq = int(event.payload["sequence_number"])
                with trace.lock:
                    trace.biz_pred_commit_at.setdefault(src_id, now)
                    if event.kind == "PREDICTION_COMPLETED":
                        prev = trace.latest_seen_pred_seq.get(event.key, -1)
                        # Check authoritative DB row instead of RAM arrival order
                        row = connection.execute(
                            "SELECT sequence_number FROM latest_valid_prediction WHERE machine_id=%s",
                            (event.key,),
                        ).fetchone()
                        if row is not None:
                            db_seq = int(row["sequence_number"])
                            if db_seq < prev:
                                trace.stale_pred_updates += 1
                            trace.latest_seen_pred_seq[event.key] = db_seq
            return res

    class TrackingAlertRepo(AlertRepository):
        def create_or_dedupe(self, connection, event, evidence):
            res = super().create_or_dedupe(connection, event, evidence)
            with trace.lock:
                trace.biz_alert_commit_at.setdefault(event.payload["event_id"], time.monotonic())
            return res

    evidence_client = LocalEvidenceClient(proc_pool, contracts)
    biz_group_live = settings.group_prefix + contracts.topology["profiles"]["business-live"]["group_id"]
    biz_group_pred = settings.group_prefix + contracts.topology["profiles"]["business-prediction"]["group_id"]
    biz_group_alert = settings.group_prefix + contracts.topology["profiles"]["business-alert"]["group_id"]
    projector = TrackingProjector()
    alert_repo = TrackingAlertRepo(lambda: biz_pool())
    biz_handlers = {
        "business-live": ProjectionHandler(lambda: biz_pool(), biz_group_live, topic="sensor.readings", projector=projector),
        "business-prediction": ProjectionHandler(lambda: biz_pool(), biz_group_pred, topic="inference.results", projector=projector),
        "business-alert": AlertHandler(lambda: biz_pool(), biz_group_alert, evidence_client, repository=alert_repo),
    }

    stop = threading.Event()
    samples: list[dict] = []
    relay_pub = KafkaPublisher(settings, "outbox")
    from confluent_kafka import Producer as RawKafkaProducer

    producer = RawKafkaProducer({**settings.producer_config(), "linger.ms": 2})
    t_worker = t_relay = t_biz = t_sampler = None

    try:
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
                    with trace.lock:
                        trace.errors += 1
                    stop.wait(0.05)

        t_worker = threading.Thread(target=lambda: runner.run(stop), daemon=True, name="bench-worker")
        t_relay = threading.Thread(target=relay_loop, daemon=True, name="bench-relay")
        t_biz = threading.Thread(
            target=lambda: serve_consumers(settings, biz_handlers, stop, contracts=contracts),
            daemon=True,
            name="bench-biz",
        )
        t_worker.start()
        t_relay.start()
        t_biz.start()

        # Wait up to 10s until Processing runner receives Kafka partition assignment
        assign_deadline = time.monotonic() + 10
        while not runner.tokens and time.monotonic() < assign_deadline:
            time.sleep(0.05)

        epoch = f"epoch-{run_id}"
        seq_counter = {m: 0 for m in rates}
        example_reading = json.loads((ROOT / "contracts/kafka/examples/sensor-reading.json").read_text())
        base_features = example_reading["features"]

        t_start = time.monotonic()
        t_warmup_end = t_start + args.warmup_seconds
        t_measure_end = t_warmup_end + args.measure_seconds
        total_duration = args.warmup_seconds + args.measure_seconds

        # Sampler thread
        def sampler():
            prev_cpu = sum(os.times()[:2])
            prev_wall = time.monotonic()
            while not stop.is_set():
                time.sleep(0.25)
                now_wall = time.monotonic()
                now_cpu = sum(os.times()[:2])
                dt = max(0.001, now_wall - prev_wall)
                cpu_pct = round(((now_cpu - prev_cpu) / dt) * 100.0, 2)
                prev_cpu, prev_wall = now_cpu, now_wall
                snap = dict(runner.snapshot)
                with pg_connect(proc_dsn) as conn:
                    outbox_rows = conn.execute(
                        "SELECT topic, count(*) AS cnt, COALESCE(MAX(EXTRACT(EPOCH FROM (clock_timestamp() - created_at))), 0) AS oldest_s "
                        "FROM maintai_kafka_outbox WHERE status != 'PUBLISHED' GROUP BY topic"
                    ).fetchall()
                    outbox_by_topic = {
                        r["topic"]: (r["cnt"], float(r["oldest_s"])) for r in outbox_rows
                    }
                    # Reorder buffer exists when processing_alert_ledger table is deployed (Phase 1+)
                    has_ledger = conn.execute(
                        "SELECT to_regclass('processing_alert_ledger') IS NOT NULL AS exists"
                    ).fetchone()["exists"]
                    if has_ledger:
                        ledger_row = conn.execute(
                            "SELECT count(*) FILTER (WHERE disposition NOT IN ('EMITTED','RESOLVED_NO_SIGNAL')) AS depth, "
                            "COALESCE(MAX(EXTRACT(EPOCH FROM (clock_timestamp() - admitted_at)) * 1000.0) "
                            "FILTER (WHERE disposition IN ('SIGNAL_READY','NO_SIGNAL','BLOCKED')), 0.0) AS wait_ms "
                            "FROM processing_alert_ledger"
                        ).fetchone()
                        reorder_depth = int(ledger_row["depth"])
                        frontier_wait_ms = float(ledger_row["wait_ms"])
                    else:
                        reorder_depth = 0
                        frontier_wait_ms = 0.0
                    pg_stats = conn.execute(
                        "SELECT count(*) FILTER (WHERE datname IN (%s, %s)) AS total_conn, "
                        "count(*) FILTER (WHERE datname IN (%s, %s) AND state='active') AS active_conn, "
                        "count(*) FILTER (WHERE datname IN (%s, %s) AND wait_event_type='Lock') AS lock_wait "
                        "FROM pg_stat_activity",
                        (proc_db, biz_db, proc_db, biz_db, proc_db, biz_db),
                    ).fetchone()
                if now_wall >= t_warmup_end and now_wall <= t_measure_end + 1.0:
                    with trace.lock:
                        ml_cur = trace.ml_active
                    samples.append(
                        {
                            "t_rel": round(now_wall - t_warmup_end, 2),
                            "pending_records": snap.get("pending_records", 0),
                            "active_keys": snap.get("active_keys", 0),
                            "active_inflight": snap.get("active_inflight", snap.get("active_keys", 0)),
                            "max_per_key_inflight": snap.get("max_per_key_inflight", 1 if snap.get("active_keys", 0) else 0),
                            "ml_active": ml_cur,
                            "outbox_pending": sum(r["cnt"] for r in outbox_rows),
                            "outbox_pending_results": outbox_by_topic.get("inference.results", (0, 0.0))[0],
                            "outbox_pending_alerts": outbox_by_topic.get("alert.signals", (0, 0.0))[0],
                            "oldest_outbox_s": max((float(r["oldest_s"]) for r in outbox_rows), default=0.0),
                            "alert_reorder_depth": reorder_depth,
                            "alert_frontier_wait_ms": round(frontier_wait_ms, 3),
                            "pg_conns": pg_stats["total_conn"],
                            "pg_active": pg_stats["active_conn"],
                            "pg_lock_waits": pg_stats["lock_wait"],
                            "cpu_pct": cpu_pct,
                            "rss_mb": rss_mb(),
                        }
                    )

        t_sampler = threading.Thread(target=sampler, daemon=True, name="bench-sampler")
        t_sampler.start()

        # Scheduled multi-machine generator
        next_due = {m: t_start + (idx * 0.002) for idx, m in enumerate(sorted(rates))}
        window_event_ids: list[str] = []
        all_event_ids: list[str] = []

        while True:
            now = time.monotonic()
            if now >= t_measure_end:
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
                is_alert_candidate = (seq + hash(m)) % 10 == 0
                features = dict(base_features)
                if is_alert_candidate:
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
        # Wait for full drain in Processing + Outbox + Business DB (up to 90s)
        drain_deadline = time.monotonic() + 90.0
        while time.monotonic() < drain_deadline:
            with pg_connect(proc_dsn) as pconn, pg_connect(biz_dsn) as bconn:
                proc_done = pconn.execute("SELECT count(*) AS n FROM processing_results").fetchone()["n"]
                outbox_rem = pconn.execute(
                    "SELECT count(*) AS n FROM maintai_kafka_outbox WHERE status != 'PUBLISHED'"
                ).fetchone()["n"]
                pred_done = bconn.execute(
                    "SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='inference.results'"
                ).fetchone()["n"]
                alert_done = bconn.execute(
                    "SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='alert.signals'"
                ).fetchone()["n"]
                alert_expected = pconn.execute(
                    "SELECT count(*) AS n FROM maintai_kafka_outbox WHERE topic='alert.signals'"
                ).fetchone()["n"]
            if (
                proc_done >= total_sent
                and outbox_rem == 0
                and pred_done >= total_sent
                and alert_done >= alert_expected
            ):
                break
            time.sleep(0.1)
        time.sleep(0.2)
        stop.set()
        t_worker.join(timeout=10)
        t_relay.join(timeout=5)
        t_biz.join(timeout=10)
        t_sampler.join(timeout=2)

        # Compute metrics strictly for window_event_ids (steady-state measurement window)
        win_set = set(window_event_ids)
        inf_lat = []
        db_tx_lat = []
        inf_to_db = []
        db_to_kafka_res = []
        kafka_to_biz_pred = []
        sig_to_alert = []
        hot_e2e_pred = []
        bg_e2e_pred = []

        for eid in window_event_ids:
            if eid in trace.inf_start_at and eid in trace.inf_end_at:
                inf_lat.append((trace.inf_end_at[eid] - trace.inf_start_at[eid]) * 1000.0)
            if eid in trace.persist_start_at and eid in trace.persist_end_at:
                db_tx_lat.append((trace.persist_end_at[eid] - trace.persist_start_at[eid]) * 1000.0)
            if eid in trace.inf_end_at and eid in trace.persist_end_at:
                inf_to_db.append((trace.persist_end_at[eid] - trace.inf_end_at[eid]) * 1000.0)
            if eid in trace.persist_end_at and eid in trace.kafka_result_pub_at:
                db_to_kafka_res.append((trace.kafka_result_pub_at[eid] - trace.persist_end_at[eid]) * 1000.0)
            if eid in trace.kafka_result_pub_at and eid in trace.biz_pred_commit_at:
                kafka_to_biz_pred.append((trace.biz_pred_commit_at[eid] - trace.kafka_result_pub_at[eid]) * 1000.0)
            if eid in trace.kafka_alert_pub_at and eid in trace.biz_alert_commit_at:
                sig_to_alert.append((trace.biz_alert_commit_at[eid] - trace.kafka_alert_pub_at[eid]) * 1000.0)
            if eid in trace.published_at and eid in trace.biz_pred_commit_at:
                e2e = (trace.biz_pred_commit_at[eid] - trace.published_at[eid]) * 1000.0
                if trace.event_machine[eid] == "M-001":
                    hot_e2e_pred.append(e2e)
                else:
                    bg_e2e_pred.append(e2e)

        # Correctness invariants on DB & Kafka traces
        with pg_connect(proc_dsn) as pconn, pg_connect(biz_dsn) as bconn:
            total_proc_results = pconn.execute("SELECT count(*) AS n FROM processing_results").fetchone()["n"]
            pred_completed = pconn.execute(
                "SELECT count(*) AS n FROM processing_results WHERE kind='PREDICTION_COMPLETED'"
            ).fetchone()["n"]
            dup_outcomes = pconn.execute(
                "SELECT count(*) AS n FROM (SELECT event_id FROM processing_results GROUP BY event_id HAVING count(*) > 1) s"
            ).fetchone()["n"]
            dup_alerts = bconn.execute(
                "SELECT count(*) AS n FROM (SELECT signal_id FROM alert_signal_links GROUP BY signal_id HAVING count(*) > 1) s"
            ).fetchone()["n"]
            lvp_rows = bconn.execute("SELECT machine_id, sequence_number FROM latest_valid_prediction").fetchall()
            expected_lvp = pconn.execute(
                "SELECT se.machine_id, MAX(se.sequence_number) AS max_seq FROM sensor_events se "
                "JOIN processing_results pr ON pr.event_id = se.event_id "
                "WHERE pr.kind='PREDICTION_COMPLETED' GROUP BY se.machine_id"
            ).fetchall()

        lvp_map = {r["machine_id"]: int(r["sequence_number"]) for r in lvp_rows}
        exp_map = {r["machine_id"]: int(r["max_seq"]) for r in expected_lvp}
        wrong_latest_seq = sum(1 for m, exp in exp_map.items() if lvp_map.get(m) != exp)

        alert_seq_violations = 0
        last_alert_seq: dict[str, int] = {}
        for m, seq, _ in trace.alert_pub_sequence:
            if seq <= last_alert_seq.get(m, -1):
                alert_seq_violations += 1
            last_alert_seq[m] = seq

        # Read back actual alert.signals Kafka broker logs from captured start offsets
        from confluent_kafka import Consumer as RawKafkaConsumer

        broker_reader = RawKafkaConsumer(
            {
                **settings.client_config(),
                "group.id": f"bench-verify-{run_id}",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )
        try:
            alert_tps = [
                TopicPartition("alert.signals", p, captured_offsets[TopicPartition("alert.signals", p)])
                for p in range(12)
            ]
            broker_reader.assign(alert_tps)
            end_offsets = {}
            for p in range(12):
                _, hi = broker_reader.get_watermark_offsets(TopicPartition("alert.signals", p), timeout=5)
                end_offsets[p] = hi
            broker_last_seq: dict[str, int] = {}
            read_deadline = time.monotonic() + 10.0
            while time.monotonic() < read_deadline and any(
                broker_reader.position([TopicPartition("alert.signals", p)])[0].offset < end_offsets[p]
                for p in range(12)
                if captured_offsets[TopicPartition("alert.signals", p)] < end_offsets[p]
            ):
                msg = broker_reader.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                ev = contracts.decode("alert.signals", msg.value())
                src_seq = trace.event_seq.get(ev.payload["event_id"], -1)
                if src_seq > 0:
                    if src_seq <= broker_last_seq.get(ev.key, -1):
                        alert_seq_violations += 1
                    broker_last_seq[ev.key] = src_seq
        finally:
            broker_reader.close()

        win_admitted = sum(
            1 for eid, ts in trace.admitted_at.items() if t_warmup_end <= ts <= t_measure_end
        )
        win_outcomes = sum(
            1 for eid, ts in trace.persist_end_at.items() if t_warmup_end <= ts <= t_measure_end
        )
        win_preds = sum(
            1 for eid, ts in trace.biz_pred_commit_at.items() if t_warmup_end <= ts <= t_measure_end
        )
        with pg_connect(proc_dsn) as pconn:
            has_ledger = pconn.execute(
                "SELECT to_regclass('processing_alert_ledger') IS NOT NULL AS exists"
            ).fetchone()["exists"]
            if has_ledger:
                fw_rows = pconn.execute(
                    "SELECT EXTRACT(EPOCH FROM (resolved_at - decided_at)) * 1000.0 AS wait_ms "
                    "FROM processing_alert_ledger WHERE decided_at IS NOT NULL AND resolved_at IS NOT NULL"
                ).fetchall()
                ledger_frontier_wait_ms = [max(0.0, float(r["wait_ms"])) for r in fw_rows]
            else:
                ledger_frontier_wait_ms = []

        return {
            "workload": workload,
            "run_index": run_idx,
            "mode": args.mode,
            "config": {
                "max_active_keys": args.max_active_keys,
                "max_inflight_per_key": args.max_inflight_per_key,
                "max_total_inflight": inflight_budget,
                "ml_max_concurrency": args.ml_max_concurrency,
                "ml_latency_ms": args.ml_latency_ms,
                "ml_slow_prob": args.ml_slow_prob,
                "ml_slow_ms": args.ml_slow_ms,
                "warmup_seconds": args.warmup_seconds,
                "measure_seconds": args.measure_seconds,
                "machines": args.machines,
            },
            "throughput": {
                "target_rps": round(sum(rates.values()), 2),
                "total_sent_with_warmup": total_sent,
                "total_terminal_outcomes": total_proc_results,
                "total_predictions_completed": pred_completed,
                "window_sent": len(window_event_ids),
                "window_admitted_rps": round(win_admitted / args.measure_seconds, 2),
                "window_outcomes_rps": round(win_outcomes / args.measure_seconds, 2),
                "window_predictions_rps": round(win_preds / args.measure_seconds, 2),
                "errors": trace.errors,
            },
            "latency_ms": {
                "inference": summarize(inf_lat),
                "processing_db_tx": summarize(db_tx_lat),
                "inference_to_db_commit": summarize(inf_to_db),
                "db_commit_to_kafka_publish": summarize(db_to_kafka_res),
                "kafka_publish_to_business_db": summarize(kafka_to_biz_pred),
                "alert_signal_to_alert_creation": summarize(sig_to_alert),
                "alert_frontier_wait": summarize(ledger_frontier_wait_ms),
                "hot_machine_pred_e2e": summarize(hot_e2e_pred),
                "background_machine_pred_e2e": summarize(bg_e2e_pred),
            },
            "resources_and_queues": {
                "ml_peak_concurrency": trace.ml_peak,
                "max_active_inflight": max((s["active_inflight"] for s in samples), default=0),
                "max_per_key_inflight": max((s["max_per_key_inflight"] for s in samples), default=0),
                "max_pending_records": max((s["pending_records"] for s in samples), default=0),
                "max_outbox_pending": max((s["outbox_pending"] for s in samples), default=0),
                "max_outbox_pending_results": max((s["outbox_pending_results"] for s in samples), default=0),
                "max_outbox_pending_alerts": max((s["outbox_pending_alerts"] for s in samples), default=0),
                "max_oldest_outbox_s": round(max((s["oldest_outbox_s"] for s in samples), default=0.0), 3),
                "max_alert_reorder_depth": max((s["alert_reorder_depth"] for s in samples), default=0),
                "max_pg_conns": max((s["pg_conns"] for s in samples), default=0),
                "max_pg_lock_waits": max((s["pg_lock_waits"] for s in samples), default=0),
                "mean_cpu_pct": round(statistics.fmean(s["cpu_pct"] for s in samples), 2) if samples else 0.0,
                "peak_rss_mb": max((s["rss_mb"] for s in samples), default=0.0),
                "frontier_stall_s": round(
                    proc_registry.get_sample_value("maintai_processing_frontier_stall_seconds") or 0.0, 3
                ),
            },
            "correctness": {
                "missing_readings": max(0, total_sent - total_proc_results),
                "duplicate_terminal_outcomes": dup_outcomes,
                "duplicate_alerts": dup_alerts,
                "stale_prediction_updates": trace.stale_pred_updates,
                "wrong_latest_sequence": wrong_latest_seq,
                "alert_sequence_violations": alert_seq_violations,
                "offset_commit_violations": trace.offset_commit_violations,
            },
        }
    finally:
        stop.set()
        for t in (t_worker, t_relay, t_biz, t_sampler):
            if t is not None and t.is_alive():
                t.join(timeout=10)
        relay_pub.close()
        producer.flush(5)
        proc_pool.close()
        biz_pool.close()
        with pg_connect(args.database_url) as conn:
            conn.autocommit = True
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(proc_db)))
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(biz_db)))


def main():
    parser = argparse.ArgumentParser(description="Controlled PG17 + Kafka 4.0 Hybrid Ordering Benchmark")
    parser.add_argument("--mode", choices=["baseline", "hybrid"], default="baseline")
    parser.add_argument("--database-url", default="postgresql://maintai_test:maintai_test@127.0.0.1:35432/maintai_processing_test")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:39092")
    parser.add_argument("--machines", type=int, default=50)
    parser.add_argument("--max-active-keys", type=int, default=8)
    parser.add_argument("--max-inflight-per-key", type=int, default=1)
    parser.add_argument("--max-total-inflight", type=int, default=None)
    parser.add_argument("--max-pending-records", type=int, default=512)
    parser.add_argument("--max-pending-per-key", type=int, default=64)
    parser.add_argument("--max-uncommitted-per-partition", type=int, default=256)
    parser.add_argument("--ml-max-concurrency", type=int, default=16)
    parser.add_argument("--ml-latency-ms", type=float, default=25.0)
    parser.add_argument("--ml-jitter-ms", type=float, default=5.0)
    parser.add_argument("--ml-slow-prob", type=float, default=0.08)
    parser.add_argument("--ml-slow-ms", type=float, default=220.0)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    parser.add_argument("--measure-seconds", type=int, default=15)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--workloads", default="A,B,C")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    all_results = []
    for w in [x.strip().upper() for x in args.workloads.split(",") if x.strip()]:
        for r in range(1, args.runs + 1):
            print(f"[BENCH] Running Workload {w} run {r}/{args.runs} mode={args.mode} per_key={args.max_inflight_per_key}...", flush=True)
            result = run_single(args, w, r)
            (out_dir / f"workload_{w}_run{r}.json").write_text(json.dumps(result, indent=2) + "\n")
            all_results.append(result)
            print(
                f"  -> sent={result['throughput']['window_sent']} out/s={result['throughput']['window_outcomes_rps']} "
                f"hot_p95={result['latency_ms']['hot_machine_pred_e2e']['p95']}ms "
                f"bg_p95={result['latency_ms']['background_machine_pred_e2e']['p95']}ms "
                f"violations={sum(result['correctness'].values())}",
                flush=True,
            )

    summary_rows = []
    for r in all_results:
        summary_rows.append(
            {
                "workload": r["workload"],
                "run": r["run_index"],
                "mode": r["mode"],
                "per_key": r["config"]["max_inflight_per_key"],
                "target_rps": r["throughput"]["target_rps"],
                "admitted_rps": r["throughput"]["window_admitted_rps"],
                "outcomes_rps": r["throughput"]["window_outcomes_rps"],
                "preds_rps": r["throughput"]["window_predictions_rps"],
                "errors": r["throughput"]["errors"],
                "inf_p50_ms": r["latency_ms"]["inference"]["p50"],
                "inf_p95_ms": r["latency_ms"]["inference"]["p95"],
                "hot_e2e_p50_ms": r["latency_ms"]["hot_machine_pred_e2e"]["p50"],
                "hot_e2e_p95_ms": r["latency_ms"]["hot_machine_pred_e2e"]["p95"],
                "hot_e2e_p99_ms": r["latency_ms"]["hot_machine_pred_e2e"]["p99"],
                "bg_e2e_p50_ms": r["latency_ms"]["background_machine_pred_e2e"]["p50"],
                "bg_e2e_p95_ms": r["latency_ms"]["background_machine_pred_e2e"]["p95"],
                "db_tx_p95_ms": r["latency_ms"]["processing_db_tx"]["p95"],
                "outbox_p95_ms": r["latency_ms"]["db_commit_to_kafka_publish"]["p95"],
                "alert_p95_ms": r["latency_ms"]["alert_signal_to_alert_creation"]["p95"],
                "ml_peak": r["resources_and_queues"]["ml_peak_concurrency"],
                "max_pending": r["resources_and_queues"]["max_pending_records"],
                "max_outbox": r["resources_and_queues"]["max_outbox_pending"],
                "pg_conns": r["resources_and_queues"]["max_pg_conns"],
                "pg_locks": r["resources_and_queues"]["max_pg_lock_waits"],
                "cpu_pct": r["resources_and_queues"]["mean_cpu_pct"],
                "rss_mb": r["resources_and_queues"]["peak_rss_mb"],
                "correctness_violations": sum(r["correctness"].values()),
            }
        )
    if summary_rows:
        with (out_dir / "summary.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)
    (out_dir / "benchmark_config.json").write_text(json.dumps(vars(args), indent=2) + "\n")


if __name__ == "__main__":
    main()
