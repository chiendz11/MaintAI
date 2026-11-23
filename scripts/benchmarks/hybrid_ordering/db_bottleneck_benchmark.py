#!/usr/bin/env python3
"""Phase 2: Instrumented PostgreSQL 17 TxA/TxC/Relay/Business bottleneck analyzer.

Compares Baseline (`per_key=1, total=8, hybrid=false`) vs Hybrid (`per_key=2, total=8, hybrid=true`)
on Official 50 RPS Workloads A & C (`5s` warmup + `15s` measure x `3` runs), plus an opt-in
non-default `synchronous_commit=off` comparison on Hybrid Workload A and `EXPLAIN (ANALYZE, BUFFERS)`
for hot TxA/TxC SQL statements.
"""

from __future__ import annotations

import argparse
import contextvars
import inspect
import json
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from prometheus_client import CollectorRegistry
from psycopg import Rollback, sql
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[3]
for rel in ("", "processing/worker/src", "processing/outbox-relay/src", "backend/business-consumer/src"):
    path_str = str(ROOT / rel) if rel else str(ROOT)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from backend.shared.alerts.handler import AlertHandler  # noqa: E402
from backend.shared.alerts.repository import AlertRepository  # noqa: E402
from backend.shared.inbox.handler import ProjectionHandler  # noqa: E402
from backend.shared.persistence.migration import upgrade as upgrade_business  # noqa: E402
from backend.shared.persistence.pool import ConnectionPool as BusinessPool  # noqa: E402
from backend.shared.persistence.registry import RegistryRepository  # noqa: E402
from backend.shared.projections.projector import Projector  # noqa: E402
from consumer.handler import KeyedProcessingHandler, ProcessingHandler  # noqa: E402
from maintai_messaging.config import KafkaSettings, KeyedSettings  # noqa: E402
from maintai_messaging.contracts import default_contracts  # noqa: E402
from maintai_messaging.keyed import KeyedConsumerRunner  # noqa: E402
from maintai_messaging.metrics import ProcessingMetrics  # noqa: E402
from maintai_messaging.ordering import OrderedOutboxRelay, activate_processing_ordering  # noqa: E402
from maintai_messaging.outcomes import ModelIdentity  # noqa: E402
from maintai_messaging.postgres import connect as pg_connect  # noqa: E402
from maintai_messaging.producer import KafkaPublisher  # noqa: E402
from maintai_messaging.service import serve_consumers  # noqa: E402
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

CURRENT_TX_PHASE: contextvars.ContextVar[str] = contextvars.ContextVar("tx_phase", default="other")


class PhaseDBMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.phases: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        self.substeps: dict[str, list[float]] = defaultdict(list)

    def record_conn(
        self,
        phase: str,
        *,
        pool_wait_ms: float,
        total_ms: float,
        exec_ms: float,
        commit_ms: float,
        stmt_count: int,
    ):
        with self.lock:
            b = self.phases[phase]
            b["pool_wait_ms"].append(pool_wait_ms)
            b["total_ms"].append(total_ms)
            b["exec_ms"].append(exec_ms)
            b["commit_ms"].append(commit_ms)
            b["stmt_count"].append(float(stmt_count))

    def record_substep(self, name: str, elapsed_ms: float):
        with self.lock:
            self.substeps[name].append(elapsed_ms)

    def snapshot(self) -> dict:
        with self.lock:
            out = {"phases": {}, "substeps": {}}
            for phase, metrics in sorted(self.phases.items()):
                out["phases"][phase] = {
                    k: summarize(v) for k, v in metrics.items()
                }
                out["phases"][phase]["sum_total_ms"] = round(sum(metrics["total_ms"]), 3)
                out["phases"][phase]["sum_commit_ms"] = round(sum(metrics["commit_ms"]), 3)
                out["phases"][phase]["sum_exec_ms"] = round(sum(metrics["exec_ms"]), 3)
            for name, vals in sorted(self.substeps.items()):
                out["substeps"][name] = summarize(vals)
            return out


class TrackedTx:
    def __init__(self, inner, proxy: "TrackedConnection"):
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


class TrackedConnection:
    def __init__(self, conn, pool_wait_ms: float, tracker: PhaseDBMetrics, active_ref: list[bool]):
        self._conn = conn
        self._tracker = tracker
        self._active_ref = active_ref
        self.pool_wait_ms = pool_wait_ms
        self.stmt_count = 0
        self.exec_ms = 0.0
        self.commit_ms = 0.0
        self.opened_at = time.perf_counter()

    def execute(self, *args, **kwargs):
        t0 = time.perf_counter()
        try:
            return self._conn.execute(*args, **kwargs)
        finally:
            self.exec_ms += (time.perf_counter() - t0) * 1000.0
            self.stmt_count += 1

    def transaction(self, *args, **kwargs):
        return TrackedTx(self._conn.transaction(*args, **kwargs), self)

    def commit(self):
        t0 = time.perf_counter()
        try:
            return self._conn.commit()
        finally:
            self.commit_ms += (time.perf_counter() - t0) * 1000.0

    def __getattr__(self, name):
        return getattr(self._conn, name)


@contextmanager
def tracked_factory(pool, tracker: PhaseDBMetrics, active_ref: list[bool], default_phase: str):
    phase = CURRENT_TX_PHASE.get()
    if phase == "other":
        phase = default_phase
    t0 = time.perf_counter()
    with pool() as conn:
        t1 = time.perf_counter()
        proxy = TrackedConnection(conn, (t1 - t0) * 1000.0, tracker, active_ref)
        try:
            yield proxy
        finally:
            total_ms = (time.perf_counter() - t1) * 1000.0
            if active_ref[0]:
                tracker.record_conn(
                    phase,
                    pool_wait_ms=proxy.pool_wait_ms,
                    total_ms=total_ms,
                    exec_ms=proxy.exec_ms,
                    commit_ms=proxy.commit_ms,
                    stmt_count=proxy.stmt_count,
                )


def pg_snapshot(dsn: str, dbs: list[str]) -> dict:
    with pg_connect(dsn) as c:
        wal = c.execute(
            "SELECT wal_records, wal_fpi, wal_bytes::bigint AS wal_bytes, wal_buffers_full, "
            "wal_write, wal_sync FROM pg_stat_wal"
        ).fetchone()
        db_rows = c.execute(
            "SELECT datname, xact_commit, xact_rollback, blks_read, blks_hit, "
            "tup_returned, tup_fetched, tup_inserted, tup_updated "
            "FROM pg_stat_database WHERE datname = ANY(%s)",
            (dbs,),
        ).fetchall()
    return {
        "wal": dict(wal),
        "dbs": {r["datname"]: dict(r) for r in db_rows},
    }


def read_pg_cgroup(container: str = "maintai-m2-postgres-1") -> dict:
    try:
        out = subprocess.check_output(
            [
                "docker",
                "exec",
                container,
                "sh",
                "-c",
                "cat /sys/fs/cgroup/cpu.stat 2>/dev/null || true; "
                "echo IOSTAT_BEGIN; cat /sys/fs/cgroup/io.stat 2>/dev/null || true",
            ],
            text=True,
            timeout=5,
        )
    except Exception:
        return {}
    cpu = {}
    io_rbytes = 0
    io_wbytes = 0
    io_rios = 0
    io_wios = 0
    in_io = False
    for line in out.splitlines():
        if line == "IOSTAT_BEGIN":
            in_io = True
            continue
        if not in_io:
            parts = line.split()
            if len(parts) == 2:
                try:
                    cpu[parts[0]] = int(parts[1])
                except ValueError:
                    pass
        else:
            for tok in line.split()[1:]:
                if "=" in tok:
                    k, v = tok.split("=", 1)
                    try:
                        iv = int(v)
                    except ValueError:
                        continue
                    if k == "rbytes":
                        io_rbytes += iv
                    elif k == "wbytes":
                        io_wbytes += iv
                    elif k == "rios":
                        io_rios += iv
                    elif k == "wios":
                        io_wios += iv
    return {
        **cpu,
        "io_rbytes": io_rbytes,
        "io_wbytes": io_wbytes,
        "io_rios": io_rios,
        "io_wios": io_wios,
    }


def collect_explain_plans(proc_dsn: str, consumer_group: str) -> dict[str, list[str]]:
    plans = {}
    with pg_connect(proc_dsn) as c:
        c.execute("ANALYZE")
        m_sample = c.execute(
            "SELECT machine_id, MAX(sequence_number) AS max_seq, MAX(event_id) AS any_eid "
            "FROM sensor_events GROUP BY machine_id ORDER BY count(*) DESC LIMIT 1"
        ).fetchone()
        if m_sample is None:
            return {"error": ["No sensor_events rows"]}
        mid = m_sample["machine_id"]
        seq = m_sample["max_seq"]
        eid = m_sample["any_eid"]
        queries = {
            "txa_admission_cursor_lock": (
                "SELECT * FROM processing_admission_cursor "
                "WHERE consumer_group=%s AND machine_id=%s FOR UPDATE",
                (consumer_group, mid),
            ),
            "txa_inbox_lookup": (
                "SELECT identity_sha256 FROM maintai_kafka_inbox "
                "WHERE consumer_group=%s AND topic='sensor.readings' AND message_id=%s",
                (consumer_group, eid),
            ),
            "txa_ledger_lookup_by_msgid": (
                "SELECT admission_seq,sequence_number,identity_sha256,disposition "
                "FROM processing_alert_ledger WHERE consumer_group=%s AND topic='sensor.readings' AND message_id=%s",
                (consumer_group, eid),
            ),
            "txa_sensor_events_seq_check": (
                "SELECT event_id FROM sensor_events WHERE machine_id=%s AND sequence_number=%s "
                "AND event_id <> %s LIMIT 1",
                (mid, seq, "nonexistent-evt"),
            ),
            "txc_drain_frontier_scan": (
                "SELECT * FROM processing_alert_ledger "
                "WHERE consumer_group=%s AND machine_id=%s AND admission_seq > %s "
                "ORDER BY admission_seq ASC FOR UPDATE",
                (consumer_group, mid, 0),
            ),
            "relay_claim_lateral": (
                "SELECT k.topic,k.message_key FROM maintai_outbox_keys k "
                "CROSS JOIN LATERAL ("
                "SELECT o.created_at,o.status,o.next_attempt_at,o.lease_until "
                "FROM maintai_kafka_outbox o "
                "WHERE o.topic=k.topic AND o.message_key=k.message_key "
                "AND o.dispatch_seq=k.last_published+1 "
                "AND o.dispatch_seq IS NOT NULL LIMIT 1"
                ") o "
                "WHERE k.last_published<k.last_allocated "
                "AND (k.owner IS NULL OR k.lease_until<clock_timestamp()) "
                "AND ((o.status='PENDING' AND o.next_attempt_at<=clock_timestamp()) "
                "OR (o.status='LEASED' AND o.lease_until<clock_timestamp())) "
                "ORDER BY o.created_at,k.topic,k.message_key LIMIT 1 FOR UPDATE OF k SKIP LOCKED",
                (),
            ),
        }
        for name, (sql_txt, params) in queries.items():
            try:
                with c.transaction():
                    rows = c.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql_txt}", params).fetchall()
                    plans[name] = [r["QUERY PLAN"] for r in rows]
                    raise Rollback()
            except Rollback:
                pass
        idx_rows = c.execute(
            "SELECT tablename, indexname, indexdef FROM pg_indexes "
            "WHERE schemaname='public' AND tablename IN ('sensor_events','processing_admission_cursor','processing_alert_ledger','maintai_kafka_outbox') "
            "ORDER BY tablename, indexname"
        ).fetchall()
        plans["_indexes"] = [f"{r['tablename']}: {r['indexdef']}" for r in idx_rows]
    return plans


def run_instrumented_trial(
    *,
    mode: str,
    workload: str,
    run_idx: int,
    database_url: str,
    bootstrap_servers: str,
    synchronous_commit: str = "on",
    warmup_seconds: int = 5,
    measure_seconds: int = 15,
    capture_explain: bool = False,
) -> dict:
    run_id = uuid4().hex[:12]
    proc_db = f"bench2_proc_{run_id}"
    biz_db = f"bench2_biz_{run_id}"
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
        group_prefix=f"bench2-{run_id}-",
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
    pool_opts = (
        "-c statement_timeout=15000 -c lock_timeout=5000 "
        "-c idle_in_transaction_session_timeout=60000"
        + (f" -c synchronous_commit={synchronous_commit}" if synchronous_commit != "on" else "")
    )
    proc_pool = ProcessingPool(proc_dsn, size=12, options=pool_opts)
    biz_pool = BusinessPool(biz_dsn, size=12, options=pool_opts)

    tracker = PhaseDBMetrics()
    measure_active = [False]
    trace = RunTrace()

    def proc_factory(default_phase: str = "proc_other"):
        return tracked_factory(proc_pool, tracker, measure_active, default_phase)

    def biz_factory(default_phase: str = "biz_other"):
        return tracked_factory(biz_pool, tracker, measure_active, default_phase)

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
        seed=2000 + run_idx,
    )
    validator = FeatureValidator.from_file(
        ROOT / "contracts/v1.3/schemas/features.schema.json", version="sensor-validator-1.0.0"
    )
    policy = RiskPolicy.from_file(ROOT / "processing/config/risk-policies/demo-risk-v1.json")
    proc_group = settings.group_prefix + contracts.topology["profiles"]["processing"]["group_id"]

    class InstApp(ProcessingHandler):
        def prepare(self, event, record, context):
            tok = CURRENT_TX_PHASE.set("StageB_prepare")
            try:
                return super().prepare(event, record, context)
            finally:
                CURRENT_TX_PHASE.reset(tok)

        def persist_prepared(self, connection, event, record, outbox, prepared):
            with trace.lock:
                trace.persist_start_at[event.message_id] = time.monotonic()
            res = super().persist_prepared(connection, event, record, outbox, prepared)
            with trace.lock:
                trace.persist_end_at[event.message_id] = time.monotonic()
            return res

        def persist_hybrid(self, connection, event, record, outbox, prepared, **kwargs):
            with trace.lock:
                trace.persist_start_at[event.message_id] = time.monotonic()
            t0 = time.perf_counter()
            res = super().persist_hybrid(connection, event, record, outbox, prepared, **kwargs)
            t1 = time.perf_counter()
            if measure_active[0]:
                tracker.record_substep("persist_hybrid_fn_ms", (t1 - t0) * 1000.0)
            with trace.lock:
                trace.persist_end_at[event.message_id] = time.monotonic()
            return res

    app_handler = InstApp(
        lambda: proc_factory("proc_other"),
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

    # Instrument resequencer substeps in Hybrid mode
    orig_admit = app_handler.resequencer.admit
    orig_disp = app_handler.resequencer.record_disposition
    orig_drain = app_handler.resequencer.drain_frontier

    def wrapped_admit(*a, **kw):
        t0 = time.perf_counter()
        res = orig_admit(*a, **kw)
        if measure_active[0]:
            tracker.record_substep("txa_resequencer_admit_ms", (time.perf_counter() - t0) * 1000.0)
        return res

    def wrapped_disp(*a, **kw):
        t0 = time.perf_counter()
        res = orig_disp(*a, **kw)
        if measure_active[0]:
            tracker.record_substep("txc_record_disposition_ms", (time.perf_counter() - t0) * 1000.0)
        return res

    def wrapped_drain(*a, **kw):
        t0 = time.perf_counter()
        res = orig_drain(*a, **kw)
        if measure_active[0]:
            tracker.record_substep("txc_drain_frontier_ms", (time.perf_counter() - t0) * 1000.0)
        return res

    app_handler.resequencer.admit = wrapped_admit
    app_handler.resequencer.record_disposition = wrapped_disp
    app_handler.resequencer.drain_frontier = wrapped_drain

    class InstKeyed(KeyedProcessingHandler):
        def admit(self, event, record, context):
            with trace.lock:
                trace.admitted_at.setdefault(event.message_id, time.monotonic())
            tok = CURRENT_TX_PHASE.set("TxA_admit")
            try:
                return super().admit(event, record, context)
            finally:
                CURRENT_TX_PHASE.reset(tok)

        def _durable_replay(self, event, record, context, *, allow_ledger_signal=False):
            tok = CURRENT_TX_PHASE.set("Baseline_durable_replay")
            try:
                return super()._durable_replay(
                    event, record, context, allow_ledger_signal=allow_ledger_signal
                )
            finally:
                CURRENT_TX_PHASE.reset(tok)

        def handle(self, event, record, context):
            with trace.lock:
                trace.admitted_at.setdefault(event.message_id, time.monotonic())
            tok = CURRENT_TX_PHASE.set(
                "TxC_persist" if self.hybrid_ordering_enabled else "Baseline_tx"
            )
            try:
                return super().handle(event, record, context)
            finally:
                CURRENT_TX_PHASE.reset(tok)

    keyed_handler = InstKeyed(
        app_handler,
        on_error=lambda: setattr(trace, "errors", trace.errors + 1),
        hybrid_ordering_enabled=(mode == "hybrid"),
    )
    runner = KeyedConsumerRunner(
        settings,
        "processing",
        keyed_handler,
        limits=limits,
        metrics=ProcessingMetrics(CollectorRegistry()),
        contracts=contracts,
    )

    class InstRelay(OrderedOutboxRelay):
        def _claim(self):
            tok = CURRENT_TX_PHASE.set("Relay_claim")
            try:
                return super()._claim()
            finally:
                CURRENT_TX_PHASE.reset(tok)

        def _finish(self, row, success, delay):
            tok = CURRENT_TX_PHASE.set("Relay_finish")
            try:
                return super()._finish(row, success, delay)
            finally:
                CURRENT_TX_PHASE.reset(tok)

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

    class InstProj(ProjectionHandler):
        def __call__(self, event, record):
            tok = CURRENT_TX_PHASE.set(
                "Biz_live" if self.topic == "sensor.readings" else "Biz_pred"
            )
            try:
                return super().__call__(event, record)
            finally:
                CURRENT_TX_PHASE.reset(tok)

    class InstAlert(AlertHandler):
        def __call__(self, event, record):
            tok = CURRENT_TX_PHASE.set("Biz_alert")
            try:
                return super().__call__(event, record)
            finally:
                CURRENT_TX_PHASE.reset(tok)

    class TrackingProjector(Projector):
        def __call__(self, connection, event, record, outbox):
            res = super().__call__(connection, event, record, outbox)
            if event.topic == "inference.results":
                with trace.lock:
                    trace.biz_pred_commit_at.setdefault(event.source_event_id, time.monotonic())
            return res

    projector = TrackingProjector()
    evidence_client = LocalEvidenceClient(proc_pool, contracts)
    biz_group_live = settings.group_prefix + contracts.topology["profiles"]["business-live"]["group_id"]
    biz_group_pred = settings.group_prefix + contracts.topology["profiles"]["business-prediction"]["group_id"]
    biz_group_alert = settings.group_prefix + contracts.topology["profiles"]["business-alert"]["group_id"]
    biz_handlers = {
        "business-live": InstProj(lambda: biz_factory("Biz_live"), biz_group_live, topic="sensor.readings", projector=projector),
        "business-prediction": InstProj(lambda: biz_factory("Biz_pred"), biz_group_pred, topic="inference.results", projector=projector),
        "business-alert": InstAlert(lambda: biz_factory("Biz_alert"), biz_group_alert, evidence_client, repository=AlertRepository(lambda: biz_factory("Biz_alert"))),
    }

    stop = threading.Event()
    relay_pub = KafkaPublisher(settings, "outbox")
    from confluent_kafka import Producer as RawKafkaProducer

    producer = RawKafkaProducer({**settings.producer_config(), "linger.ms": 2})
    wait_events = Counter()
    lock_modes = Counter()
    max_active_pg = 0
    max_wait_pg = 0
    explain_plans = {}
    t_worker = t_relay = t_biz = t_pgmon = None

    try:
        relay = InstRelay(
            lambda: proc_factory("Relay_other"),
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

        t_worker = threading.Thread(target=lambda: runner.run(stop), daemon=True)
        t_relay = threading.Thread(target=relay_loop, daemon=True)
        t_biz = threading.Thread(
            target=lambda: serve_consumers(settings, biz_handlers, stop, contracts=contracts),
            daemon=True,
        )
        t_worker.start()
        t_relay.start()
        t_biz.start()

        dl = time.monotonic() + 10
        while not runner.tokens and time.monotonic() < dl:
            time.sleep(0.05)

        epoch = f"epoch-{run_id}"
        seq_counter = {m: 0 for m in rates}
        example_reading = json.loads((ROOT / "contracts/kafka/examples/sensor-reading.json").read_text())
        base_features = example_reading["features"]

        t_start = time.monotonic()
        t_warmup_end = t_start + warmup_seconds
        t_measure_end = t_warmup_end + measure_seconds

        def pg_monitor():
            nonlocal max_active_pg, max_wait_pg
            with pg_connect(database_url) as mconn:
                while not stop.is_set():
                    time.sleep(0.1)
                    if not measure_active[0]:
                        continue
                    rows = mconn.execute(
                        "SELECT state, wait_event_type, wait_event "
                        "FROM pg_stat_activity WHERE datname = ANY(%s) AND pid <> pg_backend_pid()",
                        ([proc_db, biz_db],),
                    ).fetchall()
                    act = sum(1 for r in rows if r["state"] == "active")
                    lks = sum(1 for r in rows if r["wait_event_type"] == "Lock")
                    max_active_pg = max(max_active_pg, act)
                    max_wait_pg = max(max_wait_pg, lks)
                    for r in rows:
                        if r["state"] == "active" and r["wait_event_type"]:
                            wait_events[f"{r['wait_event_type']}:{r['wait_event']}"] += 1
                    lk_rows = mconn.execute(
                        "SELECT l.mode, l.granted FROM pg_locks l JOIN pg_database d ON l.database=d.oid "
                        "WHERE d.datname = ANY(%s) AND NOT l.granted",
                        ([proc_db, biz_db],),
                    ).fetchall()
                    for r in lk_rows:
                        lock_modes[f"WAITING:{r['mode']}"] += 1

        t_pgmon = threading.Thread(target=pg_monitor, daemon=True)
        t_pgmon.start()

        next_due = {m: t_start + (idx * 0.002) for idx, m in enumerate(sorted(rates))}
        window_event_ids: list[str] = []
        all_event_ids: list[str] = []
        snap_before = None
        cg_before = None
        snap_after = None
        cg_after = None

        while True:
            now = time.monotonic()
            if snap_before is None and now >= t_warmup_end:
                measure_active[0] = True
                snap_before = pg_snapshot(database_url, [proc_db, biz_db])
                cg_before = read_pg_cgroup()
            if now >= t_measure_end:
                measure_active[0] = False
                snap_after = pg_snapshot(database_url, [proc_db, biz_db])
                cg_after = read_pg_cgroup()
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
            if pr >= total_sent and ob == 0 and bd >= total_sent:
                break
            time.sleep(0.1)

        if capture_explain:
            explain_plans = collect_explain_plans(proc_dsn, proc_group)

        stop.set()
        t_worker.join(timeout=10)
        t_relay.join(timeout=5)
        t_biz.join(timeout=10)
        t_pgmon.join(timeout=2)

        win_admitted = sum(1 for ts in trace.admitted_at.values() if t_warmup_end <= ts <= t_measure_end)
        win_outcomes = sum(1 for ts in trace.persist_end_at.values() if t_warmup_end <= ts <= t_measure_end)
        win_preds = sum(1 for ts in trace.biz_pred_commit_at.values() if t_warmup_end <= ts <= t_measure_end)

        wal_b, wal_a = snap_before["wal"], snap_after["wal"]
        wal_delta = {
            "wal_records": wal_a["wal_records"] - wal_b["wal_records"],
            "wal_bytes": wal_a["wal_bytes"] - wal_b["wal_bytes"],
            "wal_write": wal_a["wal_write"] - wal_b["wal_write"],
            "wal_sync": wal_a["wal_sync"] - wal_b["wal_sync"],
        }
        db_deltas = {}
        for k, dbname in (("proc_db", proc_db), ("biz_db", biz_db)):
            b, a = snap_before["dbs"][dbname], snap_after["dbs"][dbname]
            db_deltas[k] = {
                m: a[m] - b[m]
                for m in ("xact_commit", "xact_rollback", "blks_read", "blks_hit", "tup_returned", "tup_fetched", "tup_inserted", "tup_updated")
            }
        total_commits = db_deltas["proc_db"]["xact_commit"] + db_deltas["biz_db"]["xact_commit"]

        pg_cpu_pct = None
        pg_wbytes = None
        pg_wios = None
        if cg_before and cg_after and "usage_usec" in cg_after and "usage_usec" in cg_before:
            pg_cpu_pct = round(
                ((cg_after["usage_usec"] - cg_before["usage_usec"]) / (measure_seconds * 1_000_000.0)) * 100.0,
                2,
            )
            pg_wbytes = cg_after.get("io_wbytes", 0) - cg_before.get("io_wbytes", 0)
            pg_wios = cg_after.get("io_wios", 0) - cg_before.get("io_wios", 0)

        db_snap = tracker.snapshot()
        txa_sum_ms = db_snap["phases"].get("TxA_admit", {}).get("sum_total_ms", 0.0)
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

        return {
            "mode": mode,
            "workload": workload,
            "run_index": run_idx,
            "synchronous_commit": synchronous_commit,
            "throughput": {
                "target_rps": round(sum(rates.values()), 2),
                "window_sent": len(window_event_ids),
                "admitted_rps": round(win_admitted / measure_seconds, 2),
                "outcomes_rps": round(win_outcomes / measure_seconds, 2),
                "preds_rps": round(win_preds / measure_seconds, 2),
                "errors": trace.errors,
            },
            "e2e_ms": {
                "hot_pred": summarize(hot_e2e),
                "bg_pred": summarize(bg_e2e),
            },
            "db_metrics": db_snap,
            "txa_poll_stall_pct": round((txa_sum_ms / (measure_seconds * 1000.0)) * 100.0, 2),
            "pg_internals": {
                "wal_delta": wal_delta,
                "wal_sync_per_sec": round(wal_delta["wal_sync"] / measure_seconds, 2),
                "wal_bytes_per_sec": round(wal_delta["wal_bytes"] / measure_seconds, 1),
                "wal_sync_per_outcome": round(wal_delta["wal_sync"] / max(1, win_outcomes), 3),
                "commits_per_outcome": round(total_commits / max(1, win_outcomes), 3),
                "db_deltas": db_deltas,
                "pg_cpu_pct": pg_cpu_pct,
                "pg_io_wbytes": pg_wbytes,
                "pg_io_wios": pg_wios,
                "max_active_pg": max_active_pg,
                "max_wait_pg": max_wait_pg,
                "wait_events": dict(wait_events.most_common(12)),
                "lock_waits": dict(lock_modes.most_common(10)),
            },
            "explain_plans": explain_plans,
        }
    finally:
        stop.set()
        for t in (t_worker, t_relay, t_biz, t_pgmon):
            if t is not None and t.is_alive():
                t.join(timeout=10)
        relay_pub.close()
        producer.flush(5)
        proc_pool.close()
        biz_pool.close()
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
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    matrix = [
        ("baseline", "A", "on", args.runs, False),
        ("hybrid", "A", "on", args.runs, False),
        ("baseline", "C", "on", args.runs, False),
        ("hybrid", "C", "on", args.runs, True),
        # Separate opt-in durability trade-off experiment (non-default):
        ("hybrid", "A", "off", 1, False),
    ]
    results = []
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    for mode, wl, sc, nruns, do_explain in matrix:
        for r in range(1, nruns + 1):
            cap_exp = do_explain and r == nruns
            print(f"[PHASE2] mode={mode} workload={wl} sync_commit={sc} run={r}/{nruns}", flush=True)
            res = run_instrumented_trial(
                mode=mode,
                workload=wl,
                run_idx=r,
                database_url=args.database_url,
                bootstrap_servers=args.bootstrap_servers,
                synchronous_commit=sc,
                capture_explain=cap_exp,
            )
            results.append(res)
            out_path.write_text(json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), "runs": results}, indent=2) + "\n")
            tp = res["throughput"]
            pg = res["pg_internals"]
            print(
                f"  -> adm={tp['admitted_rps']} out={tp['outcomes_rps']} pred={tp['preds_rps']} "
                f"hot_p95={res['e2e_ms']['hot_pred']['p95']}ms bg_p95={res['e2e_ms']['bg_pred']['p95']}ms "
                f"txa_stall={res['txa_poll_stall_pct']}% wal_sync/s={pg['wal_sync_per_sec']} "
                f"commits/out={pg['commits_per_outcome']} pg_cpu={pg['pg_cpu_pct']}%",
                flush=True,
            )

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
