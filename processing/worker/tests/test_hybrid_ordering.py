"""Integration tests for Hybrid Ordering (Keyed Parallel Scheduling) on real PG17 & Kafka 4."""

import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from urllib.parse import urlparse

import pytest
from confluent_kafka import Consumer as RawKafkaConsumer
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from maintai_messaging.admin import create_local_topics
from maintai_messaging.config import KafkaSettings, KeyedSettings
from maintai_messaging.consumer import Record
from maintai_messaging.contracts import canonical_json, default_contracts
from maintai_messaging.errors import LeaseLost, PublishUnavailable
from maintai_messaging.keyed import KeyedConsumerRunner
from maintai_messaging.metrics import ProcessingMetrics
from maintai_messaging.ordering import WorkContext, activate_processing_ordering, require_ordering
from maintai_messaging.producer import KafkaPublisher
from processing.worker.src.consumer.handler import (
    KeyedProcessingHandler,
    ProcessingHandler,
    RegistryUnavailable,
)
from prometheus_client import CollectorRegistry
from relay.main import create_relay

from .test_worker_transaction import Providers, handler, source

pytestmark = pytest.mark.integration


@pytest.fixture
def ordered_database(migrated_database):
    with migrated_database.factory() as connection:
        activate_processing_ordering(connection, legacy_writers_drained=True)
    return migrated_database


def kafka_settings():
    if os.environ.get("MAINTAI_KAFKA_INTEGRATION") != "1":
        pytest.skip("Kafka integration tests require MAINTAI_KAFKA_INTEGRATION=1")
    bootstrap = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS") or os.environ.get(
        "MAINTAI_M2_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:39092"
    )
    if urlparse("kafka://" + bootstrap).hostname not in {"127.0.0.1", "localhost"}:
        pytest.fail("Tests require an isolated loopback Kafka broker")
    return KafkaSettings(
        local_mode=True,
        security_protocol="PLAINTEXT",
        bootstrap_servers=bootstrap,
        group_prefix="hyb-" + uuid.uuid4().hex + "-",
        poll_interval_seconds=0.01,
        producer_git_sha="a" * 40,
    )


def reading(machine="M-001", seq=1, offset=1, event_id=None, stream_epoch="epoch-1", features=None):
    original, _ = source(features=features)
    payload = {
        **original.payload,
        "machine_id": machine,
        "sequence_number": seq,
        "stream_epoch": stream_epoch,
        "event_id": event_id or f"evt-{machine}-{seq}",
    }
    event = default_contracts().decode("sensor.readings", canonical_json(payload))
    return event, Record(event.topic, 0, offset, event.key.encode(), event.wire_bytes)


def hybrid_handler(database, providers, *, group="hyb-group", unavailable_disposition="BLOCKED"):
    app = handler(database, providers, group=group)
    app.hybrid_ordering_enabled = True
    app.unavailable_alert_disposition = unavailable_disposition
    return app


def test_same_machine_parallel_inference_no_db_locks_and_ooo_prediction(
    ordered_database, business_registry_service
):
    from backend.shared.ordering.identity import guard_source
    from backend.shared.persistence.migration import upgrade as upgrade_biz
    from backend.shared.persistence.registry import RegistryRepository
    from backend.shared.projections.projector import Projector

    providers = Providers(probability=0.9)
    app = hybrid_handler(ordered_database, providers)
    thread_state = threading.local()
    base_factory = app.connection_factory

    @contextmanager
    def tracked_factory():
        with base_factory() as conn:
            thread_state.active = getattr(thread_state, "active", 0) + 1
            try:
                yield conn
            finally:
                thread_state.active -= 1

    app.connection_factory = tracked_factory
    barrier = threading.Barrier(2)
    seq2_persisted = threading.Event()
    seq1_done = threading.Event()
    saw_zero_locks = []
    base_predict = app.predict

    def predict(**kwargs):
        assert getattr(thread_state, "active", 0) == 0, "ML HTTP thread must hold zero owner DB connections"
        with ordered_database.factory() as inspect_conn:
            held_locks = inspect_conn.execute(
                """SELECT count(*) AS n FROM pg_locks l
                JOIN pg_class c ON c.oid = l.relation
                WHERE c.relname IN (
                    'maintai_processing_machine_cursor',
                    'processing_admission_cursor',
                    'processing_source_cursor'
                ) AND l.mode LIKE '%Exclusive%'"""
            ).fetchone()["n"]
            saw_zero_locks.append(held_locks)
        barrier.wait(timeout=5)
        if kwargs["event_id"] == "evt-M-001-1":
            # seq=1 waits until seq=2 commits Stage C and publishes prediction out-of-order
            assert seq2_persisted.wait(5)
            time.sleep(0.02)
            resp = Providers(probability=0.1).predict(**kwargs)  # seq=1 -> NORMAL
            return resp
        return base_predict(**kwargs)

    app.predict = predict
    consumer = KeyedProcessingHandler(app)
    assignment = consumer.acquire([TopicPartition("sensor.readings", 0)])[("sensor.readings", 0)]
    ctx = WorkContext(assignment, time.monotonic() + 15)

    ev1, rec1 = reading("M-001", 1, 1)
    ev2, rec2 = reading("M-001", 2, 2)
    t1 = consumer.admit(ev1, rec1, ctx)
    t2 = consumer.admit(ev2, rec2, ctx)
    ctx1 = WorkContext(assignment, time.monotonic() + 15, admission=t1)
    ctx2 = WorkContext(assignment, time.monotonic() + 15, admission=t2)

    def run_two():
        res = consumer.handle(ev2, rec2, ctx2)
        seq2_persisted.set()
        return res

    def run_one():
        res = consumer.handle(ev1, rec1, ctx1)
        seq1_done.set()
        return res

    with ThreadPoolExecutor(2) as pool:
        f1 = pool.submit(run_one)
        f2 = pool.submit(run_two)
        f2.result(timeout=10)
        # Before seq=1 finishes: seq=2 prediction & inference.results exist out-of-order,
        # while alert.signals stays held by resequencer!
        with ordered_database.factory() as conn:
            kinds = [r["event_id"] for r in conn.execute("SELECT event_id FROM processing_results").fetchall()]
            assert kinds == ["evt-M-001-2"]
            outbox_topics = {
                r["topic"] for r in conn.execute("SELECT topic FROM maintai_kafka_outbox").fetchall()
            }
            assert outbox_topics == {"inference.results"}
            ledger = conn.execute(
                "SELECT admission_seq,disposition FROM processing_alert_ledger ORDER BY admission_seq"
            ).fetchall()
            assert [(int(r["admission_seq"]), r["disposition"]) for r in ledger] == [
                (1, "PENDING"),
                (2, "SIGNAL_READY"),
            ]
        f1.result(timeout=10)

    assert seq1_done.is_set()
    assert saw_zero_locks and all(x == 0 for x in saw_zero_locks)
    with ordered_database.factory() as conn:
        assert conn.execute("SELECT count(*) AS n FROM processing_results").fetchone()["n"] == 2
        alerts_outbox = conn.execute(
            "SELECT message_id,source_sequence_number,dispatch_seq FROM maintai_kafka_outbox WHERE topic='alert.signals'"
        ).fetchall()
        assert len(alerts_outbox) == 1
        assert int(alerts_outbox[0]["source_sequence_number"]) == 2
        assert (
            conn.execute("SELECT last_sequence FROM maintai_processing_machine_cursor").fetchone()[
                "last_sequence"
            ]
            == 2
        )

        # Feed out-of-order outcomes (seq=2 first, then seq=1) into Business Projector
        rows = conn.execute(
            "SELECT r.outcome, e.sequence_number FROM processing_results r "
            "JOIN sensor_events e USING(event_id) ORDER BY e.sequence_number DESC"
        ).fetchall()

    # Verify Business Projection Sequence Guard refuses stale overwrite from seq=1
    from psycopg import sql as psql
    from sqlalchemy.engine import make_url
    from maintai_messaging.postgres import connect as pg_connect

    biz_name = "hyb_proj_" + uuid.uuid4().hex[:12]
    with pg_connect(ordered_database.dsn) as conn:
        conn.autocommit = True
        conn.execute(psql.SQL("CREATE DATABASE {}").format(psql.Identifier(biz_name)))
    biz_dsn = make_url(ordered_database.dsn).set(database=biz_name).render_as_string(hide_password=False)
    try:
        upgrade_biz(biz_dsn)
        RegistryRepository(lambda: pg_connect(biz_dsn)).provision(
            [{"machine_id": "M-001", "display_name": "One"}]
        )
        projector = Projector()
        contracts = default_contracts()
        assert len(rows) == 2
        for row in rows:  # seq=2 first, then seq=1 (both PREDICTION_COMPLETED)!
            outcome_ev = contracts.decode("inference.results", bytes(row["outcome"]))
            assert outcome_ev.kind == "PREDICTION_COMPLETED"
            with pg_connect(biz_dsn) as conn, conn.transaction():
                projector(
                    conn,
                    outcome_ev,
                    Record("inference.results", 0, 1, outcome_ev.key.encode(), outcome_ev.wire_bytes),
                    None,
                )
        with pg_connect(biz_dsn) as conn:
            assert guard_source is not None
            lvp = conn.execute(
                "SELECT sequence_number FROM latest_valid_prediction WHERE machine_id='M-001'"
            ).fetchone()
            assert int(lvp["sequence_number"]) == 2
    finally:
        with pg_connect(ordered_database.dsn) as conn:
            conn.autocommit = True
            conn.execute(psql.SQL("DROP DATABASE {} WITH (FORCE)").format(psql.Identifier(biz_name)))


def test_real_kafka_ordered_alerts_gaps_nosignal_safe_offset_and_no_dup_outbox(
    ordered_database,
):
    settings = kafka_settings()
    contracts = default_contracts()
    create_local_topics(settings, contracts=contracts)

    # Capture end offsets and initialize test consumer group at tail so prior test traffic is ignored
    from confluent_kafka import ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient, OffsetSpec

    admin = AdminClient(settings.client_config())
    start_alert_offsets = {
        tp.partition: fut.result(timeout=10).offset
        for tp, fut in admin.list_offsets(
            {TopicPartition("alert.signals", p): OffsetSpec.latest() for p in range(12)}
        ).items()
    }
    proc_group = settings.group_prefix + contracts.topology["profiles"]["processing"]["group_id"]
    start_sensor_tps = [
        TopicPartition("sensor.readings", tp.partition, fut.result(timeout=10).offset)
        for tp, fut in admin.list_offsets(
            {TopicPartition("sensor.readings", p): OffsetSpec.latest() for p in range(12)}
        ).items()
    ]
    for fut in admin.alter_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(proc_group, start_sensor_tps)]
    ).values():
        reply = fut.result(timeout=10)
        for p in reply.topic_partitions or []:
            if p.error is not None:
                raise KafkaException(p.error)

    run_tag = uuid.uuid4().hex[:8]
    m1 = f"M-ORD-{run_tag}"
    m2 = f"M-OTH-{run_tag}"

    def make_payload(machine, seq, *, torque=65.0, bad=False):
        orig, _ = source()
        features = dict(orig.payload["features"])
        if bad:
            features["product_type"] = "INVALID_X"
        else:
            features["torque_nm"] = torque
        return {
            **orig.payload,
            "machine_id": machine,
            "sequence_number": seq,
            "event_id": f"evt-{machine}-{seq}",
            "features": features,
        }

    # Publish readings onto Kafka broker before starting runner
    base_example = Providers()
    def custom_predict(*, event_id, machine_id, features, correlation_id):
        if event_id == f"evt-{m1}-1":
            time.sleep(0.35)  # M-001 seq=1 stays slow while seq 2,3,5,6 overtake!
        prob = 0.88 if float(features.get("torque_nm", 40.0)) >= 60.0 else 0.10
        return Providers(probability=prob).predict(
            event_id=event_id, machine_id=machine_id, features=features, correlation_id=correlation_id
        )

    app = hybrid_handler(
        ordered_database,
        base_example,
        group=settings.group_prefix + contracts.topology["profiles"]["processing"]["group_id"],
    )
    app.predict = custom_predict
    keyed_handler = KeyedProcessingHandler(app)

    commit_history = []

    class InspectingRunner(KeyedConsumerRunner):
        def _commit(self):
            before = list(self.ledger.candidates())
            super()._commit()
            for part, entries in before:
                commit_history.append([(e.event.key, int(e.event.payload["sequence_number"])) for e in entries])

    registry = CollectorRegistry()
    runner = InspectingRunner(
        settings,
        "processing",
        keyed_handler,
        limits=KeyedSettings(
            hybrid_ordering_enabled=True,
            max_active_keys=8,
            max_inflight_per_key=2,
            max_total_inflight=4,
            drain_timeout_seconds=30,
        ),
        metrics=ProcessingMetrics(registry),
        contracts=contracts,
    )

    with KafkaPublisher(settings, "ingestion", contracts=contracts) as pub:
        # 1: HIGH (slow), 2: NORMAL (NO_SIGNAL), 3: INVALID (NO_SIGNAL), gap 4, 5: HIGH, 6: HIGH
        pub.publish("sensor.readings", make_payload(m1, 1, torque=65.0))
        pub.publish("sensor.readings", make_payload(m1, 2, torque=20.0))
        pub.publish("sensor.readings", make_payload(m1, 3, bad=True))
        pub.publish("sensor.readings", make_payload(m1, 5, torque=65.0))
        pub.publish("sensor.readings", make_payload(m1, 6, torque=65.0))
        pub.publish("sensor.readings", make_payload(m2, 1, torque=65.0))

    stop = threading.Event()
    t = threading.Thread(target=lambda: runner.run(stop, max_records=6), daemon=True)
    t.start()
    t.join(timeout=30)
    assert not t.is_alive(), "Runner did not complete 6 records in time"
    assert runner.processed == 6
    m1_commits = [seq for batch in commit_history for key, seq in batch if key == m1]
    assert m1_commits == [1, 2, 3, 5, 6]

    # Check Postgres ledger & outbox invariants
    with ordered_database.factory() as conn:
        ledger = conn.execute(
            "SELECT machine_id,admission_seq,sequence_number,disposition,outcome_kind "
            "FROM processing_alert_ledger ORDER BY machine_id,admission_seq"
        ).fetchall()
        m1_rows = [r for r in ledger if r["machine_id"] == m1]
        assert [(int(r["sequence_number"]), r["disposition"]) for r in m1_rows] == [
            (1, "EMITTED"),
            (2, "RESOLVED_NO_SIGNAL"),
            (3, "RESOLVED_NO_SIGNAL"),
            (5, "EMITTED"),
            (6, "EMITTED"),
        ]
        m1_alerts = conn.execute(
            "SELECT source_sequence_number,dispatch_seq FROM maintai_kafka_outbox "
            "WHERE topic='alert.signals' AND message_key=%s ORDER BY dispatch_seq",
            (m1,),
        ).fetchall()
        assert [(int(r["source_sequence_number"]), int(r["dispatch_seq"])) for r in m1_alerts] == [
            (1, 1),
            (5, 2),
            (6, 3),
        ]

    # Run OrderedOutboxRelay with an injected PublishUnavailable on first attempt to prove no duplicate logical events (#11)
    with KafkaPublisher(settings, "outbox", contracts=contracts) as pub:
        flaky_state = {"tripped": False}

        class FlakyPub:
            settings = pub.settings
            contracts = pub.contracts

            def publish_event(self, ev):
                if ev.topic == "alert.signals" and ev.key == m1 and not flaky_state["tripped"]:
                    flaky_state["tripped"] = True
                    raise PublishUnavailable("simulated transient broker timeout")
                return pub.publish_event(ev)

        relay = create_relay(ordered_database.factory, FlakyPub(), mode="ordered", batch_size=20)
        for _ in range(10):
            relay.run_once()
        with ordered_database.factory() as conn, conn.transaction():
            require_ordering(conn)
            conn.execute(
                "UPDATE maintai_kafka_outbox SET next_attempt_at=clock_timestamp() WHERE status='PENDING'"
            )
        while relay.run_once() > 0:
            pass

    # Verify actual alert.signals Kafka topic readback strictly equals [1, 5, 6] for m1 (0 duplicates)
    reader = RawKafkaConsumer(
        {
            **settings.client_config(),
            "group.id": "verify-" + uuid.uuid4().hex,
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    try:
        tps = [TopicPartition("alert.signals", p, start_alert_offsets[p]) for p in range(12)]
        reader.assign(tps)
        hi = {p: reader.get_watermark_offsets(TopicPartition("alert.signals", p), timeout=5)[1] for p in range(12)}
        seen_m1 = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and any(
            reader.position([TopicPartition("alert.signals", p)])[0].offset < hi[p]
            for p in range(12)
            if start_alert_offsets[p] < hi[p]
        ):
            msg = reader.poll(0.1)
            if msg is None or msg.error():
                continue
            ev = contracts.decode("alert.signals", msg.value())
            if ev.key == m1:
                seen_m1.append(ev.payload["event_id"])
        assert seen_m1 == [f"evt-{m1}-1", f"evt-{m1}-5", f"evt-{m1}-6"]
    finally:
        reader.close()


def test_blocked_disposition_resolution_optin_nosignal_and_registry_retry(ordered_database):
    contracts = default_contracts()
    # 1. Default safe mode (BLOCKED) + transient RegistryUnavailable
    class CustomProviders(Providers):
        def __init__(self):
            super().__init__(probability=0.9)
            self.fail_events = {"evt-M-001-1"}
            self.registry_down = {"evt-M-001-3"}

        def lookup(self, machine_id, *, correlation_id):
            return super().lookup(machine_id, correlation_id=correlation_id)

        def predict(self, *, event_id, machine_id, features, correlation_id):
            if event_id in self.fail_events:
                self.calls += 1
                return Providers(failure="ML_TIMEOUT").predict(
                    event_id=event_id,
                    machine_id=machine_id,
                    features=features,
                    correlation_id=correlation_id,
                )
            return super().predict(
                event_id=event_id,
                machine_id=machine_id,
                features=features,
                correlation_id=correlation_id,
            )

    providers = CustomProviders()
    app = hybrid_handler(ordered_database, providers, unavailable_disposition="BLOCKED")
    consumer = KeyedProcessingHandler(app)
    assignment = consumer.acquire([TopicPartition("sensor.readings", 0)])[("sensor.readings", 0)]

    ev1, rec1 = reading("M-001", 1, 1)
    ev2, rec2 = reading("M-001", 2, 2)
    ev_other, rec_other = reading("M-002", 1, 3)

    t1 = consumer.admit(ev1, rec1, WorkContext(assignment, time.monotonic() + 15))
    t2 = consumer.admit(ev2, rec2, WorkContext(assignment, time.monotonic() + 15))
    t_other = consumer.admit(ev_other, rec_other, WorkContext(assignment, time.monotonic() + 15))

    consumer.handle(ev1, rec1, WorkContext(assignment, time.monotonic() + 15, admission=t1))
    consumer.handle(ev2, rec2, WorkContext(assignment, time.monotonic() + 15, admission=t2))
    consumer.handle(ev_other, rec_other, WorkContext(assignment, time.monotonic() + 15, admission=t_other))

    with ordered_database.factory() as conn:
        blocked = app.resequencer.list_blocked(conn, consumer_group=consumer.consumer_group)
        assert len(blocked) == 1 and int(blocked[0]["admission_seq"]) == 1
        # M-001 frontier held at 0; M-002 alert emitted normally; inference.results published for M-001 seq=1&2
        alerts = conn.execute(
            "SELECT message_key,source_sequence_number FROM maintai_kafka_outbox WHERE topic='alert.signals'"
        ).fetchall()
        assert [(r["message_key"], int(r["source_sequence_number"])) for r in alerts] == [("M-002", 1)]
        results = conn.execute(
            "SELECT e.sequence_number, r.kind FROM processing_results r JOIN sensor_events e USING(event_id) "
            "WHERE e.machine_id='M-001' ORDER BY e.sequence_number"
        ).fetchall()
        assert [(int(r["sequence_number"]), r["kind"]) for r in results] == [
            (1, "INFERENCE_UNAVAILABLE"),
            (2, "PREDICTION_COMPLETED"),
        ]

        # Operator resolves BLOCKED -> seq=1 becomes RESOLVED_NO_SIGNAL, seq=2 immediately emits!
        with conn.transaction():
            drain = app.resequencer.resolve_blocked(
                conn,
                consumer_group=consumer.consumer_group,
                machine_id="M-001",
                admission_seq=1,
                action="SKIP_NO_SIGNAL",
                actor="oncall-engineer",
                note="ML timeout verified non-faulting",
                contracts=contracts,
            )
        assert drain["emitted"] == 1 and drain["resolved_no_signal"] == 1
        m1_alerts = conn.execute(
            "SELECT source_sequence_number FROM maintai_kafka_outbox WHERE topic='alert.signals' AND message_key='M-001'"
        ).fetchall()
        assert [int(r["source_sequence_number"]) for r in m1_alerts] == [2]

    # 2. Transient RegistryUnavailable stays PENDING and rolls back Stage C, then recovers on replay
    providers.registry_status = "UNAVAILABLE"
    ev3, rec3 = reading("M-001", 3, 4)
    t3 = consumer.admit(ev3, rec3, WorkContext(assignment, time.monotonic() + 15))
    with pytest.raises(RegistryUnavailable):
        consumer.handle(ev3, rec3, WorkContext(assignment, time.monotonic() + 15, admission=t3))
    with ordered_database.factory() as conn:
        row3 = conn.execute(
            "SELECT disposition FROM processing_alert_ledger WHERE message_id=%s", (ev3.message_id,)
        ).fetchone()
        assert row3["disposition"] == "PENDING"
    providers.registry_status = "FOUND"
    t3_replay = consumer.admit(ev3, rec3, WorkContext(assignment, time.monotonic() + 15))
    assert t3_replay.admission_seq == t3.admission_seq and not t3_replay.replayed
    consumer.handle(ev3, rec3, WorkContext(assignment, time.monotonic() + 15, admission=t3_replay))
    with ordered_database.factory() as conn:
        row3_done = conn.execute(
            "SELECT disposition FROM processing_alert_ledger WHERE message_id=%s", (ev3.message_id,)
        ).fetchone()
        assert row3_done["disposition"] == "EMITTED"

    # 3. Explicit opt-in unavailable_disposition='NO_SIGNAL' resolves frontier without blocking
    optin_app = hybrid_handler(
        ordered_database,
        CustomProviders(),
        group="hyb-optin",
        unavailable_disposition="NO_SIGNAL",
    )
    optin_consumer = KeyedProcessingHandler(optin_app)
    optin_assign = optin_consumer.acquire([TopicPartition("sensor.readings", 0)])[
        ("sensor.readings", 0)
    ]
    ev_opt, rec_opt = reading("M-003", 1, 10, event_id="evt-M-001-1-opt")
    optin_app.predict = lambda **kw: Providers(failure="ML_5XX").predict(**kw)
    t_opt = optin_consumer.admit(ev_opt, rec_opt, WorkContext(optin_assign, time.monotonic() + 15))
    optin_consumer.handle(
        ev_opt, rec_opt, WorkContext(optin_assign, time.monotonic() + 15, admission=t_opt)
    )
    with ordered_database.factory() as conn:
        row_opt = conn.execute(
            "SELECT disposition,disposition_reason FROM processing_alert_ledger "
            "WHERE consumer_group='hyb-optin' AND message_id=%s",
            (ev_opt.message_id,),
        ).fetchone()
        assert row_opt["disposition"] == "RESOLVED_NO_SIGNAL"
        assert row_opt["disposition_reason"] == "UNAVAILABLE_OPT_IN_NO_SIGNAL"


def test_crash_after_stage_a_and_after_stage_c_recover_idempotently(ordered_database):
    providers = Providers(probability=0.9)
    app = hybrid_handler(ordered_database, providers)
    consumer = KeyedProcessingHandler(app)
    assignment = consumer.acquire([TopicPartition("sensor.readings", 0)])[("sensor.readings", 0)]

    ev1, rec1 = reading("M-001", 1, 1)
    ev2, rec2 = reading("M-001", 2, 2)

    # Admit both seq=1 and seq=2 (Stage A commits)
    t1 = consumer.admit(ev1, rec1, WorkContext(assignment, time.monotonic() + 15))
    t2 = consumer.admit(ev2, rec2, WorkContext(assignment, time.monotonic() + 15))

    # Crash simulation A: seq=1 crashes during Stage B before Stage C; seq=2 completes Stage C out-of-order
    orig_predict = app.predict

    def crash_seq1(**kwargs):
        if kwargs["event_id"] == ev1.message_id:
            raise RuntimeError("simulated worker crash before Stage C")
        return orig_predict(**kwargs)

    app.predict = crash_seq1
    with pytest.raises(RuntimeError):
        consumer.handle(ev1, rec1, WorkContext(assignment, time.monotonic() + 15, admission=t1))
    consumer.handle(ev2, rec2, WorkContext(assignment, time.monotonic() + 15, admission=t2))
    assert providers.calls == 1  # seq=2 only

    # Crash simulation B: Pod restarts BEFORE offset commit, replays BOTH seq=2 and seq=1!
    app.predict = orig_predict
    restarted = KeyedProcessingHandler(app)
    a2 = restarted.acquire([TopicPartition("sensor.readings", 0)])[("sensor.readings", 0)]

    t1_re = restarted.admit(ev1, rec1, WorkContext(a2, time.monotonic() + 15))
    t2_re = restarted.admit(ev2, rec2, WorkContext(a2, time.monotonic() + 15))
    assert t1_re.admission_seq == t1.admission_seq and not t1_re.replayed
    assert t2_re.admission_seq == t2.admission_seq and t2_re.replayed

    # Replaying seq=2 does NOT invoke ML again
    restarted.handle(ev2, rec2, WorkContext(a2, time.monotonic() + 15, admission=t2_re))
    assert providers.calls == 1

    # Replaying seq=1 completes ML and drains both seq=1 and seq=2 to alert.signals in order [1, 2]!
    restarted.handle(ev1, rec1, WorkContext(a2, time.monotonic() + 15, admission=t1_re))
    assert providers.calls == 2
    with ordered_database.factory() as conn:
        sigs = conn.execute(
            "SELECT source_sequence_number,dispatch_seq FROM maintai_kafka_outbox "
            "WHERE topic='alert.signals' ORDER BY dispatch_seq"
        ).fetchall()
        assert [(int(r["source_sequence_number"]), int(r["dispatch_seq"])) for r in sigs] == [
            (1, 1),
            (2, 2),
        ]


def test_rebalance_fences_stale_worker_in_stage_c(ordered_database):
    entered, resume = threading.Event(), threading.Event()
    providers = Providers(probability=0.9)
    app = hybrid_handler(ordered_database, providers)
    orig_predict = app.predict

    def slow_predict(**kwargs):
        entered.set()
        assert resume.wait(5)
        return orig_predict(**kwargs)

    app.predict = slow_predict
    old = KeyedProcessingHandler(app)
    old_assign = old.acquire([TopicPartition("sensor.readings", 0)])[("sensor.readings", 0)]
    ev1, rec1 = reading("M-001", 1, 1)
    t1 = old.admit(ev1, rec1, WorkContext(old_assign, time.monotonic() + 15))

    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(
            old.handle, ev1, rec1, WorkContext(old_assign, time.monotonic() + 15, admission=t1)
        )
        try:
            assert entered.wait(5)
            new_app = hybrid_handler(ordered_database, Providers(probability=0.9))
            new = KeyedProcessingHandler(new_app)
            new_assign = new.acquire([TopicPartition("sensor.readings", 0)])[("sensor.readings", 0)]
            resume.set()
            with pytest.raises(LeaseLost):
                future.result(timeout=5)
            with ordered_database.factory() as conn:
                assert conn.execute("SELECT count(*) AS n FROM processing_results").fetchone()["n"] == 0
            new_t1 = new.admit(ev1, rec1, WorkContext(new_assign, time.monotonic() + 15))
            new.handle(ev1, rec1, WorkContext(new_assign, time.monotonic() + 15, admission=new_t1))
        finally:
            resume.set()
    with ordered_database.factory() as conn:
        assert conn.execute("SELECT count(*) AS n FROM processing_results").fetchone()["n"] == 1


def test_r101_critical_slow_and_r102_normal_fast_end_to_end_ordering(ordered_database):
    """Explicit §9.1 & §9.2 verification: R101 CRITICAL (slow) + R102 NORMAL (fast)."""
    from psycopg import sql as psql
    from sqlalchemy.engine import make_url
    from maintai_messaging.postgres import connect as pg_connect
    from backend.shared.alerts.handler import AlertHandler
    from backend.shared.alerts.repository import AlertRepository
    from backend.shared.persistence.migration import upgrade as upgrade_biz
    from backend.shared.persistence.projections import ProjectionRepository
    from backend.shared.persistence.registry import RegistryRepository
    from backend.shared.projections.projector import Projector
    from processing.shared.persistence.status import StatusRepository

    providers = Providers(probability=0.92)  # >= 0.85 -> CRITICAL
    app = hybrid_handler(ordered_database, providers)
    base_predict = app.predict
    barrier = threading.Barrier(2)
    r102_persisted = threading.Event()

    def predict(**kwargs):
        barrier.wait(timeout=5)
        if kwargs["event_id"] == "evt-M-001-101":
            assert r102_persisted.wait(5)
            time.sleep(0.02)
            return Providers(probability=0.92).predict(**kwargs)  # CRITICAL
        return Providers(probability=0.08).predict(**kwargs)  # R102 NORMAL

    app.predict = predict
    consumer = KeyedProcessingHandler(app)
    assignment = consumer.acquire([TopicPartition("sensor.readings", 0)])[("sensor.readings", 0)]

    ev101, rec101 = reading("M-001", 101, 101)
    ev102, rec102 = reading("M-001", 102, 102)
    t101 = consumer.admit(ev101, rec101, WorkContext(assignment, time.monotonic() + 15))
    t102 = consumer.admit(ev102, rec102, WorkContext(assignment, time.monotonic() + 15))
    ctx101 = WorkContext(assignment, time.monotonic() + 15, admission=t101)
    ctx102 = WorkContext(assignment, time.monotonic() + 15, admission=t102)

    def run_102():
        res = consumer.handle(ev102, rec102, ctx102)
        r102_persisted.set()
        return res

    with ThreadPoolExecutor(2) as pool:
        f101 = pool.submit(consumer.handle, ev101, rec101, ctx101)
        f102 = pool.submit(run_102)
        f102.result(timeout=10)
        # Out-of-order point: R102 (NORMAL) committed inference.results first, while
        # processing_alert_ledger holds R102 (NO_SIGNAL) waiting on R101 (PENDING).
        with ordered_database.factory() as conn:
            inf_rows = conn.execute(
                "SELECT source_sequence_number FROM maintai_kafka_outbox "
                "WHERE topic='inference.results' ORDER BY created_at"
            ).fetchall()
            assert [int(r["source_sequence_number"]) for r in inf_rows] == [102]
            assert (
                conn.execute(
                    "SELECT count(*) AS n FROM maintai_kafka_outbox WHERE topic='alert.signals'"
                ).fetchone()["n"]
                == 0
            )
            ledger = conn.execute(
                "SELECT sequence_number, disposition FROM processing_alert_ledger ORDER BY admission_seq"
            ).fetchall()
            assert [(int(r["sequence_number"]), r["disposition"]) for r in ledger] == [
                (101, "PENDING"),
                (102, "NO_SIGNAL"),
            ]
        f101.result(timeout=10)

    assert base_predict is not None
    contracts = default_contracts()
    with ordered_database.factory() as conn:
        # Both R102 (enqueued first) and R101 (enqueued second) exist in history and outbox.
        assert conn.execute("SELECT count(*) AS n FROM processing_results").fetchone()["n"] == 2
        assert conn.execute("SELECT count(*) AS n FROM predictions").fetchone()["n"] == 2
        assert conn.execute("SELECT count(*) AS n FROM processing_conflicts").fetchone()["n"] == 0
        cursor = conn.execute(
            "SELECT last_sequence FROM maintai_processing_machine_cursor WHERE machine_id='M-001'"
        ).fetchone()
        assert int(cursor["last_sequence"]) == 102
        adm = conn.execute(
            "SELECT last_admitted_sequence, alert_frontier_seq FROM processing_admission_cursor WHERE machine_id='M-001'"
        ).fetchone()
        assert int(adm["last_admitted_sequence"]) == 102
        assert int(adm["alert_frontier_seq"]) == 2

        inf_outbox = conn.execute(
            "SELECT payload, message_id FROM maintai_kafka_outbox WHERE topic='inference.results' ORDER BY created_at"
        ).fetchall()
        sig_outbox = conn.execute(
            "SELECT payload, message_id, source_sequence_number, dispatch_seq "
            "FROM maintai_kafka_outbox WHERE topic='alert.signals' ORDER BY dispatch_seq"
        ).fetchall()
        assert [(int(r["source_sequence_number"]), int(r["dispatch_seq"])) for r in sig_outbox] == [
            (101, 1)
        ]
        ledger_final = conn.execute(
            "SELECT sequence_number, disposition FROM processing_alert_ledger ORDER BY admission_seq"
        ).fetchall()
        assert [(int(r["sequence_number"]), r["disposition"]) for r in ledger_final] == [
            (101, "EMITTED"),
            (102, "RESOLVED_NO_SIGNAL"),
        ]

    # Deliver out-of-order inference.results ([102 NORMAL, then 101 CRITICAL]) and ordered
    # alert.signals ([101 CRITICAL]) into Business DB, checking UI view + alert lifecycle.
    biz_name = "hyb_e2e_" + uuid.uuid4().hex[:12]
    with pg_connect(ordered_database.dsn) as conn:
        conn.autocommit = True
        conn.execute(psql.SQL("CREATE DATABASE {}").format(psql.Identifier(biz_name)))
    biz_dsn = make_url(ordered_database.dsn).set(database=biz_name).render_as_string(hide_password=False)
    try:
        upgrade_biz(biz_dsn)
        RegistryRepository(lambda: pg_connect(biz_dsn)).provision(
            [{"machine_id": "M-001", "display_name": "One"}]
        )
        projector = Projector()
        for idx, row in enumerate(inf_outbox, start=1):
            ev = contracts.decode("inference.results", bytes(row["payload"]))
            with pg_connect(biz_dsn) as conn, conn.transaction():
                projector(
                    conn,
                    ev,
                    Record("inference.results", 0, idx, ev.key.encode(), ev.wire_bytes),
                    None,
                )

        status_repo = StatusRepository(ordered_database.factory)

        class LocalEvidenceClient:
            def fetch(self, payload):
                with ordered_database.factory() as pconn:
                    res = pconn.execute(
                        "SELECT result_id FROM processing_results WHERE event_id=%s",
                        (payload["event_id"],),
                    ).fetchone()
                return status_repo.prediction(res["result_id"])

        alert_repo = AlertRepository(lambda: pg_connect(biz_dsn))
        alert_handler = AlertHandler(
            lambda: pg_connect(biz_dsn), "business-alert-v1", LocalEvidenceClient(), repository=alert_repo
        )
        sig_ev = contracts.decode("alert.signals", bytes(sig_outbox[0]["payload"]))
        alert_handler(
            sig_ev, Record("alert.signals", 0, 1, sig_ev.key.encode(), sig_ev.wire_bytes)
        )

        proj_repo = ProjectionRepository(lambda: pg_connect(biz_dsn), alert_reader=alert_repo.active)
        live_view = proj_repo.live("M-001")
        detail_view = proj_repo.detail("M-001")
        assert live_view["latest_valid_prediction"]["sequence_number"] == 102
        assert live_view["latest_valid_prediction"]["prediction"] == "NORMAL"
        assert detail_view["state"] == "NORMAL"
        assert detail_view["latest_prediction"]["prediction"] == "NORMAL"
        # R101 CRITICAL alert was created and NOT auto-resolved by newer R102 NORMAL!
        assert len(detail_view["active_alerts"]) == 1
        alert_detail = alert_repo.detail(detail_view["active_alerts"][0], {"M-001"})["alert"]
        assert alert_detail["severity"] == "CRITICAL"
        assert alert_detail["status"] == "OPEN"
        with pg_connect(biz_dsn) as conn:
            a_row = conn.execute("SELECT status, severity, latest_source_sequence FROM alerts").fetchone()
            assert a_row["status"] == "OPEN"
            assert a_row["severity"] == "CRITICAL"
            assert int(a_row["latest_source_sequence"]) == 101
    finally:
        with pg_connect(ordered_database.dsn) as conn:
            conn.autocommit = True
            conn.execute(psql.SQL("DROP DATABASE {} WITH (FORCE)").format(psql.Identifier(biz_name)))
