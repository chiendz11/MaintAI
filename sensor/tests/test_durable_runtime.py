"""Failure-boundary tests against the durable journal and real delivery client."""

import json
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from simulator.ordering.store import ControlError, StateStore
from simulator.scheduling.engine import SensorEngine


def ack(request):
    payload = json.loads(request.content)
    return httpx.Response(
        202,
        json={
            "event_id": payload["event_id"],
            "machine_id": payload["machine_id"],
            "ingestion_status": "ACCEPTED",
            "processing_status": "PENDING",
            "server_accepted_at": "2026-10-20T00:00:00Z",
            "status_url": "https://business.example.invalid/api/v1/sensor-events/x/status",
            "correlation_id": request.headers["x-correlation-id"],
        },
    )


def test_restart_preserves_sequence_payload_cursor_and_pause(runtime_settings):
    engine = SensorEngine(runtime_settings)
    first = engine.generate_once("M-001")
    engine.store.command(
        "alice",
        "pause-1",
        "M-001",
        "pause",
        {"expected_control_version": 0},
        set(engine.scenarios),
        100,
    )
    original = engine.store.due(time.time(), 1000, 10)[0]["payload"]
    engine.close()
    again = SensorEngine(runtime_settings)
    try:
        assert again.store.get("M-001")["stream_state"] == "PAUSED"
        assert again.generate_once("M-001") is None
        assert again.store.due(time.time(), 1000, 10)[0]["payload"] == original
        again.store.command(
            "alice",
            "resume-1",
            "M-001",
            "resume",
            {"expected_control_version": 1},
            set(again.scenarios),
            100,
        )
        second = again.generate_once("M-001")
        assert second["sequence_number"] == first["sequence_number"] + 1
        assert second["event_id"] != first["event_id"]
        assert second["stream_epoch"] != first["stream_epoch"]
        assert second["features"] != first["features"]  # Cursor did not reset to row0.
    finally:
        again.close()


def test_receipt_replay_survives_restart_before_old_version_check(runtime_settings):
    engine = SensorEngine(runtime_settings)
    store = engine.store
    result = store.command(
        "alice",
        "same",
        "M-001",
        "pause",
        {"expected_control_version": 0},
        set(engine.scenarios),
        100,
    )
    engine.close()
    again = SensorEngine(runtime_settings)
    try:
        assert (
            again.store.command(
                "alice",
                "same",
                "M-001",
                "pause",
                {"expected_control_version": 0},
                set(again.scenarios),
                100,
            )
            == result
        )
        with pytest.raises(ControlError) as conflict:
            again.store.command(
                "alice",
                "same",
                "M-001",
                "resume",
                {"expected_control_version": 1},
                set(again.scenarios),
                100,
            )
        assert conflict.value.code == "IDEMPOTENCY_CONFLICT"
    finally:
        again.close()


def test_transaction_rollback_never_consumes_sequence(runtime_settings):
    engine = SensorEngine(runtime_settings)
    try:

        def fail(row):
            raise RuntimeError("Generation failed before commit")

        with pytest.raises(RuntimeError):
            engine.store.generate("M-001", fail, max_records=10, max_bytes=10000)
        assert engine.generate_once("M-001")["sequence_number"] == 1
        with ThreadPoolExecutor(max_workers=4) as pool:
            events = list(pool.map(lambda _: engine.generate_once("M-001"), range(12)))
        assert sorted(e["sequence_number"] for e in events) == list(range(2, 14))
        assert len({e["event_id"] for e in events}) == 12
    finally:
        engine.close()


def test_process_lock_rejects_second_controller(runtime_settings):
    engine = SensorEngine(runtime_settings)
    try:
        with pytest.raises(ValueError, match="Another Sensor"):
            StateStore(runtime_settings.state_path)
    finally:
        engine.close()


def test_lost_ack_after_accept_restarts_same_event_and_paused_spool_drains(runtime_settings):
    requests = []

    def lost(request):
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ReadTimeout("Response lost after accepted")
        return ack(request)

    engine = SensorEngine(runtime_settings, transport=httpx.MockTransport(lost))
    event = engine.generate_once("M-001")
    engine.store.command(
        "alice", "p", "M-001", "pause", {"expected_control_version": 0}, set(engine.scenarios), 100
    )
    engine.deliver(engine.store.due(time.time(), 1000, 10)[0])
    assert engine.store.summary()["counts"] == {"pending": 1}
    engine.close()
    again = SensorEngine(runtime_settings, transport=httpx.MockTransport(lost))
    try:
        row = again.store.due(time.time() + 60, 1000, 10)[0]
        again.deliver(row)
        assert not again.store.summary()["counts"]
        assert requests[0].content == requests[1].content
        assert requests[0].headers["x-correlation-id"] == requests[1].headers["x-correlation-id"]
        assert json.loads(requests[1].content)["event_id"] == event["event_id"]
        assert again.store.get("M-001")["stream_state"] == "PAUSED"
    finally:
        again.close()


def test_backoff_one_machine_does_not_block_another(runtime_settings):
    gate = __import__("threading").Event()

    def delayed(request):
        if json.loads(request.content)["machine_id"] == "M-001":
            gate.wait(2)
        return ack(request)

    engine = SensorEngine(runtime_settings, transport=httpx.MockTransport(delayed))
    try:
        engine.generate_once("M-001")
        engine.generate_once("M-002")
        engine.dispatch_once()
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline and len(engine.store.inspect()) != 1:
            time.sleep(0.01)
        assert [r["machine_id"] for r in engine.store.inspect()] == ["M-001"]
        assert engine.generate_once("M-002")["sequence_number"] == 2
    finally:
        gate.set()
        engine.close()


def test_quota_ttl_auth_block_and_manual_repair(runtime_settings):
    settings = runtime_settings.model_copy(update={"spool_max_records": 2})
    engine = SensorEngine(settings, transport=httpx.MockTransport(lambda _: httpx.Response(401)))
    try:
        first = engine.generate_once("M-001")
        second = engine.generate_once("M-001")
        assert engine.generate_once("M-002") is None
        row = engine.store.due(time.time(), 1000, 10)[0]
        engine.deliver(row)
        assert engine.store.summary()["counts"] == {"auth": 1, "pending": 1}
        assert not engine.store.due(time.time() + 1, 1000, 10)  # No credential spam.
        engine.store.repair(first["event_id"], "retry", "credential rotated")
        assert len(engine.store.due(time.time(), 1000, 10)) == 1
        engine.store.due(time.time() + 2000, 1000, 10)
        assert engine.store.summary()["counts"] == {"expired": 2}
        assert engine.generate_once("M-002") is None  # Never silently delete an expired payload.
        engine.store.repair(second["event_id"], "discard", "operator exported evidence")
        assert engine.generate_once("M-002") is not None
    finally:
        engine.close()


def test_byte_quota_and_source_config_change_fail_closed(runtime_settings):
    engine = SensorEngine(runtime_settings.model_copy(update={"spool_max_bytes": 5}))
    assert engine.generate_once("M-001") is None
    engine.close()
    with pytest.raises(ValueError, match="configuration differs"):
        SensorEngine(runtime_settings.model_copy(update={"seed": 888}))


def test_metrics_count_commit_and_real_attempt_not_browser(runtime_settings):
    engine = SensorEngine(runtime_settings, transport=httpx.MockTransport(ack))
    try:
        engine.generate_once("M-001")
        engine.deliver(engine.store.due(time.time(), 1000, 1)[0])
        engine.metrics.refresh(engine.store)
        r = engine.metrics.registry
        assert r.get_sample_value("maintai_simulator_generated_total") == 1
        assert (
            r.get_sample_value("maintai_simulator_send_attempts_total", {"outcome": "ACCEPTED"})
            == 1
        )
        assert r.get_sample_value("maintai_simulator_spool_pending_count") == 0
    finally:
        engine.close()


def test_concurrent_control_versions_have_one_winner(runtime_settings):
    engine = SensorEngine(runtime_settings)
    try:

        def pause(key):
            try:
                engine.store.command(
                    "alice",
                    key,
                    "M-001",
                    "pause",
                    {"expected_control_version": 0},
                    set(engine.scenarios),
                    100,
                )
                return 200
            except ControlError as error:
                return error.status

        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sorted(pool.map(pause, ["one", "two"])) == [200, 409]
        assert engine.store.get("M-001")["control_version"] == 1
    finally:
        engine.close()


def test_control_receipt_capacity_never_silently_expires_old_key(runtime_settings):
    engine = SensorEngine(runtime_settings)
    try:
        first = engine.store.command(
            "alice",
            "one",
            "M-001",
            "pause",
            {"expected_control_version": 0},
            set(engine.scenarios),
            1,
        )
        with pytest.raises(ControlError) as error:
            engine.store.command(
                "alice",
                "two",
                "M-001",
                "resume",
                {"expected_control_version": 1},
                set(engine.scenarios),
                1,
            )
        assert error.value.status == 503
        assert (
            engine.store.command(
                "alice",
                "one",
                "M-001",
                "pause",
                {"expected_control_version": 0},
                set(engine.scenarios),
                1,
            )
            == first
        )
    finally:
        engine.close()


def test_recovery_input_ramp_holds_baseline_and_never_forces_risk(runtime_settings):
    engine = SensorEngine(runtime_settings)
    try:
        before = engine.records[0].features
        row = {
            "machine_id": "M-001",
            "row_cursor": 0,
            "scenario": "recovery",
            "scenario_step": 0,
            "sequence_number": 1,
        }
        first = engine.build(row)
        last = engine.build({**row, "scenario_step": 299})
        after = engine.build({**row, "scenario_step": 900})
        assert first["features"]["torque_nm"] == before.torque_nm + 16
        assert last["features"] == after["features"] == before.model_dump()
        assert not {"risk", "prediction", "machine_state", "alert_id"}.intersection(first)
    finally:
        engine.close()


@pytest.mark.parametrize("mode,expected_requests", [("duplicate_event", 3), ("network_timeout", 1)])
def test_runtime_fault_every_is_per_machine_and_not_every_delivery(
    runtime_settings, mode, expected_requests
):
    from simulator.fault_injection import FaultMode

    calls = []

    def accept(request):
        calls.append(request)
        return ack(request)

    settings = runtime_settings.model_copy(update={"fault_mode": FaultMode(mode), "fault_every": 2})
    engine = SensorEngine(settings, transport=httpx.MockTransport(accept))
    try:
        engine.generate_once("M-001")
        engine.deliver(engine.store.due(time.time(), 1000, 1)[0])
        assert not engine.store.summary()["counts"]
        engine.generate_once("M-001")
        engine.deliver(engine.store.due(time.time(), 1000, 1)[0])
        assert len(calls) == expected_requests
    finally:
        engine.close()


def test_jitter_cannot_exceed_configured_backoff_cap(runtime_settings, monkeypatch):
    monkeypatch.setattr("simulator.scheduling.engine.random.uniform", lambda *args: 1.2)
    engine = SensorEngine(
        runtime_settings, transport=httpx.MockTransport(lambda _: httpx.Response(503))
    )
    try:
        engine.generate_once("M-001")
        row = engine.store.due(time.time(), 1000, 1)[0]
        row["attempts"] = 100
        before = time.time()
        engine.deliver(row)
        with engine.store.transaction() as db:
            next_at = db.execute("SELECT next_at FROM spool").fetchone()[0]
        assert next_at - before <= engine.settings.retry_max_seconds + 0.1
    finally:
        engine.close()
