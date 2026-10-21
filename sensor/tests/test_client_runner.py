import io
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from simulator.client import BackendClient
from simulator.config import SimulatorConfig
from simulator.dataset import DatasetReader
from simulator.event_factory import EventFactory
from simulator.fault_injection import FaultMode
from simulator.runner import ReplayRunner
from simulator.scenarios import ScenarioLoader


def payload(sample_csv):
    features = next(iter(DatasetReader(sample_csv))).features
    return (
        EventFactory(seed=42, machine_count=1, interval_ms=0)
        .create(
            features,
            0,
        )
        .model_dump(mode="json")
    )


def config(sample_csv, scenario_dir, **overrides):
    return SimulatorConfig(
        dataset_path=sample_csv,
        scenario_path=scenario_dir,
        limit=3,
        interval_ms=0,
        start_time=datetime(2026, 10, 12, tzinfo=UTC),
        run_id="test-run",
        **overrides,
    )


def test_real_http_request_contract(sample_csv, http_backend):
    url, backend = http_backend
    with BackendClient(url) as client:
        result = client.send(payload(sample_csv))
    assert result.outcome == "accepted"
    assert result.attempts == 1
    assert backend.predictions == 1


def test_duplicate_is_identical_and_creates_one_prediction(sample_csv, scenario_dir, backend):
    cfg = config(sample_csv, scenario_dir, fault_mode=FaultMode.DUPLICATE_EVENT)
    with BackendClient("http://backend", transport=httpx.MockTransport(backend.handle)) as client:
        stats = ReplayRunner(cfg, ScenarioLoader.load(cfg.scenario_yaml), client=client).run()
    assert stats.generated == 3
    assert stats.accepted == stats.deliveries == 6
    assert stats.duplicates == 3
    assert backend.predictions == 3
    for index in range(0, 6, 2):
        first, second = backend.requests[index : index + 2]
        assert first.content == second.content
        assert first.headers["x-correlation-id"] == second.headers["x-correlation-id"]
        assert first.headers["x-request-id"] != second.headers["x-request-id"]


@pytest.mark.parametrize(
    "mode",
    [
        FaultMode.MISSING_FIELD,
        FaultMode.INVALID_TYPE,
        FaultMode.OUT_OF_RANGE,
    ],
)
def test_invalid_events_rejected_without_mock_inference(sample_csv, scenario_dir, backend, mode):
    cfg = config(sample_csv, scenario_dir, fault_mode=mode)
    with BackendClient("http://backend", transport=httpx.MockTransport(backend.handle)) as client:
        stats = ReplayRunner(cfg, ScenarioLoader.load(cfg.scenario_yaml), client=client).run()
    assert stats.expected_rejections == stats.rejected == 3
    assert stats.exit_code == 0
    assert stats.retries == 0
    assert backend.predictions == 0


def test_timeout_after_server_commit_retries_without_duplicate_prediction(sample_csv, backend):
    def interrupted_response(request):
        response = backend.handle(request)
        if len(backend.requests) == 1:
            raise httpx.ReadTimeout("Lost response after backend committed", request=request)
        return response

    waits = []
    event = payload(sample_csv)
    with BackendClient(
        "http://backend",
        transport=httpx.MockTransport(interrupted_response),
        sleep=waits.append,
    ) as client:
        result = client.send(event)
    assert result.outcome == "accepted"
    assert result.attempts == 2
    assert backend.predictions == 1
    assert backend.requests[0].content == backend.requests[1].content
    assert (
        backend.requests[0].headers["x-correlation-id"]
        == backend.requests[1].headers["x-correlation-id"]
    )
    assert waits == [0.1]


@pytest.mark.parametrize("mode", [FaultMode.NETWORK_TIMEOUT, FaultMode.NETWORK_DISCONNECT])
def test_injected_transport_fault_recovers_with_same_event(sample_csv, backend, mode):
    with BackendClient(
        "http://backend",
        transport=httpx.MockTransport(backend.handle),
        sleep=lambda _: None,
    ) as client:
        result = client.send(payload(sample_csv), fault=mode)
    assert result.outcome == "accepted"
    assert result.attempts == 2
    assert backend.predictions == 1


@pytest.mark.parametrize(
    "status,retryable,attempts",
    [
        (503, True, 3),
        (504, True, 3),
        (429, True, 3),
        (503, False, 1),
        (409, False, 1),
    ],
)
def test_retry_policy_is_bounded_and_honors_backend(sample_csv, status, retryable, attempts):
    requests, sleeps = [], []

    def unavailable(request):
        requests.append(request)
        return httpx.Response(
            status, json={"error": {"code": "TEST_ERROR", "retryable": retryable}}
        )

    with BackendClient(
        "http://backend",
        transport=httpx.MockTransport(unavailable),
        sleep=sleeps.append,
    ) as client:
        result = client.send(payload(sample_csv))
    assert result.outcome == "failed"
    assert result.attempts == attempts == len(requests)
    assert sleeps == ([0.1, 0.2] if attempts == 3 else [])


def test_conflicting_duplicate_is_reported(sample_csv, backend):
    event = payload(sample_csv)
    with BackendClient("http://backend", transport=httpx.MockTransport(backend.handle)) as client:
        assert client.send(event, correlation_id="cor_test").outcome == "accepted"
        event["features"]["torque_nm"] += 1
        result = client.send(event, correlation_id="cor_test")
    assert result.outcome == "failed"
    assert result.status_code == 409
    assert result.error == "EVENT_CONFLICT"
    assert result.attempts == 1
    assert backend.predictions == 1


@pytest.mark.parametrize("body", [None, [], {}, {"validation_status": "VALID"}])
def test_malformed_200_response_never_becomes_success(sample_csv, body):
    with BackendClient(
        "http://backend",
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)),
    ) as client:
        result = client.send(payload(sample_csv))
    assert result.outcome == "failed"
    assert result.error == "INVALID_BACKEND_RESPONSE"


def test_wrong_trace_identity_is_reported(sample_csv, backend):
    def wrong_trace(request):
        body = backend.handle(request).json()
        body["correlation_id"] = "cor_unrelated"
        return httpx.Response(200, json=body)

    with BackendClient("http://backend", transport=httpx.MockTransport(wrong_trace)) as client:
        assert client.send(payload(sample_csv)).error == "INVALID_BACKEND_RESPONSE"


@pytest.mark.parametrize("wrong_trace", [False, True])
def test_standard_validation_error_envelope(sample_csv, wrong_trace):
    def validation_error(request):
        return httpx.Response(
            422,
            json={
                "error": {
                    "code": "VALIDATION_FAILED",
                    "message": "Sensor validation failed",
                    "retryable": False,
                    "details": {"field": "features.rotational_speed_rpm"},
                    "request_id": request.headers["x-request-id"],
                    "correlation_id": "cor_wrong"
                    if wrong_trace
                    else request.headers["x-correlation-id"],
                }
            },
        )

    with BackendClient("http://backend", transport=httpx.MockTransport(validation_error)) as client:
        result = client.send(payload(sample_csv))
    assert result.outcome == ("failed" if wrong_trace else "rejected")
    assert result.attempts == 1


@pytest.mark.parametrize("policy,generated", [("continue", 3), ("stop", 1)])
def test_backend_down_does_not_crash_replay(sample_csv, scenario_dir, policy, generated):
    def down(request):
        raise httpx.ConnectError("Backend disconnected", request=request)

    cfg = config(sample_csv, scenario_dir, on_error=policy)
    with BackendClient(
        "http://backend",
        transport=httpx.MockTransport(down),
        sleep=lambda _: None,
    ) as client:
        stats = ReplayRunner(cfg, ScenarioLoader.load(cfg.scenario_yaml), client=client).run()
    assert stats.generated == stats.failed == generated
    assert stats.unexpected_results == generated
    assert stats.retries == generated * 2
    assert stats.exit_code == 1


@pytest.mark.parametrize("machine_count", [1, 5, 10])
@pytest.mark.parametrize("speed", [1, 4])
def test_rate_control_balances_machines_and_preserves_logical_clock(
    sample_csv, scenario_dir, backend, machine_count, speed
):
    cfg = config(sample_csv, scenario_dir, loop_dataset=True).model_copy(
        update={
            "interval_ms": 1000,
            "speed": speed,
            "machine_count": machine_count,
            "limit": machine_count * 3,
        }
    )
    clock, sent_at = [0.0], []

    def sleep(delay):
        clock[0] += delay

    def handle(request):
        sent_at.append(clock[0])
        return backend.handle(request)

    with BackendClient("http://backend", transport=httpx.MockTransport(handle)) as client:
        stats = ReplayRunner(
            cfg,
            ScenarioLoader.load(cfg.scenario_yaml),
            client=client,
            sleep=sleep,
            monotonic=lambda: clock[0],
        ).run()
    assert stats.generated == stats.accepted == machine_count * 3
    assert sent_at == pytest.approx([i / machine_count / speed for i in range(cfg.limit)])
    events = [json.loads(request.content) for request in backend.requests]
    timestamps = [datetime.fromisoformat(event["received_at"]) for event in events]
    assert timestamps == [
        cfg.start_time + timedelta(milliseconds=i * 1000 / machine_count) for i in range(cfg.limit)
    ]
    assert {event["machine_id"] for event in events} == {
        f"M-{i:03d}" for i in range(1, machine_count + 1)
    }
    for slot in range(machine_count):
        readings = events[slot::machine_count]
        assert len({event["machine_id"] for event in readings}) == 1
        assert timestamps[slot + machine_count] - timestamps[slot] == timedelta(seconds=1)
        assert sent_at[slot + machine_count] - sent_at[slot] == pytest.approx(1 / speed)


def test_dry_run_payloads_are_reproducible_and_no_sleep(sample_csv, scenario_dir):
    cfg = config(sample_csv, scenario_dir, dry_run=True)
    outputs = [io.StringIO(), io.StringIO()]
    for output in outputs:
        stats = ReplayRunner(
            cfg,
            ScenarioLoader.load(cfg.scenario_yaml),
            output=output,
            sleep=lambda _: pytest.fail("Dry run must not sleep"),
        ).run()
        assert stats.generated == 3
        assert stats.deliveries == 0
    assert outputs[0].getvalue() == outputs[1].getvalue()


def test_dataset_loop_and_metrics_labels(sample_csv, scenario_dir):
    cfg = config(sample_csv, scenario_dir, dry_run=True, loop_dataset=True).model_copy(
        update={"limit": 12},
    )
    output = io.StringIO()
    stats = ReplayRunner(cfg, ScenarioLoader.load(cfg.scenario_yaml), output=output).run()
    assert stats.generated == 12
    assert len({json.loads(line)["event_id"] for line in output.getvalue().splitlines()}) == 12
    text = stats.prometheus_text()
    assert "maintai_simulator_generated_total 12" in text
    assert "event_id" not in text and "machine_id" not in text
