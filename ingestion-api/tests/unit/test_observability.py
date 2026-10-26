"""OBS-01 behavior: real gateway and exposition, controlled broker boundary."""

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from maintai_messaging.errors import PublishUnavailable, PublishUncertain
from starlette.testclient import TestClient

PATH = "/api/v1/sensor-readings"


def sample(obs, name, labels=None):
    return obs.registry.get_sample_value(name, labels or {}) or 0


@pytest.mark.parametrize(
    "body,status,reason,outcome",
    [
        (None, 202, None, "ACCEPTED"),
        (b"{", 400, "MALFORMED_JSON", "REJECTED"),
        (b"{}", 422, "VALIDATION_FAILED", "REJECTED"),
    ],
)
def test_ack_and_terminal_rejection_metrics(edge, payload, body, status, reason, outcome):
    app, pub, _ = edge
    with TestClient(app) as client:
        obs = app.state.runtime.obs
        for _ in range(2):  # ACKs, not unique event IDs.
            kwargs = (
                {"json": payload}
                if body is None
                else {"content": body, "headers": {"Content-Type": "application/json"}}
            )
            assert client.post(PATH, **kwargs).status_code == status
        assert len(pub.records) == 2
        assert sample(obs, "maintai_ingestion_accepted_total") == (2 if reason is None else 0)
        assert sample(obs, "maintai_ingestion_rejected_total", {"reason": reason}) == (
            2 if reason else 0
        )
        assert (
            sample(obs, "maintai_ingestion_ack_duration_seconds_count", {"outcome": outcome}) == 2
        )
        assert (
            sample(
                obs,
                "maintai_http_requests_total",
                {"route": PATH, "method": "POST", "status": str(status)},
            )
            == 2
        )
        text = client.get("/metrics").text
        assert "maintai_ingestion_publish_total" not in text
        assert "maintai_ingestion_http_requests_total" not in text
        assert "maintai_ingestion_inflight_requests" not in text
        assert payload["event_id"] not in text and "M-001" not in text
        buckets = [
            s.labels["le"]
            for m in obs.registry.collect()
            for s in m.samples
            if s.name == "maintai_ingestion_ack_duration_seconds_bucket"
        ]
        assert buckets == [
            "0.005",
            "0.01",
            "0.025",
            "0.05",
            "0.1",
            "0.25",
            "0.5",
            "1.0",
            "2.0",
            "5.0",
            "10.0",
            "+Inf",
        ]


@pytest.mark.parametrize("failure,status", [(PublishUnavailable(), 503), (PublishUncertain(), 504)])
@pytest.mark.parametrize("body", [None, b"{"])
def test_publish_failure_is_not_a_completed_rejection(edge, payload, failure, status, body):
    app, pub, _ = edge
    pub.failure = failure
    with TestClient(app) as client:
        kwargs = (
            {"json": payload}
            if body is None
            else {"content": body, "headers": {"Content-Type": "application/json"}}
        )
        assert client.post(PATH, **kwargs).status_code == status
        obs = app.state.runtime.obs
        assert sample(obs, "maintai_ingestion_accepted_total") == 0
        assert sample(obs, "maintai_ingestion_rejected_total", {"reason": "MALFORMED_JSON"}) == 0
        assert (
            sample(obs, "maintai_ingestion_publish_failures_total", {"error_type": "UNAVAILABLE"})
            == 1
        )
        assert (
            sample(
                obs,
                "maintai_dependency_errors_total",
                {"dependency": "kafka", "error_type": "UNAVAILABLE"},
            )
            == 1
        )
        assert (
            sample(obs, "maintai_ingestion_ack_duration_seconds_count", {"outcome": "ERROR"}) == 1
        )


@pytest.mark.parametrize(
    "case,status,reason",
    [
        ("scope", 403, "AUTH"),
        ("size", 413, "SIZE"),
        ("media", 415, "OTHER"),
        ("identity", 400, "OTHER"),
        ("admission", 429, "OTHER"),
        ("auth", 401, "AUTH"),
    ],
)
def test_edge_rejection_has_no_fabricated_ack(edge, payload, case, status, reason):
    app, pub, _ = edge
    with TestClient(app) as client:
        rt = app.state.runtime
        kwargs = {"json": payload}
        if case == "scope":
            payload["machine_id"] = "M-999"
        elif case == "size":
            kwargs = {"content": b"x" * 262145, "headers": {"Content-Type": "application/json"}}
        elif case == "media":
            kwargs = {"content": b"{}"}
        elif case == "identity":
            kwargs["headers"] = {"X-Request-ID": "x" * 257}
        elif case == "admission":
            for _ in range(rt.settings.max_inflight):
                assert rt.gate.try_acquire()
        elif case == "auth":
            from maintai_ingestion.errors import EdgeFailure

            def deny(_):
                raise EdgeFailure(401, "UNAUTHORIZED")

            rt.verifier.verify = deny
        try:
            assert client.post(PATH, **kwargs).status_code == status
            assert sample(rt.obs, "maintai_ingestion_rejected_total", {"reason": reason}) == 1
            assert not pub.records
            assert not any(
                s.name == "maintai_ingestion_ack_duration_seconds_count"
                for m in rt.obs.registry.collect()
                for s in m.samples
            )
        finally:
            if case == "admission":
                for _ in range(rt.settings.max_inflight):
                    rt.gate.release()


def test_health_and_scrape_are_excluded_and_routes_are_bounded(edge):
    app, _, _ = edge
    with TestClient(app) as client:
        for path in ["/health/live", "/health/ready", "/metrics", "/metrics"]:
            assert client.get(path).status_code == 200
        obs = app.state.runtime.obs
        assert not any(
            s.name == "maintai_http_requests_total"
            for m in obs.registry.collect()
            for s in m.samples
        )
        assert client.get("/random-secret-id?token=secret").status_code == 404
        assert (
            sample(
                obs,
                "maintai_http_requests_total",
                {"route": "__unmatched__", "method": "GET", "status": "404"},
            )
            == 1
        )
        assert "secret" not in client.get("/metrics").text


def test_ack_latency_includes_auth_before_publish(edge, payload):
    app, pub, _ = edge
    clock = [100.0]
    with TestClient(app) as client:
        rt = app.state.runtime
        rt.obs.clock = lambda: clock[0]
        verify, publish = rt.verifier.verify, pub.publish

        def slow_verify(header):
            clock[0] += 2
            return verify(header)

        def slow_publish(topic, body):
            clock[0] += 3
            return publish(topic, body)

        rt.verifier.verify, pub.publish = slow_verify, slow_publish
        assert client.post(PATH, json=payload).status_code == 202
        assert (
            sample(rt.obs, "maintai_ingestion_ack_duration_seconds_sum", {"outcome": "ACCEPTED"})
            == 5
        )


def test_instrumentation_failure_cannot_replace_broker_ack(edge, payload, monkeypatch, caplog):
    app, pub, _ = edge
    with TestClient(app) as client:
        obs = app.state.runtime.obs

        def broken(*args, **kwargs):
            raise RuntimeError("SECRET-do-not-log")

        monkeypatch.setattr(obs.accepted, "inc", broken)
        monkeypatch.setattr(obs.requests, "labels", broken)
        assert client.post(PATH, json=payload).status_code == 202
        assert len(pub.records) == 1 and app.state.runtime.gate.active == 0
    assert "instrumentation" in caplog.text.lower()
    assert "SECRET" not in caplog.text


def test_concurrent_admissions_keep_separate_start_times(edge, payload):
    app, pub, _ = edge
    first_entered, release_first = threading.Event(), threading.Event()
    clock = [100.0]
    with TestClient(app) as client, ThreadPoolExecutor() as pool:
        rt = app.state.runtime
        rt.obs.clock = lambda: clock[0]
        verify = rt.verifier.verify

        def ordered_verify(header):
            if header == "first":
                first_entered.set()
                assert release_first.wait(3)
            return verify(header)

        rt.verifier.verify = ordered_verify
        first = pool.submit(client.post, PATH, json=payload, headers={"Authorization": "first"})
        try:
            assert first_entered.wait(2)
            clock[0] = 110.0
            assert client.post(PATH, json=payload).status_code == 202  # duration=0
            clock[0] = 120.0
        finally:
            release_first.set()
        assert first.result(timeout=3).status_code == 202
        assert len(pub.records) == 2
        assert (
            sample(rt.obs, "maintai_ingestion_ack_duration_seconds_sum", {"outcome": "ACCEPTED"})
            == 20
        )


@pytest.mark.parametrize(
    "body,outcome,reason",
    [
        (None, "ACCEPTED", None),
        (b"{", "REJECTED", "MALFORMED_JSON"),
        (b"{}", "REJECTED", "VALIDATION_FAILED"),
    ],
)
def test_cancelled_http_worker_still_records_actual_ack(edge, payload, body, outcome, reason):
    import httpx

    app, pub, _ = edge
    pub.barrier = threading.Event()

    async def scenario():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://fixture"
            ) as client:
                kwargs = (
                    {"json": payload}
                    if body is None
                    else {"content": body, "headers": {"Content-Type": "application/json"}}
                )
                task = asyncio.create_task(client.post(PATH, **kwargs))
                try:
                    while not pub.started.is_set():
                        await asyncio.sleep(0.001)
                    task.cancel()
                finally:
                    pub.barrier.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
                await app.state.runtime.gate.empty.wait()
                assert len(pub.records) == 1
                assert sample(app.state.runtime.obs, "maintai_ingestion_accepted_total") == (
                    0 if reason else 1
                )
                if reason:
                    assert (
                        sample(
                            app.state.runtime.obs,
                            "maintai_ingestion_rejected_total",
                            {"reason": reason},
                        )
                        == 1
                    )
                assert (
                    sample(
                        app.state.runtime.obs,
                        "maintai_ingestion_ack_duration_seconds_count",
                        {"outcome": outcome},
                    )
                    == 1
                )

    asyncio.run(scenario())


@pytest.mark.parametrize("failure,status", [(None, 202), (PublishUncertain(), 504)])
def test_failed_instrumentation_logger_preserves_transport_result(
    edge, payload, monkeypatch, failure, status
):
    import logging

    app, pub, _ = edge
    pub.failure = failure
    with TestClient(app) as client:
        obs = app.state.runtime.obs

        def broken(*args, **kwargs):
            raise RuntimeError("SECRET")

        monkeypatch.setattr(obs.accepted, "inc", broken)
        monkeypatch.setattr(obs.publish_failures, "labels", broken)
        monkeypatch.setattr(logging.getLogger("maintai.ingestion"), "error", broken)
        assert client.post(PATH, json=payload).status_code == status
        assert app.state.runtime.gate.active == 0


@pytest.mark.parametrize(
    "failure,kind",
    [
        (TimeoutError("SECRET"), "TIMEOUT"),
        (ConnectionError("SECRET"), "UNAVAILABLE"),
        ({"keys": []}, "INVALID_RESPONSE"),
    ],
)
def test_jwks_dependency_counts_actual_failed_fetch_only(settings, monkeypatch, failure, kind):
    from dataclasses import replace

    from maintai_ingestion.runtime import build_runtime

    from ..fakes import FakePublisher, Probe

    def fetch():
        if isinstance(failure, Exception):
            raise failure
        return failure

    monkeypatch.setattr("maintai_ingestion.runtime.jwks_fetcher", lambda *args: fetch)
    rt = build_runtime(
        replace(settings, auth_mode="oidc"), publisher_factory=FakePublisher, probe=Probe()
    )
    try:
        assert not rt.verifier.check()
        assert not rt.verifier.check()  # Cooldown is not another dependency attempt.
        assert (
            sample(
                rt.obs,
                "maintai_dependency_errors_total",
                {"dependency": "oidc", "error_type": kind},
            )
            == 1
        )
    finally:
        rt.publisher.close()


@pytest.mark.parametrize(
    "started,end,reason",
    [
        (None, 100.0, "SOURCE_TIME_MISSING"),
        (101.0, 100.0, "CLOCK_SKEW"),
        (100.0, float("nan"), "INVALID_VALUE"),
    ],
)
def test_invalid_ack_latency_is_dropped_not_clamped(settings, started, end, reason):
    from maintai_ingestion.observability import (
        Observability,
        ObservedPublisher,
        RequestObservation,
        request_observation,
    )

    from ..fakes import FakePublisher

    obs = Observability(clock=lambda: end)
    token = request_observation.set(RequestObservation(admitted_at=started))
    try:
        # A missing/bad timer must not erase a real transport failure observation.
        pub = FakePublisher(settings.kafka)
        pub.failure = PublishUncertain()
        import json
        from pathlib import Path

        wire = json.loads(
            (
                Path(__file__).resolve().parents[3] / "contracts/kafka/examples/sensor-reading.json"
            ).read_text()
        )
        with pytest.raises(PublishUncertain):
            ObservedPublisher(pub, obs).publish("sensor.readings", wire)
        assert sample(obs, "maintai_metric_samples_dropped_total", {"reason": reason}) == 1
        assert (
            sample(obs, "maintai_ingestion_ack_duration_seconds_count", {"outcome": "ERROR"}) == 0
        )
    finally:
        request_observation.reset(token)
