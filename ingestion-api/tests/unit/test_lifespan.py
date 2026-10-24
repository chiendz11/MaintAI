import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from starlette.testclient import TestClient

from maintai_ingestion.app import create_app
from maintai_ingestion.auth import LocalFixtureVerifier

from ..fakes import FakePublisher, Probe


def test_no_response_before_ack_and_health_remains_responsive(edge, payload):
    app, pub, _ = edge
    pub.barrier = threading.Event()
    with TestClient(app) as c, ThreadPoolExecutor() as pool:
        future = pool.submit(c.post, "/api/v1/sensor-readings", json=payload)
        try:
            assert pub.started.wait(2)
            assert not future.done()
            assert c.get("/health/live").status_code == 200
        finally:
            pub.barrier.set()
        assert future.result(timeout=2).status_code == 202
    assert pub.closed == 1


def test_invalid_json_waits_for_rejected_ack(edge):
    app, pub, _ = edge
    pub.barrier = threading.Event()
    with TestClient(app) as c, ThreadPoolExecutor() as pool:
        f = pool.submit(
            c.post,
            "/api/v1/sensor-readings",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        try:
            assert pub.started.wait(2) and not f.done()
        finally:
            pub.barrier.set()
        assert f.result(timeout=2).status_code == 400


def test_admission_capacity_returns_429_and_slot_recovers(settings, payload):
    s = replace(settings, max_inflight=1)
    pub = FakePublisher(s.kafka)
    pub.barrier = threading.Event()
    app = create_app(
        s,
        publisher_factory=lambda _: pub,
        verifier=LocalFixtureVerifier("s", frozenset({"M-001"})),
        probe=Probe(),
    )
    with TestClient(app) as c, ThreadPoolExecutor() as pool:
        f = pool.submit(c.post, "/api/v1/sensor-readings", json=payload)
        try:
            assert pub.started.wait(2)
            r = c.post("/api/v1/sensor-readings", json=payload)
            assert r.status_code == 429 and r.headers["retry-after"] == "1"
        finally:
            pub.barrier.set()
        assert f.result(timeout=2).status_code == 202
        assert c.post("/api/v1/sensor-readings", json=payload).status_code == 202


def test_import_factory_has_no_producer_side_effect(settings):
    made = []

    def factory(s):
        made.append(s)
        return FakePublisher(s)

    app = create_app(
        settings,
        publisher_factory=factory,
        verifier=LocalFixtureVerifier("s", frozenset({"M-001"})),
        probe=Probe(),
    )
    assert made == []
    with TestClient(app) as c:
        assert len(made) == 1 and c.get("/health/live").status_code == 200


def test_cancelled_worker_drains_before_releasing_resources():
    from maintai_ingestion.runtime import run_blocking

    async def scenario():
        entered = threading.Event()
        release = threading.Event()

        def work():
            entered.set()
            assert release.wait(3)
            return "done"

        task = asyncio.create_task(run_blocking(work))
        while not entered.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done()
        release.set()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("Cancellation must propagate after drain")

    asyncio.run(scenario())


def test_authentication_work_is_bounded_by_admission(settings, payload):
    entered = threading.Event()
    release = threading.Event()

    class SlowVerifier:
        def verify(self, header):
            entered.set()
            assert release.wait(3)
            return LocalFixtureVerifier("s", frozenset({"M-001"})).verify(header)

        def check(self):
            return True

    s = replace(settings, max_inflight=1)
    pub = FakePublisher(s.kafka)
    app = create_app(s, publisher_factory=lambda _: pub, verifier=SlowVerifier(), probe=Probe())
    with TestClient(app) as c, ThreadPoolExecutor() as pool:
        first = pool.submit(c.post, "/api/v1/sensor-readings", json=payload)
        try:
            assert entered.wait(1)
            second = pool.submit(c.post, "/api/v1/sensor-readings", json=payload)
            r = second.result(timeout=0.5)
            assert r.status_code == 429
        finally:
            release.set()
        assert first.result(timeout=2).status_code == 202


def test_route_drains_outstanding_ack_before_single_publisher_close(edge, payload):
    import httpx

    app, pub, _ = edge
    pub.barrier = threading.Event()

    async def scenario():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://fixture"
            ) as client:
                request = asyncio.create_task(client.post("/api/v1/sensor-readings", json=payload))
                while not pub.started.is_set():
                    await asyncio.sleep(0.001)
                closing = asyncio.create_task(app.state.runtime.close())
                await asyncio.sleep(0.02)
                assert not closing.done() and pub.closed == 0
                assert (
                    await client.post("/api/v1/sensor-readings", json=payload)
                ).status_code == 503
                pub.barrier.set()
                assert (await request).status_code == 202
                await closing
                assert pub.closed == 1
        assert pub.closed == 1

    try:
        asyncio.run(scenario())
    finally:
        pub.barrier.set()
