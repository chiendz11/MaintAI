"""Real owner SQL for immutable evidence, dedupe, leasing and fenced completions."""

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from .test_alert_lifecycle import deliver_alert, repository
from .test_projections import setup

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def prepare(db):
    from backend.shared.copilot.repository import BriefRepository

    setup(db)
    deliver_alert(db)
    alert = repository(db).list({"M-001"})["items"][0]["alert_id"]
    return BriefRepository(db.factory), alert


def result(alert):
    value = json.loads(
        (ROOT / "contracts/v1.3/examples/http/backend/copilot-grounded.json").read_text()
    )["brief"]
    return {**value, "alert_id": alert}


def test_durable_enqueue_concurrent_keys_and_original_evidence(business_database):
    from backend.shared.alerts.domain import AlertError

    db = business_database
    jobs, alert = prepare(db)

    def submit(key):
        return jobs.enqueue(alert, {"M-001"}, "tech", key, "MAINTENANCE_BRIEF")

    with ThreadPoolExecutor(2) as pool:
        values = list(pool.map(submit, ["a", "b"]))
    assert values[0]["job_id"] == values[1]["job_id"]
    assert submit("a") == values[0]
    with pytest.raises(AlertError) as error:
        jobs.enqueue(alert, {"M-001"}, "tech", "a", "ALERT_EXPLANATION")
    assert error.value.status == 409
    claimed = jobs.claim("worker")
    assert claimed["input"]["alert"]["alert_id"] == alert
    assert claimed["input"]["prediction"]["model_version"]
    assert jobs.complete(claimed, result(alert))
    assert jobs.facade(alert, {"M-001"})["copilot_status"] == "AVAILABLE"
    assert jobs.job(values[0]["job_id"], {"M-001"})["status"] == "SUCCEEDED"
    assert not jobs.complete(claimed, result(alert))


def test_reclaim_fences_old_worker_and_preserves_non_grounded_result(business_database):
    db = business_database
    jobs, alert = prepare(db)
    accepted = jobs.enqueue(alert, {"M-001"}, "tech", "a", "MAINTENANCE_BRIEF")
    first = jobs.claim("old")
    with db.factory() as connection:
        connection.execute(
            "UPDATE brief_jobs SET lease_until=clock_timestamp()-interval '1 second'"
        )
    second = jobs.claim("new")
    assert second["fencing_token"] > first["fencing_token"]
    assert not jobs.complete(first, result(alert))
    value = copy.deepcopy(result(alert))
    value.update(grounded_status="INSUFFICIENT_EVIDENCE", checklist=[], citations=[])
    assert jobs.complete(second, value)
    assert jobs.job(accepted["job_id"], {"M-001"})["copilot_status"] == "INSUFFICIENT_EVIDENCE"
    assert jobs.facade(alert, {"M-001"})["copilot_status"] == "AVAILABLE"


def test_scope_and_terminal_provider_failure(business_database):
    from backend.shared.alerts.domain import AlertError

    jobs, alert = prepare(business_database)
    job = jobs.enqueue(alert, {"M-001"}, "tech", "a", "MAINTENANCE_BRIEF")
    with pytest.raises(AlertError):
        jobs.job(job["job_id"], set())
    claim = jobs.claim("worker")
    jobs.fail(claim, "RAG_CONTRACT_INVALID", retryable=False)
    assert jobs.facade(alert, {"M-001"})["copilot_status"] == "UNAVAILABLE"
    assert jobs.job(job["job_id"], {"M-001"})["status"] == "FAILED"


def test_retry_budget_deadline_and_heartbeat(business_database):
    jobs, alert = prepare(business_database)
    receipt = jobs.enqueue(alert, {"M-001"}, "tech", "retry", "MAINTENANCE_BRIEF")
    for number in range(3):
        claim = jobs.claim("worker")
        assert claim["attempts"] == number + 1
        assert jobs.heartbeat(claim)
        assert jobs.fail(claim, "RAG_UNAVAILABLE", retryable=True, delay=0)
    assert jobs.job(receipt["job_id"], {"M-001"})["status"] == "FAILED"
    receipt = jobs.enqueue(alert, {"M-001"}, "tech", "expired", "ALERT_EXPLANATION")
    with business_database.factory() as connection:
        connection.execute(
            "UPDATE brief_jobs SET deadline=clock_timestamp()-interval '1 second' WHERE job_id=%s",
            (receipt["job_id"],),
        )
    assert jobs.claim("worker") is None
    assert jobs.job(receipt["job_id"], {"M-001"})["status"] == "EXPIRED"


def test_old_completion_cannot_replace_latest_facade(business_database):
    jobs, alert = prepare(business_database)
    jobs.enqueue(alert, {"M-001"}, "tech", "old", "MAINTENANCE_BRIEF")
    old = jobs.claim("worker")
    jobs.enqueue(alert, {"M-001"}, "tech", "new", "ALERT_EXPLANATION")
    new = jobs.claim("worker")
    fresh = {**result(alert), "brief_id": "fresh"}
    assert jobs.complete(new, fresh)
    assert jobs.complete(old, result(alert))
    assert jobs.facade(alert, {"M-001"})["brief"]["brief_id"] == "fresh"


def test_corrupt_original_evidence_cannot_enqueue(business_database):
    from backend.shared.alerts.domain import AlertError

    jobs, alert = prepare(business_database)
    with business_database.factory() as connection:
        connection.execute("UPDATE alert_evidence_snapshots SET evidence=%s", (b"bad",))
    with pytest.raises(AlertError) as error:
        jobs.enqueue(alert, {"M-001"}, "tech", "bad", "MAINTENANCE_BRIEF")
    assert error.value.status == 503


def test_corrupt_job_input_never_reaches_provider(business_database):
    jobs, alert = prepare(business_database)
    receipt = jobs.enqueue(alert, {"M-001"}, "tech", "tampered", "MAINTENANCE_BRIEF")
    with business_database.factory() as connection:
        connection.execute("UPDATE brief_jobs SET input=%s", (b"{}",))
    assert jobs.claim("worker") is None
    assert jobs.job(receipt["job_id"], {"M-001"})["error_code"] == "BRIEF_INPUT_INVALID"


def test_crashed_attempt_budget_is_failed_not_deadline_expired(business_database):
    jobs, alert = prepare(business_database)
    receipt = jobs.enqueue(alert, {"M-001"}, "tech", "crash", "MAINTENANCE_BRIEF")
    for _ in range(3):
        assert jobs.claim("worker") is not None
        with business_database.factory() as connection:
            connection.execute(
                "UPDATE brief_jobs SET lease_until=clock_timestamp()-interval '1 second'"
            )
    assert jobs.claim("worker") is None
    state = jobs.job(receipt["job_id"], {"M-001"})
    assert state["status"] == "FAILED"
    assert state["error_code"] == "BRIEF_ATTEMPTS_EXHAUSTED"


def test_m7_committed_notifications_reach_two_api_listeners(business_database):
    import asyncio
    import time

    from business_sse.hub import NotificationHub
    from psycopg.conninfo import make_conninfo

    jobs, alert = prepare(business_database)

    async def run():
        hubs = [
            NotificationHub(
                None,
                dsn=make_conninfo(
                    business_database.dsn, options="-csearch_path=" + business_database.schema
                ),
                m7_factory=business_database.factory,
            )
            for _ in range(2)
        ]
        for hub in hubs:
            await hub.start()
        try:
            deadline = time.monotonic() + 5
            while not all(hub.ready for hub in hubs):
                assert time.monotonic() < deadline
                await asyncio.sleep(0.02)
            subscriptions = [hub.subscribe({"M-001"}, expires_at=time.time() + 60) for hub in hubs]
            jobs.enqueue(alert, {"M-001"}, "tech", "sse", "MAINTENANCE_BRIEF")
            claim = jobs.claim("worker")
            assert jobs.complete(claim, result(alert))
            for hub, subscription in zip(hubs, subscriptions, strict=True):
                while True:
                    frame = await asyncio.wait_for(hub.next_frame(subscription, timeout=1), 5)
                    if b"brief.ready" in frame:
                        break
                    assert time.monotonic() < deadline
        finally:
            for hub in hubs:
                await hub.stop()

    asyncio.run(run())
