"""Unit tests for Redis Streams BriefRunner and post-commit consumer publishing."""

import hashlib
import json
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import redis
from backend.shared.alerts.handler import AlertHandler
from backend.shared.alerts.repository import AlertRepository
from backend.shared.copilot.runner import BriefRunner
from backend.shared.inbox.handler import ProjectionHandler
from backend.shared.notifications import machine_key
from backend.shared.projections.projector import Projector
from backend.shared.redis_bus import BusinessRedisBus
from business_consumer.config import Settings as ConsumerSettings
from business_consumer.config import WorkerSettings
from rag_client import ProviderFailure


class _InMemoryStreamRedis:
    def __init__(self):
        self.groups: set[tuple[str, str]] = set()
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.pending: dict[tuple[str, str], dict[str, tuple[str, dict[str, str]]]] = {}
        self.acked: list[str] = []
        self.published: list[tuple[str, str]] = []
        self.hashes: dict[str, dict[str, str]] = {}
        self._seq = 0

    def ping(self):
        return True

    def xgroup_create(self, name, groupname, id="0", mkstream=False):
        if mkstream:
            self.streams.setdefault(name, [])
        if (name, groupname) in self.groups:
            raise redis.ResponseError("BUSYGROUP Consumer Group name already exists")
        self.groups.add((name, groupname))
        self.pending[(name, groupname)] = {}

    def xadd(self, name, fields, maxlen=None, approximate=True):
        self._seq += 1
        entry_id = f"{self._seq}-0"
        self.streams.setdefault(name, []).append(
            (entry_id, {str(k): str(v) for k, v in fields.items()})
        )
        return entry_id.encode("utf-8")

    def xreadgroup(self, groupname, consumername, streams, count=1, block=None):
        results = []
        for stream_name in streams:
            items = self.streams.get(stream_name, [])
            if not items:
                continue
            taken = items[:count]
            self.streams[stream_name] = items[count:]
            pel = self.pending[(stream_name, groupname)]
            encoded = []
            for entry_id, fields in taken:
                pel[entry_id] = (consumername, fields)
                encoded.append(
                    (
                        entry_id.encode("utf-8"),
                        {k.encode("utf-8"): v.encode("utf-8") for k, v in fields.items()},
                    )
                )
            results.append((stream_name.encode("utf-8"), encoded))
        return results

    def xautoclaim(self, name, groupname, consumername, min_idle_time, start_id="0-0", count=4):
        pel = self.pending.get((name, groupname), {})
        claimed = []
        for entry_id, (_old_consumer, fields) in list(pel.items())[:count]:
            pel[entry_id] = (consumername, fields)
            claimed.append(
                (
                    entry_id.encode("utf-8"),
                    {k.encode("utf-8"): v.encode("utf-8") for k, v in fields.items()},
                )
            )
        return [b"0-0", claimed, []]

    def xack(self, name, groupname, *ids):
        pel = self.pending.get((name, groupname), {})
        ack_count = 0
        for entry_id in ids:
            if pel.pop(entry_id, None) is not None:
                ack_count += 1
            self.acked.append(entry_id)
        return ack_count

    def publish(self, channel, message):
        self.published.append((channel, message))
        return 1

    def register_script(self, _script):
        def call(keys, args):
            key = keys[0]
            h = self.hashes.setdefault(key, {})
            if args[2] == "1":
                h["telemetry"] = args[4]
                h["telemetry_seq"] = args[3]
            if args[5] == "1":
                h["latest_processing"] = args[8]
                h["processing_seq"] = args[6]
                h["processing_status"] = args[7]
            if args[9] == "1":
                h["latest_valid_prediction"] = args[11]
                h["prediction_seq"] = args[10]
            h["as_of"] = args[1]
            return 1

        return call

    def close(self):
        return None


class _FakeBriefRepo:
    def __init__(self, redis_bus=None):
        self.redis_bus = redis_bus
        self.jobs = {}
        self.fail_calls = []
        self.complete_calls = []

    def add_job(self, job_id):
        self.jobs[job_id] = {
            "job_id": job_id,
            "alert_id": f"alt_{job_id}",
            "machine_id": "M-001",
            "status": "QUEUED",
            "attempts": 0,
            "max_attempts": 3,
            "deadline": datetime.now(UTC) + timedelta(minutes=5),
            "next_attempt_at": datetime.now(UTC) - timedelta(seconds=1),
            "input": {"intent": "MAINTENANCE_BRIEF"},
        }
        if self.redis_bus is not None:
            self.redis_bus.enqueue_brief_job(job_id)

    def claim_by_id(self, owner, job_id):
        row = self.jobs.get(job_id)
        if row is None or row["status"] in {"SUCCEEDED", "FAILED", "EXPIRED"}:
            return None, "DONE"
        if row["next_attempt_at"] > datetime.now(UTC):
            return None, "DEFER_BACKOFF"
        row["status"] = "RUNNING"
        row["attempts"] += 1
        row["lease_owner"] = owner
        row["fencing_token"] = row["attempts"]
        return dict(row), "CLAIMED"

    def claim(self, owner):
        for row in self.jobs.values():
            if row["status"] == "QUEUED" and row["next_attempt_at"] <= datetime.now(UTC):
                row["status"] = "RUNNING"
                row["attempts"] += 1
                row["lease_owner"] = owner
                row["fencing_token"] = row["attempts"]
                return dict(row)
        return None

    def heartbeat(self, claim):
        return True

    def complete(self, claim, output):
        row = self.jobs[claim["job_id"]]
        row["status"] = "SUCCEEDED"
        row["output"] = output
        self.complete_calls.append(claim["job_id"])
        if self.redis_bus is not None:
            self.redis_bus.publish_m7(
                "maintai_business_m7_v1", json.dumps({"job_id": claim["job_id"]})
            )
        return True

    def fail(self, claim, code, *, retryable=False, delay=1):
        row = self.jobs[claim["job_id"]]
        row["status"] = (
            "QUEUED" if retryable and row["attempts"] < row["max_attempts"] else "FAILED"
        )
        row["error_code"] = code
        row["next_attempt_at"] = datetime.now(UTC) + timedelta(seconds=delay)
        self.fail_calls.append((claim["job_id"], code, retryable))
        return True


def test_consumer_and_worker_settings_redis_validation():
    base_consumer = {
        "BUSINESS_DATABASE_URL": "postgresql://u:p@127.0.0.1:5432/business_db",
        "BUSINESS_CONSUMER_PROFILE": "local-test",
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:9092",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_LOCAL_MODE": "true",
    }
    assert ConsumerSettings.from_env(base_consumer).redis is None
    assert (
        ConsumerSettings.from_env(
            {**base_consumer, "BUSINESS_REDIS_URL": "redis://redis:6379/0"}
        ).redis
        is not None
    )

    base_worker = {
        "BUSINESS_DATABASE_URL": "postgresql://u:p@127.0.0.1:5432/business_db",
        "BUSINESS_WORKER_PROFILE": "local-test",
    }
    assert WorkerSettings.from_env(base_worker).redis is None
    assert (
        WorkerSettings.from_env({**base_worker, "BUSINESS_REDIS_URL": "redis://redis:6379/0"}).redis
        is not None
    )
    with pytest.raises(ValueError, match="rediss://"):
        WorkerSettings.from_env(
            {
                "BUSINESS_DATABASE_URL": "postgresql://u:p@db.internal:5432/business_db",
                "BUSINESS_WORKER_PROFILE": "shared",
                "BUSINESS_REDIS_URL": "redis://valkey.internal:6379/0",
            }
        )


def test_brief_runner_redis_streams_complete_retry_autoclaim_and_pg_fallback():
    stream_redis = _InMemoryStreamRedis()
    bus = BusinessRedisBus(client=stream_redis)
    repo = _FakeBriefRepo(redis_bus=bus)

    class _Client:
        def __init__(self):
            self.calls = []

        def generate(self, payload, *, job_id, timeout):
            self.calls.append(job_id)
            if job_id == "job-retry" and len([c for c in self.calls if c == "job-retry"]) == 1:
                raise ProviderFailure("LLM_RATE_LIMITED", retryable=True)
            return {"brief_id": f"brief-{job_id}", "grounded_status": "GROUNDED"}

        def close(self):
            return None

    client = _Client()
    runner = BriefRunner(repo, client, threading.Event(), redis_bus=bus)

    # Normal stream path: XADD -> XREADGROUP -> PG complete -> publish -> XACK
    repo.add_job("job-1")
    assert runner.once_stream("w-1") is True
    assert repo.complete_calls == ["job-1"]
    assert len(stream_redis.acked) == 1
    assert stream_redis.published and "job-1" in stream_redis.published[0][1]

    # Retryable failure on job-retry does NOT block job-2 behind it in the stream
    repo.add_job("job-retry")
    repo.add_job("job-2")
    assert runner.once_stream("w-1") is True
    assert repo.fail_calls == [("job-retry", "LLM_RATE_LIMITED", True)]
    assert runner.once_stream("w-1") is True
    assert "job-2" in repo.complete_calls

    # Crash recovery via XAUTOCLAIM when an orphaned PEL entry exists
    bus.ensure_brief_group()
    repo.add_job("job-crash")
    stream_redis.xreadgroup("maintai-brief-workers", "dead-worker", {"maintai:m7:brief_jobs": ">"})
    assert len(stream_redis.pending[("maintai:m7:brief_jobs", "maintai-brief-workers")]) == 1
    assert runner.once_stream("w-2", reclaim=True) is True
    assert "job-crash" in repo.complete_calls
    assert stream_redis.pending[("maintai:m7:brief_jobs", "maintai-brief-workers")] == {}

    # Fallback Postgres polling when redis_bus is disabled
    pg_repo = _FakeBriefRepo(redis_bus=None)
    pg_repo.add_job("job-pg")
    pg_runner = BriefRunner(pg_repo, client, threading.Event(), redis_bus=None)
    assert pg_runner.once("pg-owner") is True
    assert pg_repo.complete_calls == ["job-pg"]


def test_projection_and_alert_handlers_publish_to_redis_only_after_commit(monkeypatch):
    import backend.shared.alerts.handler as alert_handler_mod
    import backend.shared.inbox.handler as inbox_handler_mod

    fake_redis = _InMemoryStreamRedis()
    bus = BusinessRedisBus(client=fake_redis)

    class _TxFake:
        def __init__(self, factory, group, callback):
            self.callback = callback

        def __call__(self, event, record):
            self.callback("conn", event, SimpleNamespace(), None)
            return True

    monkeypatch.setattr(inbox_handler_mod, "TransactionalHandler", _TxFake)
    monkeypatch.setattr(alert_handler_mod, "TransactionalHandler", _TxFake)

    snapshot = json.dumps(
        {
            "event_id": "evt-1",
            "stream_epoch": "ep",
            "sequence_number": 9,
            "server_accepted_at": "2026-11-28T00:00:00Z",
            "received_at": "2026-11-28T00:00:00Z",
            "features_raw": {"torque_nm": 45.0},
            "validation_status": "UNVERIFIED",
        }
    ).encode("utf-8")

    class _Projector(Projector):
        def apply(self, connection, event, record, outbox, *, pg_notify=True):
            assert pg_notify is False
            return SimpleNamespace(
                rows={"latest_telemetry": {"sequence_number": 9, "snapshot": snapshot}},
                changed=frozenset({"latest_telemetry"}),
                telemetry_visible=True,
            )

    handler = ProjectionHandler(
        lambda: None,
        "group-live",
        projector=_Projector(),
        topic="sensor.readings",
        redis_bus=bus,
        notification_channel="maintai_business_live_v1",
    )
    event = SimpleNamespace(topic="sensor.readings", key="M-001", wire_bytes=b"{}")
    record = SimpleNamespace(topic="sensor.readings", partition=0, offset=1)
    assert handler(event, record) is True
    assert "maintai:business:live:M-001" in fake_redis.hashes
    assert fake_redis.published[0][0] == "maintai_business_live_v1"
    assert json.loads(fake_redis.published[0][1])["machine_key"] == machine_key("M-001")

    fake_redis.published.clear()

    class _Repo(AlertRepository):
        def create_or_dedupe(self, connection, event, evidence, *, out_hint=None):
            hint = json.dumps(
                {
                    "version": 1,
                    "machine_key": machine_key("M-001"),
                    "notification_id": hashlib.sha256(b"n").hexdigest()[:32],
                }
            )
            out_hint.append(hint)
            return "CREATED", "CRITICAL", "2026-11-28T00:00:00Z"

    alert_handler = AlertHandler(
        lambda: None,
        "group-alert",
        SimpleNamespace(fetch=lambda payload: {"result_id": "res-1"}),
        repository=_Repo(lambda: None, redis_bus=bus),
    )
    alert_event = SimpleNamespace(
        topic="alert.signals", payload={"machine_id": "M-001"}, wire_bytes=b"{}"
    )
    alert_record = SimpleNamespace(topic="alert.signals", partition=0, offset=2)
    assert alert_handler(alert_event, alert_record) is True
    assert fake_redis.published[0][0] == "maintai_business_alert_v1"
