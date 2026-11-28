"""Unit tests for Business Redis configuration, Lua monotonic hashes, and SSE Redis Pub/Sub."""

import asyncio
import json
import time
from types import SimpleNamespace

import pytest
from api.app import create_app
from api.config import Settings as APISettings
from backend.shared.notifications import machine_key
from backend.shared.redis_bus import BusinessRedisBus, RedisSettings, validate_redis_url
from business_sse.hub import NotificationHub, SSESettings
from fastapi.testclient import TestClient


class _InMemorySyncRedis:
    """In-memory Redis double supporting HMGET pipeline, Pub/Sub, Streams, and Lua update."""

    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttls: dict[str, int] = {}
        self.published: list[tuple[str, str]] = []
        self.pipeline_batches = 0

    def ping(self):
        return True

    def register_script(self, _script):
        def call(keys, args):
            key = keys[0]
            ttl = int(args[0])
            as_of = args[1]
            h = self.hashes.setdefault(key, {})

            def seq_cmp(a: str, b: str) -> int:
                if len(a) != len(b):
                    return 1 if len(a) > len(b) else -1
                if a == b:
                    return 0
                return 1 if a > b else -1

            updated = 0
            if args[2] == "1":
                new_seq = args[3]
                cur_seq = h.get("telemetry_seq")
                if cur_seq is None or seq_cmp(new_seq, cur_seq) >= 0:
                    h["telemetry"] = args[4]
                    h["telemetry_seq"] = new_seq
                    updated = 1
            if args[5] == "1":
                new_seq, new_status = args[6], args[7]
                cur_seq = h.get("processing_seq")
                cur_status = h.get("processing_status")
                if (
                    cur_seq is None
                    or seq_cmp(new_seq, cur_seq) > 0
                    or (
                        seq_cmp(new_seq, cur_seq) == 0
                        and cur_status == "PENDING"
                        and new_status != "PENDING"
                    )
                ):
                    h["latest_processing"] = args[8]
                    h["processing_seq"] = new_seq
                    h["processing_status"] = new_status
                    updated = 1
            if args[9] == "1":
                new_seq = args[10]
                cur_seq = h.get("prediction_seq")
                if cur_seq is None or seq_cmp(new_seq, cur_seq) > 0:
                    h["latest_valid_prediction"] = args[11]
                    h["prediction_seq"] = new_seq
                    updated = 1
            if updated:
                h["as_of"] = as_of
                self.ttls[key] = ttl
            return updated

        return call

    def pipeline(self, transaction=False):
        outer = self

        class _Pipe:
            def __init__(self):
                self.ops = []

            def hmget(self, key, *fields):
                self.ops.append((key, fields))

            def execute(self):
                outer.pipeline_batches += 1
                results = []
                for key, fields in self.ops:
                    h = outer.hashes.get(key, {})
                    results.append([h[f].encode("utf-8") if f in h else None for f in fields])
                return results

        return _Pipe()

    def publish(self, channel, message):
        self.published.append((channel, message))
        return 1

    def close(self):
        return None


class _FakeAsyncPubSub:
    def __init__(self, messages):
        self._messages = list(messages)
        self.subscribed = []

    async def subscribe(self, *channels):
        self.subscribed.extend(channels)

    async def get_message(self, *, ignore_subscribe_messages=True, timeout=1.0):
        if self._messages:
            return self._messages.pop(0)
        await asyncio.sleep(0.01)
        return None

    async def aclose(self):
        return None


class _FakeAsyncRedis:
    def __init__(self, messages):
        self._pubsub = _FakeAsyncPubSub(messages)

    async def ping(self):
        return True

    def pubsub(self, ignore_subscribe_messages=True):
        return self._pubsub

    async def aclose(self):
        return None


def _base_api_env():
    return {
        "BUSINESS_DATABASE_URL": "postgresql://u:p@127.0.0.1:5432/business_db",
        "BUSINESS_OIDC_ISSUER": "http://127.0.0.1:8080/realms/maintai",
        "BUSINESS_OIDC_AUDIENCE": "maintai-business-local",
        "BUSINESS_OIDC_JWKS_URL": "http://127.0.0.1:8080/realms/maintai/certs",
        "BUSINESS_ROLES_CLAIM": "roles",
        "BUSINESS_MACHINES_CLAIM": "machine_ids",
        "BUSINESS_PROFILE": "local-test",
    }


def test_redis_url_validation_and_settings_opt_in():
    assert RedisSettings.from_env({}, local=True) is None
    assert validate_redis_url("redis://redis:6379/0", local=True) == "redis://redis:6379/0"
    assert validate_redis_url("redis://127.0.0.1:6379/0", local=True) == "redis://127.0.0.1:6379/0"
    with pytest.raises(ValueError, match="loopback"):
        validate_redis_url("redis://cache.prod.example.com:6379/0", local=True)
    with pytest.raises(ValueError, match="rediss://"):
        validate_redis_url("redis://cache.prod.example.com:6379/0", local=False)
    assert (
        validate_redis_url("rediss://cache.prod.example.com:6379/0", local=False)
        == "rediss://cache.prod.example.com:6379/0"
    )

    cfg = APISettings.from_env(
        {
            **_base_api_env(),
            "BUSINESS_REDIS_URL": "redis://redis:6379/0",
            "BUSINESS_REDIS_PASSWORD": "super-secret-value",
        }
    )
    assert cfg.redis is not None
    assert "super-secret-value" not in repr(cfg.redis)
    assert "redis://redis:6379/0" not in repr(cfg.redis)


def test_redis_hash_enforces_monotonic_bigint_sequence_and_pipeline_read():
    fake = _InMemorySyncRedis()
    bus = BusinessRedisBus(client=fake)
    big = 2**80 + 10

    def snap(seq, status="PENDING"):
        return {
            "latest_telemetry": {
                "sequence_number": seq,
                "snapshot": json.dumps(
                    {
                        "event_id": f"e-{seq}",
                        "stream_epoch": "ep-1",
                        "sequence_number": seq,
                        "server_accepted_at": "2026-11-28T00:00:00Z",
                        "received_at": "2026-11-28T00:00:00Z",
                        "features_raw": {"torque_nm": 40.0},
                        "validation_status": "UNVERIFIED",
                    }
                ).encode("utf-8"),
            },
            "latest_processing": {
                "sequence_number": seq,
                "snapshot": json.dumps(
                    {
                        "source_event_id": f"e-{seq}",
                        "stream_epoch": "ep-1",
                        "sequence_number": seq,
                        "processing_status": status,
                        "validation_status": None if status == "PENDING" else "VALID",
                        "updated_at": "2026-11-28T00:00:01Z",
                        "failure_code": None,
                    }
                ).encode("utf-8"),
            },
        }

    assert bus.update_live("M-001", snap(big), {"latest_telemetry", "latest_processing"}) == 1
    assert bus.update_live("M-001", snap(big - 5), {"latest_telemetry", "latest_processing"}) == 0
    assert (
        bus.update_live(
            "M-001",
            snap(big, status="COMPLETED"),
            {"latest_processing"},
        )
        == 1
    )
    assert bus.update_live("M-001", snap(big, status="PENDING"), {"latest_processing"}) == 0

    batch = bus.read_live_many(["M-001", "M-MISS"])
    assert fake.pipeline_batches == 1
    assert batch["M-001"] is not None
    assert batch["M-001"]["telemetry"]["sequence_number"] == big
    assert batch["M-001"]["latest_processing"]["processing_status"] == "COMPLETED"
    assert batch["M-MISS"] is None


def test_hub_redis_pubsub_coalescing_fallback_and_non_coalesced_alerts():
    fake_sync = _InMemorySyncRedis()
    bus = BusinessRedisBus(client=fake_sync)
    telemetry_m1 = {
        "event_id": "e-1",
        "stream_epoch": "ep",
        "sequence_number": 5,
        "server_accepted_at": "2026-11-28T00:00:00Z",
        "received_at": "2026-11-28T00:00:00Z",
        "features_raw": {"torque_nm": 42.0},
        "validation_status": "VALID",
    }
    pred_m1 = {
        "source_event_id": "e-1",
        "stream_epoch": "ep",
        "sequence_number": 5,
        "source_server_accepted_at": "2026-11-28T00:00:00Z",
        "prediction_id": "pred-1",
        "prediction": 0,
        "failure_probability": 0.12,
        "model_version": "m-v0",
        "preprocessor_version": "p-v0",
        "dataset_version": "d-v0",
        "policy_version": "pol-v0",
        "completed_at": "2026-11-28T00:00:01Z",
        "machine_state_at_prediction": "NORMAL",
    }
    bus.update_live(
        "M-001",
        {
            "latest_telemetry": {
                "sequence_number": 5,
                "snapshot": json.dumps(telemetry_m1).encode("utf-8"),
            },
            "latest_valid_prediction": {
                "sequence_number": 5,
                "snapshot": json.dumps(pred_m1).encode("utf-8"),
            },
        },
        {"latest_telemetry", "latest_valid_prediction"},
    )

    class _FallbackRepo:
        def __init__(self):
            self.live_calls = []

        def live(self, machine):
            self.live_calls.append(machine)
            return {
                "machine_id": machine,
                "telemetry": {**telemetry_m1, "event_id": f"pg-{machine}"},
                "latest_processing": None,
                "latest_valid_prediction": None,
                "as_of": "2026-11-28T00:00:02Z",
            }

        def live_many(self, machines):
            return {m: self.live(m) for m in machines}

    class _AlertRepo:
        def notification(self, identifier, machine):
            return {
                "schema_version": "1.0.0",
                "type": "alert.created",
                "machine_id": machine,
                "occurred_at": "2026-11-28T00:00:03Z",
                "data": {
                    "alert_id": f"alt-{identifier[:4]}",
                    "machine_id": machine,
                    "severity": "CRITICAL",
                    "status": "OPEN",
                    "updated_at": "2026-11-28T00:00:03Z",
                },
            }

    a1, a2 = "a" * 32, "b" * 32
    messages = [
        {
            "channel": b"maintai_business_live_v1",
            "data": json.dumps({"version": 1, "machine_key": machine_key("M-001")}).encode(),
        },
        {
            "channel": b"maintai_business_live_v1",
            "data": json.dumps({"version": 1, "machine_key": machine_key("M-001")}).encode(),
        },
        {
            "channel": b"maintai_business_live_v1",
            "data": json.dumps({"version": 1, "machine_key": machine_key("M-002")}).encode(),
        },
        {
            "channel": b"maintai_business_alert_v1",
            "data": json.dumps(
                {"version": 1, "machine_key": machine_key("M-001"), "notification_id": a1}
            ).encode(),
        },
        {
            "channel": b"maintai_business_alert_v1",
            "data": json.dumps(
                {"version": 1, "machine_key": machine_key("M-001"), "notification_id": a2}
            ).encode(),
        },
    ]

    async def run():
        repo = _FallbackRepo()
        hub = NotificationHub(
            repo,
            dsn="unused",
            settings=SSESettings(coalesce_seconds=0.04),
            alert_repository=_AlertRepo(),
            redis_settings=RedisSettings(url="redis://redis:6379/0"),
            redis_bus=bus,
            redis_async_factory=lambda: _FakeAsyncRedis(messages),
        )
        await hub.start()
        for _ in range(30):
            if hub.ready:
                break
            await asyncio.sleep(0.01)
        assert hub.ready
        sub = hub.subscribe(frozenset({"M-001", "M-002"}), expires_at=time.time() + 60)
        # Re-seed hints cleanly for subscriber after subscription exists.
        hub.invalidate(machine_key("M-001"))
        hub.invalidate(machine_key("M-001"))
        hub.invalidate(machine_key("M-002"))
        hub.invalidate_alert(
            json.dumps({"version": 1, "machine_key": machine_key("M-001"), "notification_id": a1})
        )
        hub.invalidate_alert(
            json.dumps({"version": 1, "machine_key": machine_key("M-001"), "notification_id": a2})
        )
        await asyncio.sleep(0.1)

        frames = []
        while True:
            frame = await hub.next_frame(sub, timeout=0.02)
            if frame is None or frame == b": heartbeat\n\n":
                break
            frames.append(frame)
        await hub.stop()

        events = [json.loads(f.split(b"data: ", 1)[1]) for f in frames]
        types_for_m1 = [e["type"] for e in events if e["machine_id"] == "M-001"]
        assert types_for_m1.count("telemetry.received") == 1
        assert types_for_m1.count("prediction.completed") == 1
        assert types_for_m1.count("alert.created") == 2
        assert repo.live_calls == ["M-002"]

    asyncio.run(run())


def test_health_ready_includes_redis_check_only_when_configured():
    repo = SimpleNamespace(check=lambda: True, get=lambda m: None)
    verifier = SimpleNamespace(check=lambda: True)
    redis_hub = NotificationHub(
        repo,
        dsn="unused",
        redis_settings=RedisSettings(url="redis://redis:6379/0"),
    )
    app = create_app(repo, verifier, notifications=redis_hub)
    client = TestClient(app)

    degraded = client.get("/health/ready")
    assert degraded.status_code == 503
    assert degraded.json()["checks"]["redis"] is False

    redis_hub.ready = True
    healthy = client.get("/health/ready")
    assert healthy.status_code == 200
    assert healthy.json()["checks"]["redis"] is True

    pg_hub = NotificationHub(repo, dsn="unused")
    pg_hub.ready = True
    pg_client = TestClient(create_app(repo, verifier, notifications=pg_hub))
    pg_resp = pg_client.get("/health/ready")
    assert pg_resp.status_code == 200
    assert "redis" not in pg_resp.json()["checks"]
