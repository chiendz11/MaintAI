"""Business enables only implemented roles and binds the actual SDK group identity."""

import pytest
from maintai_messaging.config import KafkaSettings


def test_factory_enables_three_serial_business_roles_with_matching_inbox_groups():
    from business_consumer.main import make_handlers

    settings = KafkaSettings(local_mode=True, security_protocol="PLAINTEXT", group_prefix="test-")
    handlers = make_handlers(lambda: None, settings)
    assert set(handlers) == {"business-live", "business-prediction", "business-alert"}
    assert handlers["business-live"].consumer_group == "test-business-live-v1"
    assert handlers["business-prediction"].consumer_group == "test-business-prediction-v1"
    assert handlers["business-live"].topic == "sensor.readings"
    assert handlers["business-prediction"].topic == "inference.results"


def test_consumer_settings_need_own_db_and_kafka_without_oidc_or_ml():
    from business_consumer.config import Settings

    env = {
        "BUSINESS_DATABASE_URL": "postgresql://test@127.0.0.1/test",
        "BUSINESS_CONSUMER_PROFILE": "local-test",
        "BUSINESS_CONSUMER_HOST": "127.0.0.1",
        "KAFKA_LOCAL_MODE": "true",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:39092",
    }
    settings = Settings.from_env(env)
    assert settings.host == "127.0.0.1"
    assert settings.kafka.local_mode
    assert "test@" not in repr(settings)
    with pytest.raises(ValueError):
        Settings.from_env({**env, "BUSINESS_CONSUMER_HOST": "0.0.0.0"})
    with pytest.raises(ValueError):
        Settings.from_env({**env, "BUSINESS_DATABASE_URL": ""})


def test_consumer_probes_require_fresh_dependencies_and_not_draining():
    import asyncio
    import threading

    import httpx
    from business_consumer.observability import Metrics

    async def run():
        stop = threading.Event()
        metrics = Metrics(stop=stop)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=metrics.app), base_url="http://test"
        ) as http:
            assert (await http.get("/health/live")).status_code == 200
            assert (await http.get("/health/ready")).status_code == 503
            metrics.dependencies(database=True, kafka=True)
            assert (await http.get("/health/ready")).status_code == 200
            stop.set()
            assert (await http.get("/health/ready")).status_code == 503
            assert (await http.get("/metrics")).status_code == 200

    asyncio.run(run())


def test_test_group_bootstrap_discards_partial_offsets_after_transient_leader_error(monkeypatch):
    from types import SimpleNamespace

    from confluent_kafka import KafkaError, KafkaException

    from . import test_kafka_runtime as bootstrap

    class Future:
        def __init__(self, value=None, error=None):
            self.value, self.error = value, error

        def result(self, timeout):
            if self.error:
                raise self.error
            return self.value

    class Admin:
        def __init__(self):
            self.captures, self.requests = 0, []

        def create_topics(self, topics):
            return {topic.topic: Future() for topic in topics}

        def list_topics(self, timeout):
            partitions = {p: SimpleNamespace(leader=1, error=None) for p in range(12)}
            return SimpleNamespace(
                topics={
                    name: SimpleNamespace(error=None, partitions=partitions)
                    for name in ("sensor.readings", "inference.results", "alert.signals")
                }
            )

        def list_offsets(self, offsets):
            self.captures += 1
            return {
                tp: Future(
                    SimpleNamespace(offset=42 if self.captures == 1 else 84),
                    KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION))
                    if self.captures == 1 and tp.partition == 6
                    else None,
                )
                for tp in offsets
            }

        def alter_consumer_group_offsets(self, requests):
            self.requests.extend(requests)
            return {
                r.group_id: Future(SimpleNamespace(topic_partitions=r.topic_partitions))
                for r in requests
            }

    admin = Admin()
    monkeypatch.setattr(bootstrap, "AdminClient", lambda _: admin)
    settings = KafkaSettings(
        local_mode=True, security_protocol="PLAINTEXT", group_prefix="m3-" + "a" * 32 + "-"
    )
    bootstrap.prepare_new_test_groups(settings)
    assert len(admin.requests) == 3
    assert {tp.offset for request in admin.requests for tp in request.topic_partitions} == {84}
