import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import replace

import pytest
from confluent_kafka import TopicPartition
from maintai_messaging.consumer import ConsumerRunner, DurableReceipt

from maintai_storage.consumer import ObservedConsumer
from maintai_storage.health import ManagementServer
from maintai_storage.metrics import Metrics
from maintai_storage.runtime import Runtime
from maintai_storage.state import State

from .conftest import Admin, ConsumerClient, ObjectStore, provider_error


def wait_until(condition, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.01)
    pytest.fail("Fixture condition did not complete")


def test_state_age_is_held_until_broker_commit_and_is_not_kafka_backlog(settings, record):
    now = [10.0]
    state = State(settings.roles, clock=lambda: now[0])
    state.begin("storage-accepted", record)
    now[0] += 3
    assert state.oldest_age() == 3
    state.committed("storage-accepted", [TopicPartition(record.topic, 0, record.offset)])
    assert state.oldest_age() == 3
    state.committed("storage-accepted", [TopicPartition(record.topic, 0, record.offset + 1)])
    assert state.oldest_age() == 0
    state.begin("storage-rejected", record)
    state.revoked("storage-accepted")
    assert state.pending
    state.revoked("storage-rejected")
    assert not state.pending


def test_dependency_outage_changes_readiness_but_not_liveness(settings):
    state = State(settings.roles)
    for role in settings.roles:
        state.tick(role)
    for name in ("s3", "kafka"):
        state.dependency(name, True)
    assert state.snapshot(settings)["ready"]
    state.dependency("s3", False)
    assert not state.snapshot(settings)["ready"]
    assert state.snapshot(settings)["live"]


def test_stale_dependency_and_stalled_loop_are_not_reported_healthy(settings):
    now = [0.0]
    state = State(settings.roles, clock=lambda: now[0])
    for role in settings.roles:
        state.tick(role)
    state.dependency("s3", True)
    state.dependency("kafka", True)
    now[0] = settings.readiness_max_age_seconds + 1
    assert not state.snapshot(settings)["ready"] and state.snapshot(settings)["live"]
    now[0] = settings.liveness_max_age_seconds + 1
    assert not state.snapshot(settings)["live"]


def test_management_probes_are_cached_and_metrics_follow_obs01(settings):
    state = State(settings.roles)
    metrics = Metrics(state)
    server = ManagementServer(settings, state, metrics)
    server.start()
    url = f"http://127.0.0.1:{server.server.server_port}"
    try:
        with urllib.request.urlopen(url + "/health/live", timeout=2) as response:
            assert json.load(response)["service"] == "storage-consumer"
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(url + "/health/ready", timeout=2)
        assert error.value.code == 503
        metrics.batch("COMMITTED")
        metrics.write(0.5, "SUCCESS")
        with urllib.request.urlopen(url + "/metrics", timeout=2) as response:
            text = response.read().decode()
            assert "text/plain" in response.headers["Content-Type"]
            assert 'maintai_storage_batches_total{status="COMMITTED"} 1.0' in text
            assert 'le="120.0"' in text
            assert "machine_id=" not in text and "archive_kind=" not in text
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(url + "/api/history", timeout=2)
        assert error.value.code == 404
    finally:
        server.close()


def test_runtime_runs_all_three_groups_and_commits_only_after_manifests(settings, record):
    settings = replace(settings, archive_outcomes=True)
    stop = threading.Event()
    store, admin = ObjectStore(), Admin()
    clients, errors, codes = [], [], []
    lock = threading.Lock()

    def factory(config):
        role = config["client.id"].removeprefix(settings.kafka.client_id + "-")
        topic = {
            "storage-accepted": "sensor.readings",
            "storage-rejected": "sensor.rejected",
            "storage-outcomes": "inference.results",
        }[role]

        def after_commit():
            with lock:
                # Each group must have finished object + manifest before its commit callback.
                assert any(
                    f"topic={topic}/" in key and key.endswith("manifest.json")
                    for key in store.objects
                )
                if len(clients) == 3 and all(client.positions for client in clients):
                    stop.set()

        client = ConsumerClient([replace(record, topic=topic)], after_commit=after_commit)
        with lock:
            clients.append(client)
        assert config["enable.auto.commit"] is False
        assert config["enable.auto.offset.store"] is False
        assert config["allow.auto.create.topics"] is False
        return client

    runtime = Runtime(settings, s3=store, admin=admin, consumer_factory=factory)

    def run():
        try:
            codes.append(runtime.run(stop))
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        wait_until(lambda: bool(codes) or bool(errors))
    finally:
        stop.set()
        thread.join(5)
    assert not thread.is_alive() and errors == [] and codes == [0]
    assert len(store.objects) == 6 and len(clients) == 3
    assert all(client.closed and len(client.positions) == 1 for client in clients)
    assert store.closed


def test_unversioned_bucket_blocks_consumption_before_any_write(settings):
    stop = threading.Event()
    store = ObjectStore()
    store.versioning = "Suspended"
    runtime = Runtime(
        settings,
        s3=store,
        admin=Admin(),
        consumer_factory=lambda _: pytest.fail("Consumer started before preflight"),
    )
    thread = threading.Thread(target=runtime.run, args=(stop,))
    thread.start()
    try:
        wait_until(lambda: runtime.management is not None and runtime.management.thread.is_alive())
        wait_until(lambda: runtime.state.dependencies["s3"][1] is not None)
        assert not runtime.state.snapshot(settings)["ready"]
        assert runtime.state.snapshot(settings)["live"]
        assert not store.objects
    finally:
        stop.set()
        thread.join(5)
    assert not thread.is_alive()


def test_failure_stops_sibling_group_without_committing_bad_record(settings, record):
    store = ObjectStore()
    store.failure, store.remaining = provider_error("AccessDenied", 403), 99
    clients = []

    def factory(config):
        records = [record] if config["group.id"].endswith("raw-archive-v1") else []
        client = ConsumerClient(records)
        clients.append(client)
        return client

    runtime = Runtime(settings, s3=store, admin=Admin(), consumer_factory=factory)
    assert runtime.run() == 1
    assert all(client.closed and not client.positions for client in clients)
    assert not runtime.state.snapshot(settings)["ready"]


def test_rebalance_during_write_cannot_commit_lost_assignment(settings, record):
    gate, stop = threading.Event(), threading.Event()
    state = State(settings.roles)

    def revoke(client):
        client.callbacks["on_lost"](client, client.assignment())
        client.assign([])
        gate.set()
        stop.set()

    client = ConsumerClient([record], during_wait=revoke)

    def durable(_event, value):
        assert gate.wait(2)
        return DurableReceipt.for_record(value)

    ConsumerRunner(
        settings.kafka,
        "storage-accepted",
        durable,
        client=ObservedConsumer(client, "storage-accepted", state),
    ).run(stop)
    assert not client.positions and client.closed
    assert state.oldest_age() == 0


def test_missing_topic_and_s3_outage_fail_readiness_without_touching_data(settings):
    store, admin = ObjectStore(), Admin()
    admin.topics.pop("sensor.rejected")
    runtime = Runtime(settings, s3=store, admin=admin)
    assert runtime.probe() is False
    assert runtime.state.dependencies["kafka"][0] is False
    assert not store.objects
    assert runtime.state.snapshot(settings)["live"]
