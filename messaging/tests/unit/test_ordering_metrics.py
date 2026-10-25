from maintai_messaging.keyed import KeyedConsumerRunner
from maintai_messaging.metrics import OutboxOrderMetrics, ProcessingMetrics
from prometheus_client import CollectorRegistry, generate_latest

from ..conftest import ConsumerClient
from .test_keyed import FencedFixture, reading


def test_keyed_gauges_reset_after_assignment_release_and_histogram_has_no_id_labels(
    settings, contracts, example
):
    registry = CollectorRegistry()
    metrics = ProcessingMetrics(registry)
    runner = KeyedConsumerRunner(
        settings,
        "processing",
        FencedFixture(lambda *_: None),
        metrics=metrics,
        contracts=contracts,
        client=ConsumerClient([reading(example, "M-secret", 1, 0)]),
    )
    runner.run(max_records=1)
    text = generate_latest(registry).decode()
    assert "maintai_processing_active_keys 0.0" in text
    assert "maintai_processing_uncommitted_records 0.0" in text
    assert "maintai_processing_queue_wait_seconds_count 1.0" in text
    assert "M-secret" not in text


def test_metrics_failure_never_changes_durable_processing(settings, contracts, example):
    class Broken:
        def update(self, *_args):
            raise OSError("collector unavailable")

    runner = KeyedConsumerRunner(
        settings,
        "processing",
        FencedFixture(lambda *_: None),
        metrics=Broken(),
        contracts=contracts,
        client=ConsumerClient([reading(example, "M-A", 1, 0)]),
    )
    assert runner.run(max_records=1) == 1


def test_failed_outbox_metric_query_emits_no_fake_healthy_zero():
    registry = CollectorRegistry()

    def unavailable():
        raise OSError("DB unavailable")

    OutboxOrderMetrics(unavailable, registry)
    text = generate_latest(registry).decode()
    assert "maintai_outbox_blocked_keys{" not in text
    assert "maintai_outbox_head_blockage_age_seconds{" not in text
