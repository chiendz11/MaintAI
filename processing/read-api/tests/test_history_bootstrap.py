"""A new M6 broker must be sufficient for the installed Storage/browser harness."""

import os

import pytest
from confluent_kafka.admin import AdminClient
from processing.tests.storage_provider import StorageProvider

pytestmark = pytest.mark.integration


def test_storage_harness_provisions_both_raw_topics_before_starting():
    if os.environ.get("MAINTAI_M6_INTEGRATION") != "1":
        pytest.skip("M6 integration services must be explicitly enabled")
    bootstrap = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:40092")
    # Reproduce the browser harness: existing M3 bootstrap creates only its three topics.
    import importlib
    from uuid import uuid4

    from maintai_messaging.config import KafkaSettings

    kafka = KafkaSettings(
        local_mode=True,
        security_protocol="PLAINTEXT",
        bootstrap_servers=bootstrap,
        group_prefix="m3-flow-" + uuid4().hex + "-",
    )
    importlib.import_module(
        "backend.business-consumer.tests.test_kafka_runtime"
    ).prepare_new_test_groups(
        kafka,
        include_processing=True,
    )
    provider = StorageProvider(bootstrap)
    try:
        metadata = AdminClient(provider.settings.kafka.client_config()).list_topics(timeout=5)
        for name in ("sensor.readings", "sensor.rejected"):
            assert name in metadata.topics, f"Storage harness did not provision {name}"
            assert len(metadata.topics[name].partitions) == 12
            assert all(p.leader >= 0 for p in metadata.topics[name].partitions.values())
        assert provider.publish()
    finally:
        provider.close()
