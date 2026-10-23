import json
from pathlib import Path

import pytest

from maintai_ingestion.auth import LocalFixtureVerifier
from maintai_ingestion.config import AppSettings

from .fakes import FakePublisher, Probe

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def payload():
    return json.loads((ROOT / "contracts/kafka/examples/sensor-request.json").read_text())


@pytest.fixture
def settings():
    return AppSettings.from_env(
        {
            "INGESTION_ENV": "test",
            "INGESTION_HOST": "127.0.0.1",
            "AUTH_MODE": "local-fixture",
            "LOCAL_SOURCE_SUBJECT": "verified-source",
            "LOCAL_ALLOWED_MACHINE_IDS": "M-001",
            "BUSINESS_PUBLIC_ORIGIN": "http://127.0.0.1:8003",
            "KAFKA_LOCAL_MODE": "true",
            "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            "INGESTION_PROBE_INTERVAL_SECONDS": ".1",
        }
    )


@pytest.fixture
def edge(settings):
    from maintai_ingestion.app import create_app

    publisher = FakePublisher(settings.kafka)
    probe = Probe()
    app = create_app(
        settings,
        publisher_factory=lambda _: publisher,
        verifier=LocalFixtureVerifier("verified-source", frozenset({"M-001"})),
        probe=probe,
    )
    return app, publisher, probe
