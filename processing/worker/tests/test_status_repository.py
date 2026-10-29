"""Owner status reads see terminal Worker commits and hide in-flight SQL."""

import pytest
from processing.shared.persistence.repository import ProcessingRepository
from processing.worker.tests.test_worker_transaction import Providers, handler, source

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "provider,features,status,validation,failure",
    [
        (Providers(), None, "COMPLETED", "VALID", None),
        (Providers(), {"bad": "raw"}, "INVALID", "INVALID", None),
        (Providers(failure="ML_UNAVAILABLE"), None, "FAILED", "VALID", "ML_UNAVAILABLE"),
    ],
)
def test_status_reads_real_worker_terminal_outcomes(
    migrated_database,
    provider,
    features,
    status,
    validation,
    failure,
):
    from processing.shared.persistence.status import StatusRepository

    event, record = source(features=features)
    reader = StatusRepository(migrated_database.factory)
    assert reader.get(event.message_id) is None
    handler(migrated_database, provider)(event, record)
    value = reader.get(event.message_id)
    assert value["event_id"] == event.message_id
    assert value["machine_id"] == "M-001"
    assert value["processing_status"] == status
    assert value["validation_status"] == validation
    assert value["failure_code"] == failure
    assert value["result_id"]
    assert reader.get("missing") is None


def test_uncommitted_worker_event_is_not_visible(migrated_database):
    from processing.shared.persistence.status import StatusRepository

    event, record = source()
    reader = StatusRepository(migrated_database.factory)
    with migrated_database.factory() as connection:
        ProcessingRepository().claim(
            connection, event, record, ruleset_version="fixture", ruleset_sha256="1" * 64
        )
        assert reader.get(event.message_id) is None
        connection.rollback()
    assert reader.get(event.message_id) is None
