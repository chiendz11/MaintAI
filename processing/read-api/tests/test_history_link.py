"""Raw receipts only link to Processing evidence of the identical source payload."""

import pytest
from processing.shared.history_meta.link import ProcessingLink
from processing.worker.tests.test_worker_transaction import Providers, handler, source

pytestmark = pytest.mark.integration


def test_link_matches_payload_and_preserves_conflicting_receipt(migrated_database):
    event, record = source()
    handler(migrated_database, Providers())(event, record)
    link = ProcessingLink(migrated_database.factory)
    value = link(event)
    assert value["validation_status"] == "VALID"
    assert value["result_id"]
    changed, _ = source(features={"conflicting": "receipt"})
    assert changed.payload["event_id"] == event.payload["event_id"]
    assert link(changed) is None
