"""M6 retention reports pins and candidates without enabling source deletion."""

from datetime import timedelta

import pytest
from processing.shared.history_meta.retention import dry_run

from .test_history_snapshots import ACTOR, NOW, QUERY, create, insert

pytestmark = pytest.mark.integration


def test_retention_dry_run_reports_snapshot_pin_without_mutation(migrated_database):
    db = migrated_database
    insert(db, "retained", 1)
    history = create(db)
    page = history.page(ACTOR, "M-001", QUERY)
    report = dry_run(history, before=NOW + timedelta(days=1))
    assert report["retention_enabled"] is False
    assert report["cleaner_dry_run"] is True
    assert report["candidates"][0]["result_id"] == page["items"][0]["result_id"]
    assert "ACTIVE_SNAPSHOT" in report["candidates"][0]["blocked_by"]
    assert "PROCESSED_ARCHIVE_DISABLED" in report["candidates"][0]["blocked_by"]
    assert history.page(ACTOR, "M-001", QUERY)["items"] == page["items"]
