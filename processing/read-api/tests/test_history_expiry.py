"""Metadata expiry preserves job holds and expired idempotency descriptors."""

from datetime import timedelta

import pytest
from processing.shared.history_meta.expiry import expire_metadata
from processing.shared.history_meta.exports import Exports
from processing.shared.history_meta.security import HistoryError

from .test_history_exports import request
from .test_history_snapshots import ACTOR, NOW, QUERY, create, insert

pytestmark = pytest.mark.integration


def test_members_revalidate_snapshot_expiry_after_earlier_lookup(migrated_database):
    insert(migrated_database, "race", 1)
    history = create(migrated_database)
    page = history.page(ACTOR, "M-001", QUERY)
    snapshot = history.snapshot(ACTOR, page["snapshot_id"])
    history.clock = lambda: NOW + timedelta(minutes=11)
    expire_metadata(history.meta, now=history.clock())
    with pytest.raises(HistoryError) as error:
        list(history.members(snapshot))
    assert error.value.status == 410


def test_expiry_releases_only_unheld_members_and_never_resurrects_stream(migrated_database):
    db = migrated_database
    insert(db, "expiry", 1)
    history = create(db)
    ordinary = history.page(ACTOR, "M-001", QUERY)
    exports = Exports(history, None)
    _, stream = exports.accept(ACTOR, request(), "stream-tombstone")
    stream.close()
    history.clock = lambda: NOW + timedelta(minutes=11)
    assert expire_metadata(history.meta, now=history.clock())["snapshots_released"] == 2
    with db.factory() as connection:
        assert (
            connection.execute("SELECT count(*) AS n FROM processing_results").fetchone()["n"] == 1
        )
    with pytest.raises(HistoryError) as expired:
        exports.accept(ACTOR, request(), "stream-tombstone")
    assert expired.value.status == 410
    with pytest.raises(HistoryError):
        history.snapshot(ACTOR, ordinary["snapshot_id"])
