"""Database snapshots freeze committed outcomes and bind cursor query/actor."""

from datetime import UTC, datetime, timedelta

import pytest
from processing.shared.history_meta.persistence import Metadata, metadata_schema
from processing.shared.history_meta.security import Actor, CursorKeys, HistoryError
from processing.shared.history_meta.snapshots import History
from processing.worker.tests.test_worker_transaction import Providers, handler, source

pytestmark = pytest.mark.integration
NOW = datetime(2026, 10, 29, tzinfo=UTC)
ACTOR = Actor("tech", frozenset({"M-001"}), frozenset({"TECHNICIAN"}))
QUERY = {"from": "2026-10-28T00:00:00Z", "to": "2026-10-30T00:00:00Z"}


def create(db):
    keys = CursorKeys({"one": b"k" * 32}, active="one", clock=lambda: NOW.timestamp())
    return History(
        db.factory, Metadata(db.factory, metadata_schema(db.schema)), keys, clock=lambda: NOW
    )


def insert(db, event_id, offset=1, *, invalid=False, failed=False):
    handler(db, Providers(failure="ML_UNAVAILABLE" if failed else None))(
        *source(
            event_id=event_id, offset=offset, features={"bad\x00key": "raw"} if invalid else None
        )
    )
    with db.factory() as c:
        c.execute(
            "UPDATE processing_results SET completed_at=%s WHERE event_id=%s", (NOW, event_id)
        )


def test_pages_freeze_tied_completion_times_and_late_commits(migrated_database):
    db = migrated_database
    insert(db, "event-a", 1)
    insert(db, "event-b", 2)
    history = create(db)
    page = history.page(ACTOR, "M-001", QUERY, limit=1)
    assert page["schema_version"] == "2.0.0"
    assert page["time_basis"] == "processed_at"
    assert page["next_cursor"]
    insert(db, "late", 3)
    second = history.page(ACTOR, "M-001", {}, limit=1, cursor=page["next_cursor"])
    assert second["snapshot_id"] == page["snapshot_id"]
    assert second["next_cursor"] is None
    assert len({page["items"][0]["result_id"], second["items"][0]["result_id"]}) == 2
    assert "late" not in [
        page["items"][0]["source_event_id"],
        second["items"][0]["source_event_id"],
    ]
    assert len(history.page(ACTOR, "M-001", QUERY)["items"]) == 3


def test_cursor_rejects_changed_query_actor_and_scope(migrated_database):
    db = migrated_database
    insert(db, "a", 1)
    insert(db, "b", 2)
    history = create(db)
    token = history.page(ACTOR, "M-001", QUERY, limit=1)["next_cursor"]
    for actor, query in [
        (Actor("other", ACTOR.machines, ACTOR.roles), {}),
        (ACTOR, {"result_kind": "DATA_INVALID"}),
    ]:
        with pytest.raises(HistoryError):
            history.page(actor, "M-001", query, cursor=token)


def test_all_terminal_outcomes_keep_original_evidence_and_null_ml(migrated_database):
    db = migrated_database
    insert(db, "valid", 1)
    insert(db, "invalid", 2, invalid=True)
    insert(db, "failed", 3, failed=True)
    values = create(db).page(ACTOR, "M-001", QUERY)["items"]
    assert {v["outcome"]["kind"] for v in values} == {
        "PREDICTION_COMPLETED",
        "DATA_INVALID",
        "INFERENCE_UNAVAILABLE",
    }
    for value in values:
        assert value["processed_at"] == NOW.isoformat()
        if value["outcome"]["kind"] != "PREDICTION_COMPLETED":
            assert value["risk_snapshot"] is None


def test_snapshot_expiry_and_query_limits(migrated_database):
    history = create(migrated_database)
    with pytest.raises(HistoryError) as exc:
        history.page(ACTOR, "M-001", {"from": "2025-01-01T00:00:00Z", "to": "2026-10-30T00:00:00Z"})
    assert exc.value.status == 422
    page = history.page(ACTOR, "M-001", QUERY)
    history.clock = lambda: NOW + timedelta(minutes=11)
    with pytest.raises(HistoryError) as exc:
        history.snapshot(ACTOR, page["snapshot_id"])
    assert exc.value.status == 410


def test_empty_complete_processing_history_is_grounded(migrated_database):
    page = create(migrated_database).page(ACTOR, "M-001", QUERY)
    assert page["items"] == [] and page["coverage"] == "COMPLETE"
    assert page["tiers_used"] == ["HOT"]
    assert page["available_from"] is None
