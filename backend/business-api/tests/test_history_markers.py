"""Business marker pagination freezes membership across replicas and late alerts."""

from datetime import UTC, datetime, timedelta

import pytest
from api.auth import Identity, RegistryFailure
from backend.shared.persistence.history_markers import MarkerSnapshots
from backend.shared.persistence.migration import upgrade

pytestmark = pytest.mark.integration


def test_composite_marker_pages_are_stable_and_actor_bound(registry_database):
    db = registry_database
    upgrade(db.dsn, schema=db.schema)
    now = datetime.now(UTC)
    actor = Identity("tech", frozenset({"M-001"}))
    repository = MarkerSnapshots(db.factory, {"one": b"k" * 32}, active="one", clock=lambda: now)
    markers = [{"kind": "ALERT", "reference_id": str(i), "position_at": None} for i in range(101)]
    page = repository.create(
        actor,
        "M-001",
        {"metric": "risk_score"},
        {
            "as_of": now.isoformat(),
            "expires_at": (now + timedelta(minutes=10)).isoformat(),
            "snapshot_id": "processing-snapshot",
            "markers": [],
        },
        markers,
    )
    assert len(page["markers"]) == 100
    replica = MarkerSnapshots(db.factory, {"one": b"k" * 32}, active="one", clock=lambda: now)
    second = replica.page(actor, "M-001", {"metric": "risk_score"}, page["markers_next_cursor"])
    assert len(second["markers"]) == 1
    assert second["snapshot_id"] == page["snapshot_id"]
    with pytest.raises(RegistryFailure):
        replica.page(
            Identity("another", actor.machines),
            "M-001",
            {"metric": "risk_score"},
            page["markers_next_cursor"],
        )
    replica.clock = lambda: now + timedelta(minutes=11)
    with pytest.raises(RegistryFailure) as expired:
        replica.page(actor, "M-001", {"metric": "risk_score"}, page["markers_next_cursor"])
    assert expired.value.status == 410
