"""Real SQL: machine scope/state filters precede count/page and reads share a snapshot."""

import json
from pathlib import Path

import pytest
from backend.shared.persistence.projections import ProjectionRepository
from backend.shared.persistence.registry import RegistryRepository
from backend.shared.projections.views import MachineViewUnavailable
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from .test_projections import deliver, event, setup

pytestmark = pytest.mark.integration


def alerts_fixture(connection, machines):
    connection.execute("SELECT 1")
    return {machine: (["alert-1"] if machine == "M-001" else []) for machine in machines}


def validate(name, value):
    directory = Path(__file__).resolve().parents[3] / "contracts/v1.3/schemas"
    registry = Registry().with_resources(
        [
            (p.as_uri(), Resource.from_contents(json.loads(p.read_text())))
            for p in directory.glob("*.schema.json")
        ]
    )
    Draft202012Validator(
        {"$ref": (directory / "business.schema.json").as_uri() + "#/$defs/" + name},
        registry=registry,
    ).validate(value)


def test_machine_reads_scope_state_count_pagination_and_native_schema(business_database):
    db = business_database
    projector = setup(db)
    RegistryRepository(db.factory).provision(
        [{"machine_id": machine, "display_name": machine} for machine in ("M-002", "M-003")]
    )
    deliver(db, projector, event("sensor-reading"))
    deliver(db, projector, event("completed"))
    deliver(db, projector, event("completed", 3, machine_id="M-003", event_id="e-3"), 3)
    repository = ProjectionRepository(db.factory, alert_reader=alerts_fixture)
    page = repository.fleet({"M-001", "M-002"}, state=None, limit=1, offset=1)
    assert page["total"] == 2
    assert [item["machine_id"] for item in page["items"]] == ["M-002"]
    validate("MachineList", page)
    page = repository.fleet({"M-001", "M-002"}, state="HIGH", limit=50, offset=0)
    assert page["total"] == 1
    assert page["items"][0]["latest_risk_score"] == 0.5
    assert page["items"][0]["active_alert_count"] == 1
    validate("MachineList", page)
    detail = repository.detail("M-001")
    validate("MachineDetail", detail)
    assert detail["latest_sensor"]["event_id"] == "e-1"
    assert detail["latest_prediction"]["failure_probability"] == 0.8
    assert detail["active_alerts"] == ["alert-1"]
    assert repository.fleet({"M-001"}, state=None, limit=50, offset=100) == {
        "items": [],
        "total": 1,
    }
    assert repository.fleet({"absent"}, state=None, limit=50, offset=0) == {"items": [], "total": 0}
    assert repository.detail("absent") is None
    assert repository.fleet({"M-001"}, state=None, limit=50, offset=2**80) == {
        "items": [],
        "total": 1,
    }


def test_missing_alert_source_never_becomes_fake_empty_alerts(business_database):
    db = business_database
    setup(db)
    repository = ProjectionRepository(db.factory)
    with pytest.raises(MachineViewUnavailable):
        repository.detail("M-001")
    with pytest.raises(MachineViewUnavailable):
        repository.fleet({"M-001"}, state=None, limit=50, offset=0)


def test_terminal_views_preserve_historical_prediction_only_in_live(business_database):
    db = business_database
    projector = setup(db)
    repository = ProjectionRepository(db.factory, alert_reader=alerts_fixture)
    deliver(db, projector, event("completed", 1))
    deliver(db, projector, event("sensor-reading", 2, features={"bad\x00key": "raw\x00value"}))
    deliver(db, projector, event("invalid", 2))
    detail = repository.detail("M-001")
    validate("MachineDetail", detail)
    assert detail["state"] == "INVALID"
    assert detail["latest_sensor"]["features"] == {"bad\x00key": "raw\x00value"}
    assert detail["latest_prediction"] is None
    assert repository.live("M-001")["latest_valid_prediction"]["sequence_number"] == 1
    page = repository.fleet({"M-001"}, state="INVALID", limit=50, offset=0)
    validate("MachineList", page)
    assert page["items"][0]["latest_risk_score"] is None
    deliver(db, projector, event("unavailable", 3))
    detail = repository.detail("M-001")
    validate("MachineDetail", detail)
    assert detail["state"] == "UNKNOWN"
    assert detail["latest_prediction"] is None
    assert repository.live("M-001")["latest_valid_prediction"]["sequence_number"] == 1


def test_alert_read_and_projection_read_use_same_database_snapshot(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("completed", 1))

    def concurrent_alert_reader(connection, machines):
        deliver(db, projector, event("invalid", 2))
        status = connection.execute("SELECT snapshot FROM latest_processing").fetchone()
        assert json.loads(bytes(status["snapshot"]))["processing_status"] == "COMPLETED"
        return {machine: [] for machine in machines}

    repository = ProjectionRepository(db.factory, alert_reader=concurrent_alert_reader)
    assert repository.detail("M-001")["state"] == "HIGH"
    assert repository.live("M-001")["latest_processing"]["processing_status"] == "INVALID"


def test_native_risk_serves_fleet_without_probability_recomputation(business_database):
    db = business_database
    projector = setup(db)
    deliver(db, projector, event("completed"))
    repository = ProjectionRepository(db.factory, alert_reader=alerts_fixture)
    validate("MachineDetail", repository.detail("M-001"))
    value = repository.fleet({"M-001"}, state=None, limit=50, offset=0)
    validate("MachineList", value)
    assert value["items"][0]["latest_risk_score"] == 0.5
    assert value["items"][0]["state"] == "HIGH"
