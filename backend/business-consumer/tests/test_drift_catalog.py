"""Catalog publication snapshots do not leak late reports or claim worker freshness."""

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from maintai_messaging.contracts import canonical_json

from .test_projections import setup

ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.integration


class Objects:
    def __init__(self):
        self.manifests = []
        self.objects = {}

    def add(self, identifier):
        report = json.loads(
            (ROOT / "contracts/v1.3/examples/http/drift/report-stable.json").read_text()
        )
        report["report_id"] = identifier
        raw = canonical_json(report)
        manifest = {
            "schema_version": "1.0.0",
            "report_id": identifier,
            "computed_at": report["computed_at"],
            "publication_at": report["computed_at"],
            **report["reference"],
            "overall_status": report["overall_status"],
            "report_ref": {
                "bucket": "reports",
                "key": identifier,
                "version_id": "v1",
                "sha256": hashlib.sha256(raw).hexdigest(),
                "size_bytes": len(raw),
            },
        }
        self.manifests.append(manifest)
        self.objects[identifier] = raw

    def page(self, prefix, continuation=None):
        return [(v["report_id"], v) for v in copy.deepcopy(self.manifests)], None

    def read(self, ref):
        return self.objects[ref["key"]]


def prepare(db):
    from backend.shared.monitoring.catalog import DriftCatalog

    setup(db)
    store = Objects()
    return DriftCatalog(db.factory, store, b"x" * 32, model_version="model"), store


def actor(name="viewer"):
    return SimpleNamespace(subject=name, machines=frozenset({"M-001"}), roles=frozenset({"VIEWER"}))


def test_catalog_snapshots_late_publication_and_cursor_binding(business_database):
    from backend.shared.alerts.domain import AlertError

    catalog, store = prepare(business_database)
    store.add("r1")
    store.add("r2")
    catalog.sync()
    page = catalog.list(actor(), limit=1)
    assert len(page["items"]) == 1 and page["next_cursor"]
    store.add("r3")
    catalog.sync()
    second = catalog.list(actor(), limit=1, cursor=page["next_cursor"])
    assert len(second["items"]) == 1
    assert {page["items"][0]["report_id"], second["items"][0]["report_id"]} == {"r1", "r2"}
    with pytest.raises(AlertError):
        catalog.list(actor("other"), limit=1, cursor=page["next_cursor"])
    assert catalog.summary()["drift"]["worker_status"] == "NOT_AVAILABLE"
    assert catalog.summary()["drift"]["last_success_at"] is None


def test_detail_checks_bytes_and_unknown_report(business_database):
    from backend.shared.alerts.domain import AlertError

    catalog, store = prepare(business_database)
    store.add("report")
    catalog.sync()
    assert catalog.detail("report")["report_id"] == "report"
    store.objects["report"] = b"corrupt"
    with pytest.raises(AlertError) as error:
        catalog.detail("report")
    assert error.value.status == 503
    with pytest.raises(AlertError) as error:
        catalog.detail("unknown")
    assert error.value.status == 404


def test_empty_requires_successful_observation_and_conflict_retains_old(business_database):
    from backend.shared.alerts.domain import AlertError

    catalog, store = prepare(business_database)
    with pytest.raises(AlertError):
        catalog.list(actor())
    catalog.sync()
    assert catalog.list(actor())["items"] == []
    store.add("one")
    catalog.sync()
    store.manifests[0]["report_ref"]["sha256"] = "0" * 64
    with pytest.raises(AlertError):
        catalog.sync()
    page = catalog.list(actor())
    assert page["stale"] is True
    assert page["items"][0]["report_ref"]["sha256"] != "0" * 64


def test_monitoring_summary_obeys_dependency_status_dto(business_database):
    from backend.shared.copilot.schemas import valid

    catalog, store = prepare(business_database)
    store.add("report")
    catalog.sync()
    summary = catalog.summary()
    assert valid("monitoring", summary, "Summary")
    assert summary["services"]["backend"]["status"] == "AVAILABLE"


def test_expired_snapshot_metadata_is_cleaned_in_bounded_batches(business_database):
    catalog, store = prepare(business_database)
    store.add("one")
    store.add("two")
    catalog.sync()
    catalog.list(actor(), limit=1)
    with business_database.factory() as connection:
        connection.execute(
            "UPDATE drift_list_snapshots SET expires_at=clock_timestamp()-interval '1 second'"
        )
    catalog.cleanup(batch=1)
    with business_database.factory() as connection:
        assert (
            connection.execute("SELECT count(*) AS n FROM drift_list_members").fetchone()["n"] == 1
        )
    catalog.cleanup(batch=10)
    with business_database.factory() as connection:
        assert (
            connection.execute("SELECT count(*) AS n FROM drift_list_snapshots").fetchone()["n"]
            == 0
        )
        assert connection.execute("SELECT count(*) AS n FROM drift_catalog").fetchone()["n"] == 2


def test_corrupt_manifest_does_not_starve_later_publications(business_database):
    from backend.shared.alerts.domain import AlertError

    catalog, store = prepare(business_database)
    store.add("broken")
    store.add("good")
    store.objects["broken"] = b"corrupt"
    with pytest.raises(AlertError):
        catalog.sync()
    assert catalog.detail("good")["report_id"] == "good"


def test_full_scan_continuation_survives_catalog_restart(business_database):
    from backend.shared.monitoring.catalog import DriftCatalog

    catalog, store = prepare(business_database)
    store.add("one")
    store.add("two")
    calls = []

    def page(prefix, continuation=None):
        calls.append(continuation)
        if continuation is None:
            return [("one", store.manifests[0])], "page-two"
        return [("two", store.manifests[1])], None

    store.page = page
    assert not catalog.sync(full=True, max_pages=1)
    restarted = DriftCatalog(business_database.factory, store, b"x" * 32)
    assert restarted.sync(full=True, max_pages=1)
    assert calls == [None, "page-two"]
    assert restarted.detail("two")["report_id"] == "two"


def test_missing_worker_is_degraded_even_when_services_are_available(business_database):
    catalog, store = prepare(business_database)
    catalog.services = lambda: {
        name: {"status": "AVAILABLE", "message": "fixture"} for name in ("ml", "rag")
    }
    assert catalog.summary()["status"] == "DEGRADED"
