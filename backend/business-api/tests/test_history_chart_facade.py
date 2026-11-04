"""Composite charts freeze late alerts and preserve unknown source clocks as null."""

import asyncio
import importlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from api.auth import Identity
from api.history_charts import CompositeCharts
from backend.shared.persistence.history_markers import MarkerSnapshots
from backend.shared.persistence.migration import upgrade
from backend.shared.persistence.registry import RegistryRepository
from business_processing_client.history_schemas import HistorySchemas

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("with_old_alerts", [False, True])
def test_alert_without_exact_axis_time_remains_in_marker_list(registry_database, with_old_alerts):
    db = registry_database
    upgrade(db.dsn, schema=db.schema)
    RegistryRepository(db.factory).provision([{"machine_id": "M-001", "display_name": "One"}])
    helper = importlib.import_module("backend.business-consumer.tests.test_alert_lifecycle")
    helper.deliver_alert(db)
    root = Path(__file__).resolve().parents[3]
    base = json.loads((root / "contracts/v1.3/examples/m6/processing-chart.json").read_text())
    now = datetime.now(UTC)
    base["as_of"] = now.isoformat()
    base["expires_at"] = (now.replace(year=now.year + 1)).isoformat()

    class Provider:
        async def request(self, *args, **kwargs):
            return SimpleNamespace(value=base, status=200)

    repository = MarkerSnapshots(db.factory, {"one": b"k" * 32}, active="one")
    charts = CompositeCharts(
        Provider(), HistorySchemas(root / "contracts/v1.3/schemas"), repository
    )
    request = SimpleNamespace(
        query_params={
            "metric": "risk_score",
            "from": "2020-01-01T00:00:00Z",
            "to": "2030-01-01T00:00:00Z",
        },
        state=SimpleNamespace(correlation_id="cor-one"),
    )
    # Actual selected range is bounded and contains the fixture alert creation.
    with db.factory() as connection:
        created = connection.execute("SELECT created_at FROM alerts").fetchone()["created_at"]
    from datetime import timedelta

    if with_old_alerts:
        old = created - timedelta(days=365)
        with db.factory() as connection:
            connection.execute("SET LOCAL statement_timeout=15000")
            connection.execute(
                "INSERT INTO alerts(alert_id,machine_id,severity,status,risk_score,model_version,"
                "policy_version,source_event_id,prediction_id,created_at,updated_at,"
                "latest_signal_id,latest_metadata) "
                "SELECT 'old-'||g,'M-001','HIGH','OPEN',0.8,'fixture','fixture','old-event-'||g,"
                "'old-pred-'||g,%s,%s,'old-signal-'||g,convert_to('{}','UTF8') "
                "FROM generate_series(1,100001) g",
                (old, old),
            )
    request.query_params.update(
        {
            "from": (created - timedelta(days=1)).isoformat(),
            "to": (created + timedelta(days=1)).isoformat(),
        }
    )
    actor = Identity("tech", frozenset({"M-001"}))
    page = asyncio.run(charts.get(actor, "M-001", request))
    alert = next(marker for marker in page["markers"] if marker["kind"] == "ALERT")
    assert alert["position_at"] is None
    assert alert["processed_at"] is None
    assert alert["source_server_accepted_at"] is not None
