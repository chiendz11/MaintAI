"""Snapshot-bound chart buckets and independently paginated prediction markers."""

import pytest
from processing.shared.history_meta.charts import Charts
from processing.shared.history_meta.security import HistoryError

from .test_history_snapshots import ACTOR, QUERY, create, insert

pytestmark = pytest.mark.integration


def test_processing_chart_uses_completion_clock_and_keeps_markers(migrated_database):
    insert(migrated_database, "one", 1)
    insert(migrated_database, "two", 2)
    charts = Charts(create(migrated_database))
    page = charts.get(ACTOR, "M-001", QUERY, metric="risk_score", resolution="1d", max_points=3)
    assert page["collection"] == "PROCESSING"
    assert page["time_basis"] == "processed_at"
    assert sum(b["count"] for b in page["buckets"]) == 2
    assert len(page["markers"]) == 2
    assert all(m["position_at"] == m["processed_at"] for m in page["markers"])


def test_numeric_raw_chart_is_unavailable_without_catalog(migrated_database):
    with pytest.raises(HistoryError) as exc:
        Charts(create(migrated_database)).get(ACTOR, "M-001", QUERY, metric="torque_nm")
    assert exc.value.status == 503
