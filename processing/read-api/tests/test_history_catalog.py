"""Provider fixtures test raw catalog reads, never claim a Storage publisher."""

import json
from pathlib import Path

import pytest
from processing.shared.history_meta.catalog import Catalog
from processing.shared.history_meta.schemas import Schemas
from processing.shared.history_meta.security import HistoryError

ROOT = Path(__file__).resolve().parents[3]


def test_release_schema_rejects_duplicate_keys_or_invalid_catalog_values():
    schemas = Schemas(ROOT / "contracts/v1.3/schemas")
    entry = json.loads((ROOT / "contracts/v1.3/examples/m6/raw-entry.json").read_text())
    schemas.validate("Entry", entry)
    entry["raw_object_ref"]["version_id"] = ""
    with pytest.raises(HistoryError):
        schemas.validate("Entry", entry)


def test_catalog_is_explicitly_unavailable_until_verified_sync():
    catalog = Catalog(None, None, Schemas(ROOT / "contracts/v1.3/schemas"))
    with pytest.raises(HistoryError) as error:
        catalog.coverage({"machine_id": "M-001"})
    assert error.value.status == 503


def test_coverage_requires_actual_twelve_partition_ids():
    from contextlib import contextmanager
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from maintai_messaging.contracts import canonical_json

    manifest = {
        "published_at": datetime.now(UTC).isoformat(),
        "partitions": [
            {"topic": "sensor.readings", "partition": p, "status": "COMPLETE", "gaps": []}
            for p in [*range(11), 12]
        ],
    }

    @contextmanager
    def factory():
        yield SimpleNamespace(
            execute=lambda *_: SimpleNamespace(
                fetchone=lambda: {"generation_id": "fixture", "manifest": canonical_json(manifest)}
            )
        )

    catalog = Catalog(SimpleNamespace(factory=factory, table=lambda _: "fixture"), object(), None)
    with pytest.raises(HistoryError):
        catalog.coverage({})
