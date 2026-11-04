"""M6 HTTP must require both the service token and authenticated delegation."""

import asyncio
from pathlib import Path

import httpx
import pytest
from processing.shared.history_meta.charts import Charts
from processing.shared.history_meta.exports import Exports
from processing.shared.history_meta.schemas import Schemas
from processing_read_api.app import create_app
from processing_read_api.history_api import HistoryRuntime

from .test_history_security import delegation, sign  # noqa: F401
from .test_history_snapshots import QUERY, create, insert
from .test_status_api import StatusFixture, auth

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]


def test_http_requires_profile_and_delegation_and_serves_csv(migrated_database, delegation):  # noqa: F811
    insert(migrated_database, "api-event", 1)
    verifier, token = auth()
    delegated, key, claims, service = delegation
    history = create(migrated_database)
    runtime = HistoryRuntime(
        history,
        delegated,
        Exports(history, None),
        Charts(history),
        Schemas(ROOT / "contracts/v1.3/schemas"),
    )
    app = create_app(StatusFixture(), verifier, history_runtime=runtime)
    headers = {
        "Authorization": token(sub=service.subject),
        "X-MaintAI-History-Profile": "M6-HISTORY-01/1.0.0",
        "X-MaintAI-History-Delegation": sign(key, claims),
    }

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            path = "/api/v1/internal/machines/M-001/predictions"
            assert (
                await client.get(path, headers={"Authorization": headers["Authorization"]})
            ).status_code == 400
            assert (
                await client.get(
                    path,
                    headers={
                        k: v for k, v in headers.items() if k != "X-MaintAI-History-Delegation"
                    },
                )
            ).status_code == 401
            result = await client.get(path, headers=headers, params=QUERY)
            assert result.status_code == 200, result.text
            assert result.json()["items"][0]["source_event_id"] == "api-event"
            assert (
                app.state.metrics.registry.get_sample_value(
                    "maintai_history_queries_total", {"source": "HOT", "outcome": "SUCCESS"}
                )
                == 1
            )
            response = await client.post(
                "/api/v1/internal/history-exports",
                headers={**headers, "Idempotency-Key": "api-csv"},
                json={
                    "profile": "M6-HISTORY-01/1.0.0",
                    "machine_ids": ["M-001"],
                    **QUERY,
                    "resource": "PROCESSING",
                    "format": "CSV",
                },
            )
            assert response.status_code == 200, response.text
            assert response.headers["content-type"].startswith("text/csv")
            assert "api-event" in response.text
            invalid = await client.post(
                "/api/v1/internal/history-exports", headers=headers, json={"unexpected": True}
            )
            assert invalid.status_code == 422
            assert invalid.json()["error"]["code"] == "VALIDATION_FAILED"
            wrong_type = await client.post(
                "/api/v1/internal/history-exports", headers=headers, content=b"{}"
            )
            assert wrong_type.status_code == 415
            invalid_limit = await client.get(path, headers=headers, params={"limit": 501})
            assert invalid_limit.status_code == 422
            assert invalid_limit.json()["error"]["code"] == "VALIDATION_FAILED"

    asyncio.run(run())


def test_catalog_failure_does_not_stop_export_runner():
    import threading
    from types import SimpleNamespace

    executed = threading.Event()

    class BrokenCatalog:
        def sync(self):
            raise RuntimeError("catalog unavailable")

    runtime = HistoryRuntime(
        SimpleNamespace(catalog=BrokenCatalog()),
        None,
        SimpleNamespace(run_once=executed.set),
        None,
        None,
    )
    runtime.start()
    try:
        assert executed.wait(2), "Processing jobs must run independently of raw catalog failures"
    finally:
        runtime.close()
