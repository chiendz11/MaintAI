"""Freeze Processing prediction markers together with Business Alert evidence."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta

from business_processing_client.history import opaque
from maintai_messaging.contracts import strict_json

from .auth import RegistryFailure


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() is None:
            raise ValueError("Timezone required")
        return parsed.astimezone(UTC)
    except (TypeError, ValueError):
        raise RegistryFailure(422, "VALIDATION_FAILED") from None


class CompositeCharts:
    def __init__(self, client, schemas, repository):
        self.client, self.schemas, self.repository = client, schemas, repository

    async def get(self, actor, machine, request):
        query = dict(request.query_params)
        cursor = query.pop("markers_cursor", None)
        for name in ("from", "to"):
            if name in query:
                query[name] = timestamp(query[name]).isoformat()
        if not query.get("metric"):
            raise RegistryFailure(422, "VALIDATION_FAILED")
        if cursor:
            return await asyncio.to_thread(self.repository.page, actor, machine, query, cursor)
        end = timestamp(query["to"]) if "to" in query else datetime.now(UTC)
        start = timestamp(query["from"]) if "from" in query else end - timedelta(hours=1)
        if not start < end or end - start > timedelta(days=31):
            raise RegistryFailure(422, "QUERY_TOO_LARGE")
        query.update({"from": start.isoformat(), "to": end.isoformat()})
        try:
            async with asyncio.timeout(12):
                base, markers = None, []
                internal_cursor = None
                while True:
                    params = dict(query)
                    if internal_cursor is not None:
                        params["markers_cursor"] = internal_cursor
                    response = await self.client.request(
                        actor,
                        "GET",
                        "machines/" + opaque(machine) + "/timeseries",
                        query=params,
                        correlation_id=request.state.correlation_id,
                    )
                    page = self.schemas.validate("TimeSeriesPage", response.value)
                    if base is None:
                        base = page
                    elif any(
                        page[key] != base[key]
                        for key in ("snapshot_id", "as_of", "expires_at", "buckets")
                    ):
                        raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
                    markers.extend(page["markers"])
                    if len(markers) > 100000:
                        raise RegistryFailure(422, "QUERY_TOO_LARGE")
                    next_cursor = page["markers_next_cursor"]
                    if next_cursor is None:
                        break
                    if next_cursor == internal_cursor or not page["markers"]:
                        raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
                    internal_cursor = next_cursor
                alerts = await asyncio.to_thread(self.alerts, machine, base, markers, start, end)
                # Stable insertion order from frozen source pages and Alert creation order.
                seen = set()
                combined = []
                for marker in markers + alerts:
                    key = (marker["kind"], marker["reference_id"])
                    if key not in seen:
                        combined.append(marker)
                        seen.add(key)
                value = await asyncio.to_thread(
                    self.repository.create, actor, machine, query, base, combined
                )
                return self.schemas.validate("TimeSeriesPage", value)
        except TimeoutError:
            raise RegistryFailure(504, "DEPENDENCY_TIMEOUT") from None

    def alerts(self, machine, base, predictions, start, end):
        lookup = {
            marker["reference_id"]: marker
            for marker in predictions
            if marker["kind"] == "PREDICTION"
        }
        with self.repository.factory() as connection:
            rows = connection.execute(
                "SELECT a.alert_id,a.source_event_id,a.prediction_id,a.created_at,"
                "e.evidence,e.evidence_sha256 FROM alerts a "
                "LEFT JOIN alert_evidence_snapshots e USING(alert_id) "
                "WHERE a.machine_id=%s AND a.created_at<=%s "
                "AND (a.prediction_id=ANY(%s) OR (a.created_at>=%s AND a.created_at<%s)) "
                "ORDER BY a.created_at,a.alert_id LIMIT 100001",
                (machine, base["as_of"], list(lookup), start, end),
            ).fetchall()
        if len(rows) > 100000:
            raise RegistryFailure(422, "QUERY_TOO_LARGE")
        markers = []
        for row in rows:
            if row["evidence"] is None:
                raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
            raw = bytes(row["evidence"])
            if hashlib.sha256(raw).hexdigest() != row["evidence_sha256"]:
                raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
            evidence = strict_json(raw)
            if (
                evidence["machine_id"] != machine
                or evidence["source_event_id"] != row["source_event_id"]
                or evidence["outcome"]["prediction_id"] != row["prediction_id"]
            ):
                raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
            prediction = lookup.get(row["prediction_id"])
            if prediction is not None and prediction["source_event_id"] != row["source_event_id"]:
                raise RegistryFailure(503, "DEPENDENCY_UNAVAILABLE")
            position = prediction["position_at"] if prediction else None
            # Unknown source time stays null; creation time selects its list entry only.
            selected_at = timestamp(position) if position else row["created_at"]
            if not start <= selected_at < end:
                continue
            markers.append(
                {
                    "kind": "ALERT",
                    "reference_id": row["alert_id"],
                    "source_event_id": row["source_event_id"],
                    "position_at": position,
                    "source_received_at": prediction["source_received_at"] if prediction else None,
                    "source_server_accepted_at": evidence["source_server_accepted_at"],
                    "processed_at": prediction["processed_at"] if prediction else None,
                }
            )
        return markers
