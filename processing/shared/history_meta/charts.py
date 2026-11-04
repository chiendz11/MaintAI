"""Chart buckets and marker pages operate on one immutable history snapshot."""

from maintai_messaging.contracts import strict_json
from psycopg import sql

from processing.shared.history import aggregate_samples

from .security import PROFILE, HistoryError, digest
from .snapshots import timestamp

UNITS = {
    "air_temperature_k": "K",
    "process_temperature_k": "K",
    "rotational_speed_rpm": "rpm",
    "torque_nm": "Nm",
    "tool_wear_min": "min",
    "failure_probability": "ratio",
    "risk_score": "ratio",
}


class Charts:
    def __init__(self, history):
        self.history = history

    def get(
        self,
        actor,
        machine,
        query,
        *,
        metric,
        resolution="auto",
        max_points=1000,
        markers_cursor=None,
    ):
        if metric not in UNITS:
            raise HistoryError(422, "VALIDATION_FAILED")
        collection = "PROCESSING" if metric in {"failure_probability", "risk_score"} else "RAW"
        plot = {"metric": metric, "resolution": resolution, "max_points": max_points}
        last = -1
        if markers_cursor:
            token = self.history.cursors.decode(
                markers_cursor, actor=actor.subject, scope=actor.scope
            )
            snapshot = self.history.snapshot(actor, token.get("snapshot"))
            if (
                token.get("kind") != "markers"
                or token.get("plot") != digest(plot)
                or snapshot["collection"] != collection
                or snapshot["query"].get("machine_id") != machine
            ):
                raise HistoryError(400, "INVALID_CURSOR")
            for key, value in query.items():
                normalized = timestamp(value).isoformat() if key in {"from", "to"} else value
                if snapshot["query"].get(key) != normalized:
                    raise HistoryError(400, "INVALID_CURSOR")
            last = token["last"]
        else:
            snapshot = self.history.create_snapshot(actor, machine, query, collection=collection)

        def samples():
            for _, record in self.history.members(snapshot):
                if collection == "PROCESSING":
                    outcome = record["outcome"]
                    if outcome["kind"] == "PREDICTION_COMPLETED":
                        yield timestamp(record["processed_at"]), outcome[metric]
                else:
                    yield timestamp(record["received_at"]), record["features_raw"].get(metric)

        try:
            aggregate = aggregate_samples(
                samples(),
                start=timestamp(snapshot["query"]["from"]),
                end=timestamp(snapshot["query"]["to"]),
                resolution=resolution,
                max_points=max_points,
            )
        except ValueError:
            raise HistoryError(422, "QUERY_TOO_LARGE") from None
        markers, marker_last = [], last
        with self.history.meta.factory() as connection:
            self.history.guard_members(connection, snapshot)
            with connection.cursor(name="chart_markers") as cursor:
                cursor.execute(
                    sql.SQL(
                        "SELECT ordinal,markers FROM {} WHERE snapshot_id=%s "
                        "AND ordinal>%s ORDER BY ordinal"
                    ).format(self.history.meta.table("snapshot_members")),
                    (snapshot["snapshot_id"], last),
                )
                while rows := cursor.fetchmany(64):
                    for row in rows:
                        values = strict_json(bytes(row["markers"]))["items"]
                        if values:
                            markers.extend(values)
                            marker_last = row["ordinal"]
                        if len(markers) > 100:
                            break
                    if len(markers) > 100:
                        break
        next_cursor = None
        if len(markers) > 100:
            markers = markers[:100]
            # Each source member carries at most one deduplicated prediction marker.
            with self.history.meta.factory() as connection:
                self.history.guard_members(connection, snapshot)
                ids = [m["reference_id"] for m in markers]
                # Find the last returned ordinal without ordering opaque IDs.
                rows = connection.execute(
                    sql.SQL(
                        "SELECT ordinal,markers FROM {} WHERE snapshot_id=%s "
                        "AND ordinal>%s AND ordinal<=%s ORDER BY ordinal"
                    ).format(self.history.meta.table("snapshot_members")),
                    (snapshot["snapshot_id"], last, marker_last),
                ).fetchall()
                marker_last = max(
                    r["ordinal"]
                    for r in rows
                    if any(
                        m["reference_id"] in ids for m in strict_json(bytes(r["markers"]))["items"]
                    )
                )
            next_cursor = self.history.cursors.encode(
                {
                    "kind": "markers",
                    "actor": actor.subject,
                    "scope": actor.scope,
                    "snapshot": snapshot["snapshot_id"],
                    "plot": digest(plot),
                    "last": marker_last,
                    "exp": snapshot["expires_at"].timestamp(),
                }
            )
        return {
            "schema_version": "2.0.0",
            "profile": PROFILE,
            "snapshot_id": snapshot["snapshot_id"],
            "as_of": snapshot["as_of"].isoformat(),
            "expires_at": snapshot["expires_at"].isoformat(),
            "coverage": "COMPLETE",
            "collection": collection,
            "time_basis": "processed_at" if collection == "PROCESSING" else "received_at",
            "tiers_used": ["HOT"] if collection == "PROCESSING" else ["ARCHIVE"],
            **snapshot["coverage"],
            "catalog_generation": snapshot["catalog_generation"],
            "partition_coverage": snapshot["coverage"].get("partition_coverage", []),
            "next_cursor": None,
            "metric": metric,
            "unit": UNITS[metric],
            "resolution": aggregate.resolution,
            "buckets": aggregate.buckets,
            "markers": markers,
            "markers_next_cursor": next_cursor,
        }
