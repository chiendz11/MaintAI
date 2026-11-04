"""Bounded read-only retention planning; M6 has no source deletion operation."""

from psycopg import sql

from .snapshots import timestamp


def dry_run(history, *, before, limit=1000):
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Dry-run limit must be 1..1000")
    cutoff, now = timestamp(before), history.clock()
    with history.source_factory() as connection:
        rows = connection.execute(
            "SELECT result_id,completed_at FROM processing_results WHERE completed_at<%s "
            "ORDER BY completed_at,result_id LIMIT %s",
            (cutoff, limit + 1),
        ).fetchall()
    identifiers = [row["result_id"] for row in rows[:limit]]
    pins = {}
    with history.meta.factory() as connection:
        for row in connection.execute(
            sql.SQL(
                "SELECT m.entry_id,bool_or(s.expires_at>%s) AS active_snapshot,"
                "bool_or(EXISTS(SELECT 1 FROM {} j WHERE j.snapshot_id=s.snapshot_id "
                "AND j.holds_input)) AS export_hold FROM {} m JOIN {} s USING(snapshot_id) "
                "WHERE s.collection='PROCESSING' AND m.entry_id=ANY(%s) GROUP BY m.entry_id"
            ).format(
                history.meta.table("export_jobs"),
                history.meta.table("snapshot_members"),
                history.meta.table("snapshots"),
            ),
            (now, identifiers),
        ).fetchall():
            pins[row["entry_id"]] = row
    candidates = []
    for row in rows[:limit]:
        pin = pins.get(row["result_id"], {})
        reasons = ["PROCESSED_ARCHIVE_DISABLED"]
        if pin.get("active_snapshot"):
            reasons.append("ACTIVE_SNAPSHOT")
        if pin.get("export_hold"):
            reasons.append("EXPORT_INPUT_HOLD")
        candidates.append(
            {
                "result_id": row["result_id"],
                "processed_at": row["completed_at"].isoformat(),
                "eligible_for_deletion": False,
                "blocked_by": reasons,
            }
        )
    return {
        "retention_enabled": False,
        "cleaner_dry_run": True,
        "as_of": now.isoformat(),
        "before": cutoff.isoformat(),
        "candidates": candidates,
        "truncated": len(rows) > limit,
        "raw_deletion": "DISABLED",
        "artifact_deletion": "DISABLED",
    }
