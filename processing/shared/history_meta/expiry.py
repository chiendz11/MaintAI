"""Release expired metadata membership only; never delete source or S3 objects."""

from psycopg import sql


def expire_metadata(meta, *, now, limit=100):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("Metadata expiry batch must be 1..100")
    with meta.factory() as connection, connection.transaction():
        connection.execute(
            sql.SQL(
                "UPDATE {} SET holds_input=false WHERE holds_input "
                "AND status IN ('SUCCEEDED','FAILED','EXPIRED') "
                "AND (lease_until IS NULL OR lease_until<=%s)"
            ).format(meta.table("export_jobs")),
            (now,),
        )
        rows = connection.execute(
            sql.SQL(
                "SELECT s.snapshot_id FROM {} s WHERE s.expires_at<=%s AND s.row_count>0 "
                "AND NOT EXISTS(SELECT 1 FROM {} j WHERE j.snapshot_id=s.snapshot_id "
                "AND j.holds_input) "
                "ORDER BY s.expires_at,s.snapshot_id LIMIT %s FOR UPDATE OF s SKIP LOCKED"
            ).format(meta.table("snapshots"), meta.table("export_jobs")),
            (now, limit),
        ).fetchall()
        identifiers = [row["snapshot_id"] for row in rows]
        connection.execute(
            sql.SQL("DELETE FROM {} WHERE snapshot_id=ANY(%s)").format(
                meta.table("snapshot_members")
            ),
            (identifiers,),
        )
        # Preserve headers/idempotency descriptors so an old key still returns 410.
        connection.execute(
            sql.SQL("UPDATE {} SET row_count=0 WHERE snapshot_id=ANY(%s)").format(
                meta.table("snapshots")
            ),
            (identifiers,),
        )
    return {"snapshots_released": len(identifiers)}
