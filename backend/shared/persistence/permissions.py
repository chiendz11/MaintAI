"""Explicit grants for pre-provisioned Business roles in the selected owner schema."""

from psycopg import sql


def grant_business_permissions(connection, *, reader_role, consumer_role, api_role=None):
    roles = (
        (reader_role, consumer_role) if api_role is None else (reader_role, consumer_role, api_role)
    )
    if any(not isinstance(role, str) or not role.strip() for role in roles) or len(
        set(roles)
    ) != len(roles):
        raise ValueError("Distinct existing Business reader and consumer roles are required")
    rows = connection.execute(
        "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
        (list(roles),),
    ).fetchall()
    if len(rows) != len(roles):
        raise ValueError("Provision roles separately before granting owner permissions")
    schema = connection.execute("SELECT current_schema() AS name").fetchone()["name"]
    if not schema:
        raise ValueError("Select the Business owner schema explicitly")
    for role in roles:
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)
            )
        )

    def grant(role, privileges, tables):
        for table in tables:
            connection.execute(
                sql.SQL("GRANT {} ON {}.{} TO {}").format(
                    sql.SQL(privileges),
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(role),
                )
            )

    projections = ("latest_telemetry", "latest_processing", "latest_valid_prediction")
    grant(reader_role, "SELECT", ("machines", *projections))
    grant(consumer_role, "SELECT", ("machines",))
    grant(consumer_role, "UPDATE (state,last_seen_at,updated_at)", ("machines",))
    grant(consumer_role, "SELECT, INSERT, UPDATE", (*projections, "business_source_inbox"))
    grant(
        consumer_role,
        "SELECT, INSERT",
        (
            "maintai_kafka_inbox",
            "business_projection_conflicts",
        ),
    )
    # The additive M5 tables are owned by the same schema; no cross-owner grants.
    if connection.execute("SELECT to_regclass('alerts') AS name").fetchone()["name"] is not None:
        tables = (
            "alerts",
            "alert_evidence_snapshots",
            "alert_signal_links",
            "alert_events",
            "feedback",
            "alert_notifications",
        )
        grant(reader_role, "SELECT", tables)
        grant(consumer_role, "SELECT", tables)
        grant(
            consumer_role,
            "INSERT",
            (
                "alerts",
                "alert_evidence_snapshots",
                "alert_signal_links",
                "alert_events",
                "alert_notifications",
            ),
        )
        grant(consumer_role, "UPDATE (updated_at,latest_signal_id,latest_metadata)", ("alerts",))
        if api_role is not None:
            grant(api_role, "SELECT", ("machines", *projections, *tables))
            grant(api_role, "UPDATE (updated_at)", ("machines",))
            grant(api_role, "UPDATE (status,updated_at)", ("alerts",))
            grant(api_role, "INSERT", ("alert_events", "feedback", "alert_notifications"))
    if (
        api_role is not None
        and connection.execute("SELECT to_regclass('history_marker_snapshots') AS name").fetchone()[
            "name"
        ]
        is not None
    ):
        grant(api_role, "SELECT,INSERT,UPDATE,DELETE", ("history_marker_snapshots",))
