"""Explicit grants for pre-provisioned Business roles in the selected owner schema."""

from psycopg import sql


def grant_business_permissions(
    connection, *, reader_role, consumer_role, api_role=None, rebuild_role=None
):
    roles = (
        (reader_role, consumer_role) if api_role is None else (reader_role, consumer_role, api_role)
    )
    if rebuild_role is not None:
        roles = (*roles, rebuild_role)
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

    if (
        connection.execute("SELECT to_regclass('brief_jobs') AS name").fetchone()["name"]
        is not None
    ):
        read = (
            "brief_jobs",
            "brief_facades",
            "brief_idempotency",
            "brief_job_audit",
            "business_m7_notifications",
            "drift_catalog",
            "drift_catalog_state",
        )
        grant(reader_role, "SELECT", read)
        grant(consumer_role, "SELECT", read)
        grant(consumer_role, "UPDATE", ("brief_jobs", "drift_catalog_state"))
        grant(
            consumer_role,
            "INSERT",
            ("brief_job_audit", "business_m7_notifications", "drift_catalog"),
        )
        if api_role is not None:
            grant(api_role, "SELECT", read)
            grant(
                api_role,
                "INSERT",
                (
                    "brief_jobs",
                    "brief_facades",
                    "brief_idempotency",
                    "brief_job_audit",
                    "business_m7_notifications",
                ),
            )
            grant(api_role, "UPDATE (job_id)", ("brief_facades",))
            grant(api_role, "SELECT, INSERT", ("drift_list_snapshots", "drift_list_members"))

        grant(
            consumer_role, "SELECT, INSERT, UPDATE", ("drift_scan_progress", "drift_catalog_errors")
        )
        grant(
            consumer_role,
            "DELETE",
            ("drift_list_snapshots", "drift_list_members", "drift_catalog_errors"),
        )
        grant(consumer_role, "SELECT", ("drift_list_snapshots", "drift_list_members"))

    if (
        connection.execute("SELECT to_regclass('projection_control') AS name").fetchone()["name"]
        is not None
    ):
        generation_tables = tuple("projection_" + axis for axis in projections)
        for role in (reader_role, api_role):
            if role is not None:
                grant(
                    role,
                    "SELECT",
                    ("projection_control", "projection_generations", *generation_tables),
                )
        grant(
            consumer_role,
            "SELECT",
            (
                "projection_control",
                "projection_generations",
                "projection_rebuild_jobs",
                "projection_members",
            ),
        )
        grant(consumer_role, "UPDATE (building_job)", ("projection_control",))
        grant(consumer_role, "UPDATE (state)", ("projection_generations",))
        grant(
            consumer_role,
            "UPDATE (status,last_error,lease_until,journal_bytes,journal_count,updated_at)",
            ("projection_rebuild_jobs",),
        )
        grant(consumer_role, "SELECT,INSERT", ("projection_journal",))
        connection.execute(
            sql.SQL("GRANT USAGE ON SEQUENCE {}.projection_journal_journal_id_seq TO {}").format(
                sql.Identifier(schema), sql.Identifier(consumer_role)
            )
        )
        grant(
            consumer_role,
            "SELECT,INSERT,UPDATE",
            (*generation_tables, "projection_source_inbox", "projection_row_checksums"),
        )
        if rebuild_role is not None:
            grant(
                rebuild_role,
                "SELECT",
                ("machines", *projections, "maintai_kafka_inbox", "business_source_inbox"),
            )
            grant(
                rebuild_role,
                "SELECT,INSERT,UPDATE",
                (
                    "projection_control",
                    "projection_generations",
                    "projection_rebuild_jobs",
                    "projection_source_inbox",
                    "projection_row_checksums",
                    "projection_catalog_seen",
                    "projection_raw_entries",
                    *generation_tables,
                ),
            )
            grant(rebuild_role, "SELECT", ("projection_journal",))
            grant(rebuild_role, "SELECT,INSERT", ("projection_members",))
            grant(rebuild_role, "UPDATE (applied,source_ref)", ("projection_members",))
            grant(rebuild_role, "INSERT", ("business_m7_notifications",))
