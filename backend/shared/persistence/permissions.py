"""Explicit grants for pre-provisioned Business roles in the selected owner schema."""

from psycopg import sql


def grant_business_permissions(connection, *, reader_role, consumer_role):
    roles = (reader_role, consumer_role)
    if any(not isinstance(role, str) or not role.strip() for role in roles) or len(set(roles)) != 2:
        raise ValueError("Distinct existing Business reader and consumer roles are required")
    rows = connection.execute(
        "SELECT rolname FROM pg_roles WHERE rolname=ANY(%s)",
        (list(roles),),
    ).fetchall()
    if len(rows) != 2:
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
