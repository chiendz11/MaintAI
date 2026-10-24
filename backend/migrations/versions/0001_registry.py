"""Create the registry, with no automatically provisioned machines."""

from alembic import op

revision = "business_0001_registry"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE machines (
        machine_id TEXT PRIMARY KEY CHECK (length(machine_id)>0),
        display_name TEXT NOT NULL CHECK (length(display_name)>0),
        state TEXT NOT NULL DEFAULT 'UNKNOWN'
          CHECK (state IN ('NORMAL','WATCH','HIGH','CRITICAL','INVALID','UNKNOWN')),
        last_seen_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )""")


def downgrade():
    raise RuntimeError("Registry downgrade requires a reviewed data migration")
