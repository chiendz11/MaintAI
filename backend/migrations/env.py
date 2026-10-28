"""Run only on the explicit migration connection."""

from alembic import context

connection = context.config.attributes.get("connection")
if connection is None:
    raise RuntimeError("Business migration requires an explicit owner connection")
context.configure(connection=connection, target_metadata=None)
with context.begin_transaction():
    context.run_migrations()
