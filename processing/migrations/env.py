"""Migration job supplies a connection; runtime startup does not migrate."""

from alembic import context

connection = context.config.attributes.get("connection")
if connection is None:
    raise RuntimeError("Processing migrations require an explicit owner connection")
context.configure(connection=connection, target_metadata=None)
with context.begin_transaction():
    context.run_migrations()
