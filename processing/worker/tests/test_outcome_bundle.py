"""Bundle cutover must preserve the SQL required for a fresh owner install."""

import importlib
from types import SimpleNamespace

import pytest
from maintai_messaging.contracts import default_contracts


def test_ordering_migration_accepts_current_bundle_without_changing_sql(monkeypatch):
    migration = importlib.import_module("processing.migrations.versions.0002_ordering")
    contracts = default_contracts()
    assert contracts.bundle_version == "2.0.0"
    statements = []
    connection = SimpleNamespace(exec_driver_sql=statements.append)
    monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
    migration.upgrade()
    expected = (contracts.directory / "migrations/002_processing_ordering.sql").read_bytes()
    assert statements[0].encode("utf-8") == expected


def test_ordering_migration_rejects_changed_sql_before_any_database_access(monkeypatch, tmp_path):
    migration = importlib.import_module("processing.migrations.versions.0002_ordering")
    original = default_contracts().directory / "migrations/002_processing_ordering.sql"
    target = tmp_path / "migrations/002_processing_ordering.sql"
    target.parent.mkdir()
    target.write_bytes(original.read_bytes() + b"\n")
    monkeypatch.setattr(migration, "default_contracts", lambda: SimpleNamespace(directory=tmp_path))

    def unexpected_database_access():
        pytest.fail("Unreviewed SQL reached the database")

    monkeypatch.setattr(migration.op, "get_bind", unexpected_database_access)
    with pytest.raises(RuntimeError, match="frozen migration002"):
        migration.upgrade()
