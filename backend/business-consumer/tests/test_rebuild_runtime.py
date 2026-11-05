"""Opt-in recovery has no effect on existing runtime settings or public APIs."""

import pytest


def test_generation_flag_is_strict_and_default_off():
    from backend.shared.projections.config import generations_enabled

    assert generations_enabled({}) is False
    assert generations_enabled({"BUSINESS_PROJECTION_GENERATIONS_ENABLED": "true"}) is True
    with pytest.raises(ValueError):
        generations_enabled({"BUSINESS_PROJECTION_GENERATIONS_ENABLED": "yes"})


def test_rebuild_requires_generation_support_and_separate_owner_credential():
    from backend.shared.projections.config import RebuildSettings

    assert RebuildSettings.from_env({}, local=True) is None
    with pytest.raises(ValueError):
        RebuildSettings.from_env({"BUSINESS_REBUILD_ENABLED": "true"}, local=True)


def test_cli_help_exposes_owner_operations_without_opening_database(capsys):
    from backend.shared.projections.cli import main

    with pytest.raises(SystemExit) as result:
        main(["--help"])
    assert result.value.code == 0
    output = capsys.readouterr().out
    for operation in ("bootstrap", "start", "status", "resume", "verify", "activate", "abort"):
        assert operation in output


def test_rebuild_metrics_have_bounded_labels_without_identifiers():
    from backend.shared.projections.metrics import RebuildMetrics
    from prometheus_client import CollectorRegistry, generate_latest

    registry = CollectorRegistry()
    RebuildMetrics(registry)
    output = generate_latest(registry).decode()
    assert "maintai_projection_rebuild_jobs" in output
    assert "maintai_projection_rebuild_oldest_age_seconds" in output
    assert "job_id=" not in output and "machine_id=" not in output
