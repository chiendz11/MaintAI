import json
from pathlib import Path

from drift_monitor.cli import main
from drift_monitor.models import DriftReport

ROOT = Path(__file__).resolve().parents[3]


def write_events(path, samples):
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "event_id": row.event_id,
                    "received_at": row.received_at.isoformat(),
                    "features": row.features.model_dump(),
                }
            )
            for row in samples
        )
    )


def test_committed_schemas_match_runtime(tmp_path):
    assert main(["export-schemas", "--directory", str(tmp_path)]) == 0
    for name in ["drift-reference", "drift-report"]:
        assert json.loads((tmp_path / f"{name}.schema.json").read_text()) == json.loads(
            (ROOT / "contracts" / f"{name}.schema.json").read_text()
        )


def test_offline_commands_build_reference_and_report(tmp_path, samples):
    events, reference, report = [
        tmp_path / name for name in ["events.jsonl", "ref.json", "out.json"]
    ]
    write_events(events, samples)
    assert (
        main(
            [
                "build-reference",
                "--events",
                str(events),
                "--reference-source",
                "test_fixture",
                "--model-version",
                "fixture-model",
                "--dataset-version",
                "fixture-data",
                "--output",
                str(reference),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "evaluate",
                "--events",
                str(events),
                "--reference",
                str(reference),
                "--model-version",
                "fixture-model",
                "--dataset-version",
                "fixture-data",
                "--output",
                str(report),
                "--metrics-file",
                str(tmp_path / "metrics.prom"),
            ]
        )
        == 0
    )
    parsed = DriftReport.model_validate_json(report.read_text())
    assert parsed.overall_status == "STABLE"
    assert parsed.policy["minimum_samples"] == 200
    assert (
        'maintai_drift_overall_status{status="STABLE"} 1.0'
        in (tmp_path / "metrics.prom").read_text()
    )


def test_missing_reference_outputs_error_report(config, samples, tmp_path, capsys):
    events = tmp_path / "events.jsonl"
    write_events(events, samples)
    config.reference_path.unlink()
    assert (
        main(
            [
                "evaluate",
                "--events",
                str(events),
                "--reference",
                str(config.reference_path),
                "--model-version",
                config.model_version,
                "--dataset-version",
                config.dataset_version,
            ]
        )
        == 1
    )
    assert json.loads(capsys.readouterr().out)["reason"] == "DRIFT_REFERENCE_NOT_FOUND"


def test_bad_configuration_does_not_log_database_password(monkeypatch, capsys):
    monkeypatch.setenv("DRIFT_DATABASE_URL", "postgresql://user:top-secret@db/test")
    assert main(["run"]) == 2
    assert "top-secret" not in capsys.readouterr().err
