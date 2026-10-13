import psycopg
import pytest

from drift_monitor.models import DriftStatus, PredictionReference
from drift_monitor.report import evaluate
from drift_monitor.worker import run_worker

from .support import seed_events, wire_events

pytestmark = pytest.mark.postgres


def test_valid_filter_and_idempotency(config, samples, postgres_repo):
    events = wire_events(samples)
    invalid = dict(events[-1], event_id="evt_invalid", validation_status="INVALID")
    invalid["features"] = {"product_type": "INVALID"}
    seed_events(postgres_repo.database_url, [*events, invalid])
    first = run_worker(config, postgres_repo)
    second = run_worker(config, postgres_repo)
    assert first.exit_code == second.exit_code == 0
    assert second.reused
    assert first.report == second.report
    assert first.report.current_window.sample_count == 500
    assert postgres_repo.counts() == {"STABLE": 1}
    checkpoint = postgres_repo.load_checkpoint(config.worker_name)
    assert checkpoint.last_processed_event_id == samples[-1].event_id


def test_reference_error_persisted_without_advancing_checkpoint(config, samples, postgres_repo):
    seed_events(postgres_repo.database_url, wire_events(samples))
    assert run_worker(config, postgres_repo).exit_code == 0
    before = postgres_repo.load_checkpoint(config.worker_name)
    config.model_version = "wrong-model"
    failed = run_worker(config, postgres_repo)
    assert failed.exit_code == 1
    assert failed.report.reason == "DRIFT_REFERENCE_NOT_FOUND"
    assert postgres_repo.load_checkpoint(config.worker_name) == before
    assert postgres_repo.counts()["ERROR"] == 1


def test_old_prediction_versions_excluded(config, reference, samples, postgres_repo):
    reference.prediction_reference = PredictionReference(failure_rate=0.1)
    config.reference_path.write_text(reference.model_dump_json())
    seed_events(postgres_repo.database_url, wire_events(samples))
    with psycopg.connect(postgres_repo.database_url) as conn:
        for index, sample in enumerate(samples):
            conn.execute(
                "INSERT INTO predictions (prediction_id,event_id,label,failure_probability,"
                "model_version) VALUES (%s,%s,'FAILURE',0.9,%s)",
                (
                    f"pred_{index}",
                    sample.event_id,
                    config.model_version if index < 200 else "old-model",
                ),
            )
    result = run_worker(config, postgres_repo)
    assert result.report.prediction_drift.sample_count == 200
    assert result.report.prediction_drift.current_failure_rate == 1
    assert result.report.overall_status == DriftStatus.WARNING
    with psycopg.connect(postgres_repo.database_url) as conn:
        assert conn.execute("SELECT state FROM machines").fetchone()[0] == "NORMAL"


def test_checkpoint_does_not_regress(config, samples, postgres_repo):
    newer = evaluate(config, samples)
    older = evaluate(config, samples[:250])
    postgres_repo.save_report(newer)
    postgres_repo.save_report(older)
    postgres_repo.checkpoint(config.worker_name, newer, samples[-1], newer.computed_at)
    postgres_repo.checkpoint(config.worker_name, older, samples[249], older.computed_at)
    assert postgres_repo.load_checkpoint(config.worker_name).last_report_id == newer.report_id


def test_late_predictions_recompute_same_sensor_window(config, reference, samples, postgres_repo):
    reference.prediction_reference = PredictionReference(failure_rate=0.1)
    config.reference_path.write_text(reference.model_dump_json())
    seed_events(postgres_repo.database_url, wire_events(samples))
    first = run_worker(config, postgres_repo)
    with psycopg.connect(postgres_repo.database_url) as conn:
        for index, sample in enumerate(samples):
            conn.execute(
                "INSERT INTO predictions (prediction_id,event_id,label,failure_probability,"
                "model_version) VALUES (%s,%s,'FAILURE',0.9,%s)",
                (f"pred_{index}", sample.event_id, config.model_version),
            )
    second = run_worker(config, postgres_repo)
    assert first.report.report_id != second.report.report_id
    assert second.report.prediction_drift.sample_count == 500
    assert sum(postgres_repo.counts().values()) == 2
