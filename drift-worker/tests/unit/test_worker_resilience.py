import json
from datetime import UTC, datetime

import psycopg

from drift_monitor.logging import configure_logging
from drift_monitor.metrics import render_metrics
from drift_monitor.models import Checkpoint, DriftStatus
from drift_monitor.worker import run_worker


class MemoryRepository:
    def __init__(self, samples):
        self.samples = samples
        self.reports = {}
        self.current_checkpoint = None

    def load_checkpoint(self, worker_name):
        return self.current_checkpoint

    def load_window(self, size, model_version):
        return self.samples[-size:]

    def find_report(self, report_id):
        return self.reports.get(report_id)

    def save_report(self, report):
        return self.reports.setdefault(report.report_id, report.model_copy(deep=True))

    def counts(self):
        return {
            status.value: sum(report.overall_status == status for report in self.reports.values())
            for status in DriftStatus
        }

    def checkpoint(self, worker_name, report, latest, now):
        self.current_checkpoint = Checkpoint(
            worker_name=worker_name,
            last_processed_event_at=latest.received_at if latest else None,
            last_processed_event_id=latest.event_id if latest else None,
            last_success_at=now,
            last_report_id=report.report_id,
        )


def test_repeated_window_reuses_report_and_durable_count(config, samples, tmp_path):
    repo = MemoryRepository(samples)
    metrics_file = tmp_path / "metrics.prom"
    first = run_worker(config, repo, metrics_file=metrics_file)
    second = run_worker(config, repo, metrics_file=metrics_file)
    assert first.exit_code == second.exit_code == 0
    assert first.reused is False and second.reused is True
    assert first.report.computed_at == second.report.computed_at
    assert len(repo.reports) == 1
    assert repo.current_checkpoint.last_report_id == first.report.report_id
    metrics = metrics_file.read_text()
    assert 'maintai_drift_reports_total{status="STABLE"} 1.0' in metrics
    assert "last_success_timestamp_seconds" in metrics
    assert all(name not in metrics for name in ["event_id", "report_id", "machine_id"])


def test_required_metrics_failure_does_not_checkpoint_and_retry_recovers(
    config,
    samples,
    monkeypatch,
):
    repo = MemoryRepository(samples)
    config.pushgateway_url = "http://pushgateway"
    config.pushgateway_required = True

    def failed_push(*args):
        raise ConnectionError("Pushgateway is down")

    monkeypatch.setattr("drift_monitor.worker.push_metrics", failed_push)
    failed = run_worker(config, repo)
    assert failed.exit_code == 1
    assert failed.execution_error == "PUSHGATEWAY_UNAVAILABLE"
    assert failed.report.overall_status == DriftStatus.STABLE
    assert len(repo.reports) == 1
    assert repo.current_checkpoint is None
    published = []
    monkeypatch.setattr(
        "drift_monitor.worker.push_metrics", lambda cfg, content: published.append(content)
    )
    retried = run_worker(config, repo)
    assert retried.exit_code == 0 and retried.reused
    assert len(repo.reports) == 1
    assert "last_success_timestamp_seconds" not in published[0].decode()
    assert "last_success_timestamp_seconds" in published[1].decode()


def test_db_failure_is_error_without_secret_logging(config, samples, capsys):
    class DownRepository(MemoryRepository):
        def load_window(self, *args):
            raise psycopg.OperationalError("password=do-not-log-this")

    configure_logging()
    result = run_worker(config, DownRepository(samples))
    assert result.exit_code == 1
    assert result.report.reason == "DRIFT_COMPUTE_ERROR"
    assert "do-not-log-this" not in capsys.readouterr().err


def test_persist_failure_does_not_checkpoint(config, samples):
    class FailedPersist(MemoryRepository):
        def save_report(self, *args):
            raise psycopg.OperationalError("DB write failed")

    repo = FailedPersist(samples)
    result = run_worker(config, repo)
    assert result.exit_code == 1
    assert repo.current_checkpoint is None
    assert result.report.overall_status == DriftStatus.ERROR


def test_error_report_keeps_previous_success_heartbeat(config, samples):
    repo = MemoryRepository(samples)
    successful = run_worker(config, repo)
    before = repo.current_checkpoint
    config.reference_path.unlink()
    errored = run_worker(config, repo)
    assert successful.exit_code == 0 and errored.exit_code == 1
    assert repo.current_checkpoint == before
    metrics = render_metrics(
        errored.report, repo.counts(), before.last_success_at.timestamp()
    ).decode()
    assert 'maintai_drift_overall_status{status="ERROR"} 1.0' in metrics
    assert "maintai_drift_feature_score{" not in metrics


def test_no_window_uses_insufficient_data_checkpoint(config):
    repo = MemoryRepository([])
    result = run_worker(config, repo)
    assert result.exit_code == 0
    assert result.report.overall_status == DriftStatus.INSUFFICIENT_DATA
    assert repo.current_checkpoint.last_processed_event_id is None


def test_missing_reference_does_not_query_runtime_window(config):
    class NoRuntimeQuery(MemoryRepository):
        def load_window(self, *args):
            raise AssertionError("Invalid reference must stop before loading runtime data")

    config.reference_path.unlink()
    result = run_worker(config, NoRuntimeQuery([]))
    assert result.report.reason == "DRIFT_REFERENCE_NOT_FOUND"
    assert result.report.current_window.sample_count == 0


def test_insufficient_report_schema_and_timestamp(config, samples):
    from drift_monitor.models import DriftReport

    result = run_worker(config, MemoryRepository(samples[:10]))
    encoded = result.report.model_dump_json()
    restored = DriftReport.model_validate_json(encoded)
    assert restored == result.report
    assert restored.computed_at.utcoffset().total_seconds() == 0
    assert isinstance(json.loads(encoded)["computed_at"], str)
    assert restored.computed_at <= datetime.now(UTC)
