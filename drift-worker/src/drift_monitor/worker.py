from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from drift_monitor.config import DriftConfig
from drift_monitor.logging import log
from drift_monitor.metrics import push_metrics, render_metrics
from drift_monitor.models import DriftReport, DriftStatus
from drift_monitor.reference import ReferenceError, load_reference
from drift_monitor.report import evaluate
from drift_monitor.repository import Repository


@dataclass
class RunResult:
    report: DriftReport
    exit_code: int
    reused: bool = False
    execution_error: str | None = None


def run_worker(
    config: DriftConfig, repository: Repository, *, metrics_file: Path | None = None
) -> RunResult:
    samples = []
    now = datetime.now(UTC)
    try:
        checkpoint = repository.load_checkpoint(config.worker_name)
        log(
            "drift_started",
            model_version=config.model_version,
            dataset_version=config.dataset_version,
            previous_report_id=checkpoint.last_report_id if checkpoint else None,
        )
        reference, reference_error = None, None
        try:
            reference = load_reference(
                config.reference_path, config.model_version, config.dataset_version
            )
        except ReferenceError as exc:
            reference_error = str(exc)
        if reference_error is None:
            samples = repository.load_window(config.window_size, config.model_version)
        report = evaluate(
            config, samples, computed_at=now, reason=reference_error, reference_profile=reference
        )
        stored = repository.find_report(report.report_id)
        reused = stored is not None
        report = stored if reused else repository.save_report(report)
        success = report.overall_status != DriftStatus.ERROR
        metrics = render_metrics(
            report,
            repository.counts(),
            checkpoint.last_success_at.timestamp() if checkpoint else None,
        )
        if metrics_file:
            metrics_file.parent.mkdir(parents=True, exist_ok=True)
            metrics_file.write_bytes(metrics)
        if config.pushgateway_url:
            try:
                push_metrics(config, metrics)
            except Exception as exc:
                log(
                    "metrics_push_failed", error_type=type(exc).__name__, report_id=report.report_id
                )
                if config.pushgateway_required:
                    # Keep the saved drift conclusion, but do not checkpoint failed publication.
                    return RunResult(report, 1, reused, "PUSHGATEWAY_UNAVAILABLE")
        if success:
            latest = (
                max(samples, key=lambda row: (row.received_at, row.event_id)) if samples else None
            )
            repository.checkpoint(config.worker_name, report, latest, now)
            # Publish the success heartbeat only after the checkpoint committed.
            committed = repository.load_checkpoint(config.worker_name)
            metrics = render_metrics(
                report,
                repository.counts(),
                committed.last_success_at.timestamp() if committed else None,
            )
            if metrics_file:
                metrics_file.write_bytes(metrics)
            if config.pushgateway_url:
                try:
                    push_metrics(config, metrics)
                except Exception as exc:
                    # The report was already published; retry can refresh the heartbeat.
                    log("heartbeat_push_failed", error_type=type(exc).__name__)
        log(
            "drift_report_computed",
            report_id=report.report_id,
            model_version=config.model_version,
            reference_dataset_version=config.dataset_version,
            sample_count=report.current_window.sample_count,
            overall_status=report.overall_status.value,
            reused=reused,
            reason=report.reason,
        )
        return RunResult(report, 0 if success else 1, reused)
    except Exception as exc:
        # Do not log a connection string, credentials or raw DB errors.
        log("drift_worker_failed", error_type=type(exc).__name__)
        error_report = evaluate(config, samples, computed_at=now, reason="DRIFT_COMPUTE_ERROR")
        return RunResult(error_report, 1, execution_error="DRIFT_COMPUTE_ERROR")
