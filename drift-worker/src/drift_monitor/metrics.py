import httpx
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

from drift_monitor.config import DriftConfig
from drift_monitor.models import FEATURE_NAMES, DriftReport, DriftStatus


def render_metrics(
    report: DriftReport, counts: dict[str, int], last_success: float | None
) -> bytes:
    registry = CollectorRegistry()
    score = Gauge(
        "maintai_drift_feature_score",
        "Latest feature PSI",
        ["feature", "metric"],
        registry=registry,
    )
    state = Gauge(
        "maintai_drift_feature_status",
        "Latest feature state",
        ["feature", "status"],
        registry=registry,
    )
    results = {result.feature: result for result in report.features}
    for name in FEATURE_NAMES:
        result = results.get(name)
        if result:
            score.labels(name, result.metric).set(result.score)
        status = result.status if result else report.overall_status
        for possible in DriftStatus:
            state.labels(name, possible.value).set(int(status == possible))
    overall = Gauge(
        "maintai_drift_overall_status", "Latest report state", ["status"], registry=registry
    )
    for status in DriftStatus:
        overall.labels(status.value).set(int(report.overall_status == status))
    Gauge("maintai_drift_features_drifted", "Drifted feature count", registry=registry).set(
        report.summary.warning_features + report.summary.critical_features,
    )
    Gauge("maintai_drift_window_samples", "Valid samples in latest window", registry=registry).set(
        report.current_window.sample_count,
    )
    totals = Counter(
        "maintai_drift_reports_total", "Persisted unique reports", ["status"], registry=registry
    )
    for status in DriftStatus:
        totals.labels(status.value).inc(counts.get(status.value, 0))
    if last_success is not None:
        Gauge(
            "maintai_drift_worker_last_success_timestamp_seconds",
            "Last successful batch",
            registry=registry,
        ).set(last_success)
    if report.prediction_drift and report.prediction_drift.current_failure_rate is not None:
        Gauge(
            "maintai_prediction_failure_ratio", "Matching model failure ratio", registry=registry
        ).set(
            report.prediction_drift.current_failure_rate,
        )
    return generate_latest(registry)


def push_metrics(config: DriftConfig, content: bytes) -> None:
    from urllib.parse import quote

    # PUT replaces the grouping key, removing stale feature/score series from older runs.
    url = (
        f"{config.pushgateway_url.rstrip('/')}/metrics/job/{quote(config.metrics_job, safe='')}"
        f"/environment/{quote(config.environment, safe='')}"
    )
    with httpx.Client(timeout=config.timeout_seconds) as client:
        client.put(
            url, content=content, headers={"Content-Type": CONTENT_TYPE_LATEST}
        ).raise_for_status()
