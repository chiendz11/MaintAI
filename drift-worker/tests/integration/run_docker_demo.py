"""Run sensor/worker containers against an isolated Postgres + Pushgateway test project."""

import json
import os
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import psycopg
from jsonschema import Draft202012Validator, FormatChecker
from prometheus_client.parser import text_string_to_metric_families
from support import create_fixture_tables, seed_events

from drift_monitor.repository import PostgresRepository

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "drift-worker/tests/integration/compose.yaml"
TEST_DB = "postgresql://maintai_test:maintai_test@localhost:5433/maintai_test"


def main():
    with tempfile.TemporaryDirectory(prefix="maintai-drift-docker-") as tmp:
        artifacts = Path(tmp)
        artifacts.chmod(0o755)
        env = {**os.environ, "DRIFT_FIXTURE_DIR": str(artifacts)}
        prefix = ["docker", "compose", "--file", str(COMPOSE)]

        def compose(*args, capture=False):
            result = subprocess.run(
                [*prefix, *args],
                env=env,
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE if capture else None,
                check=True,
            )
            return result.stdout

        try:
            compose("up", "--detach", "--wait", "postgres", "pushgateway")
            compose("build", "sensor", "worker")
            create_fixture_tables(TEST_DB)
            repo = PostgresRepository(TEST_DB)
            repo.init_db()
            normal = compose(
                "run",
                "--rm",
                "--no-deps",
                "sensor",
                "replay",
                "--dry-run",
                "--dataset",
                "/data/ai4i.csv",
                "--loop-dataset",
                "--limit",
                "1000",
                "--run-id",
                "baseline-fixture",
                capture=True,
            )
            (artifacts / "baseline.jsonl").write_text(normal)
            reference = compose(
                "run",
                "--rm",
                "--no-deps",
                "worker",
                "build-reference",
                "--events",
                "/fixtures/baseline.jsonl",
                "--model-version",
                "fixture-model-v1",
                "--dataset-version",
                "fixture-ai4i-v1",
                "--reference-source",
                "test_fixture",
                capture=True,
            )
            (artifacts / "reference.json").write_text(reference)
            schema = json.loads(
                (ROOT / "contracts/v1.3/schemas/drift-report.schema.json").read_text()
            )
            validator = Draft202012Validator(schema, format_checker=FormatChecker())
            for index, (scenario, expected) in enumerate(
                [
                    ("normal", "STABLE"),
                    ("sudden-drift", "CRITICAL"),
                    ("gradual-drift", "CRITICAL"),
                    ("categorical-drift", "WARNING"),
                ]
            ):
                start_time = (
                    datetime(2026, 10, 12, tzinfo=UTC) + timedelta(days=index)
                ).isoformat()
                jsonl = compose(
                    "run",
                    "--rm",
                    "--no-deps",
                    "sensor",
                    "replay",
                    "--dry-run",
                    "--dataset",
                    "/data/ai4i.csv",
                    "--loop-dataset",
                    "--limit",
                    "1000",
                    "--scenario",
                    scenario,
                    "--run-id",
                    f"docker-fixture-{scenario}",
                    "--start-time",
                    start_time,
                    capture=True,
                )
                with psycopg.connect(TEST_DB) as conn:
                    conn.execute("TRUNCATE predictions, sensor_events")
                seed_events(TEST_DB, [json.loads(line) for line in jsonl.splitlines()])
                args = (
                    "run",
                    "--rm",
                    "--no-deps",
                    "worker",
                    "run",
                    "--reference",
                    "/fixtures/reference.json",
                    "--model-version",
                    "fixture-model-v1",
                    "--dataset-version",
                    "fixture-ai4i-v1",
                )
                report = json.loads(compose(*args, capture=True))
                validator.validate(report)
                assert report["overall_status"] == expected, report
                repeat = json.loads(compose(*args, capture=True))
                assert repeat == report
                assert sum(repo.counts().values()) == index + 1
                metrics = httpx.get("http://localhost:9092/metrics", timeout=5).text
                observed = [
                    sample
                    for family in text_string_to_metric_families(metrics)
                    for sample in family.samples
                    if sample.name == "maintai_drift_overall_status"
                ]
                assert any(
                    sample.labels["status"] == expected and sample.value == 1 for sample in observed
                )
                assert "maintai_drift_worker_last_success_timestamp_seconds" in metrics
                print(
                    f"{scenario}: {expected}, persisted/checkpointed/published; retry deduplicated"
                )
            with psycopg.connect(TEST_DB) as conn:
                assert conn.execute("SELECT DISTINCT state FROM machines").fetchall() == [
                    ("NORMAL",)
                ]
        finally:
            compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
