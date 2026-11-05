"""Explicit owner operations; no startup migrations or HTTP mutation endpoint."""

import argparse
import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

from backend.shared.projections.generations import bootstrap
from backend.shared.projections.rebuild import RebuildRepository


def main(argv=None):
    parser = argparse.ArgumentParser(description="Business owner projection recovery operations")
    commands = parser.add_subparsers(dest="operation", required=True)
    commands.add_parser("bootstrap").add_argument(
        "--writers-drained", action="store_true", required=True
    )
    start = commands.add_parser("start")
    for argument in (
        "expected-active",
        "operator",
        "source-cluster",
        "telemetry-group",
        "prediction-group",
        "deadline",
    ):
        start.add_argument("--" + argument, required=True)
    start.add_argument("--journal-budget-bytes", type=int, required=True)
    for operation in ("status", "resume", "verify", "activate", "abort"):
        command = commands.add_parser(operation)
        command.add_argument("job_id")
        if operation == "activate":
            command.add_argument("--expected-active", required=True)
    args = parser.parse_args(argv)

    @contextmanager
    def factory():
        dsn = os.environ.get("BUSINESS_REBUILD_DATABASE_URL")
        if not dsn:
            raise ValueError("Explicit BUSINESS_REBUILD_DATABASE_URL owner credential is required")
        with psycopg.connect(
            dsn,
            row_factory=dict_row,
            connect_timeout=2,
            options="-c statement_timeout=3000 -c lock_timeout=3000",
        ) as connection:
            yield connection

    try:
        repository = RebuildRepository(factory)
        if args.operation == "bootstrap":
            with factory() as connection, connection.transaction():
                output = {
                    "generation_id": bootstrap(connection, writers_drained=args.writers_drained)
                }
        elif args.operation == "start":
            output = {
                "job_id": repository.start(
                    expected_active=args.expected_active,
                    operator=args.operator,
                    source_cluster=args.source_cluster,
                    groups={
                        "sensor.readings": args.telemetry_group,
                        "inference.results": args.prediction_group,
                    },
                    deadline=datetime.fromisoformat(args.deadline.replace("Z", "+00:00")),
                    journal_budget=args.journal_budget_bytes,
                )
            }
        elif args.operation == "status":
            row = repository.status(args.job_id)
            output = {
                k: row[k]
                for k in (
                    "job_id",
                    "generation_id",
                    "expected_active",
                    "status",
                    "deadline",
                    "journal_bytes",
                    "journal_count",
                    "applied_count",
                    "journal_budget",
                    "baseline_done",
                    "applied_journal",
                    "last_error",
                    "created_at",
                    "updated_at",
                )
            }
        elif args.operation == "activate":
            output = {
                "generation_id": repository.activate(
                    args.job_id, expected_active=args.expected_active
                )
            }
        else:
            getattr(repository, args.operation)(args.job_id)
            output = {"job_id": args.job_id, "operation": args.operation}
        print(json.dumps(output, default=str))
        return 0
    except Exception as exc:
        # Never expose connection strings, tokens or provider exception messages.
        print("Business rebuild operation failed: " + type(exc).__name__, file=sys.stderr)
        return 1
