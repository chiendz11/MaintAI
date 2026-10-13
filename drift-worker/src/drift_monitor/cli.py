import argparse
import json
import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from drift_monitor import __version__
from drift_monitor.config import DriftConfig
from drift_monitor.data_loader import load_jsonl
from drift_monitor.logging import configure_logging, log
from drift_monitor.metrics import render_metrics
from drift_monitor.models import DriftReport, DriftStatus, ReferenceProfile
from drift_monitor.reference import build_reference, reference_features_from_jsonl
from drift_monitor.report import evaluate
from drift_monitor.repository import PostgresRepository
from drift_monitor.worker import run_worker


def write_json(value, output: Path | None):
    text = json.dumps(value, indent=2, allow_nan=False) + "\n"
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text)
    else:
        print(text, end="")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MaintAI one-shot Drift Worker")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "evaluate"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path)
        command.add_argument("--reference", dest="reference_path", type=Path)
        command.add_argument("--model-version")
        command.add_argument("--dataset-version")
        command.add_argument("--window-size", type=int)
        command.add_argument("--minimum-samples", type=int)
        command.add_argument("--worker-name")
        command.add_argument("--output", type=Path)
        command.add_argument("--metrics-file", type=Path)
        if name == "evaluate":
            command.add_argument("--events", required=True, type=Path)
    commands.add_parser("init-db", help="Create only worker-owned report/checkpoint tables")
    schema = commands.add_parser("export-schemas")
    schema.add_argument("--directory", type=Path, default=Path("contracts"))
    reference = commands.add_parser("build-reference", help="Explicit offline reference export")
    reference.add_argument("--events", type=Path, required=True)
    reference.add_argument("--model-version", required=True)
    reference.add_argument("--dataset-version", required=True)
    reference.add_argument(
        "--reference-source",
        required=True,
        choices=["training", "approved_reference", "test_fixture"],
    )
    reference.add_argument("--bins", type=int, default=10)
    reference.add_argument("--failure-rate", type=float)
    reference.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    configure_logging()
    try:
        if args.command == "export-schemas":
            for name, model in [
                ("drift-reference", ReferenceProfile),
                ("drift-report", DriftReport),
            ]:
                schema = model.model_json_schema()
                schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
                schema["$id"] = f"urn:maintai:contracts:{name}:1.0.0"
                write_json(schema, args.directory / f"{name}.schema.json")
            return 0
        if args.command == "init-db":
            url = os.environ.get("DRIFT_DATABASE_URL")
            if not url:
                raise ValueError("DRIFT_DATABASE_URL is required")
            PostgresRepository(url).init_db()
            log("drift_tables_ready")
            return 0
        if args.command == "build-reference":
            reference = build_reference(
                reference_features_from_jsonl(args.events),
                model_version=args.model_version,
                dataset_version=args.dataset_version,
                reference_source=args.reference_source,
                bin_count=args.bins,
                failure_rate=args.failure_rate,
            )
            write_json(reference.model_dump(mode="json"), args.output)
            return 0
        overrides = {
            key: value for key, value in vars(args).items() if key in DriftConfig.model_fields
        }
        config = DriftConfig.load(args.config, overrides)
        if args.command == "evaluate":
            report = evaluate(config, load_jsonl(args.events, config.model_version))
            exit_code = int(report.overall_status == DriftStatus.ERROR)
            if args.metrics_file:
                args.metrics_file.parent.mkdir(parents=True, exist_ok=True)
                args.metrics_file.write_bytes(render_metrics(report, {}, None))
        else:
            if config.database_url is None:
                raise ValueError("DRIFT_DATABASE_URL is required for database runs")
            result = run_worker(
                config,
                PostgresRepository(config.database_url.get_secret_value()),
                metrics_file=args.metrics_file,
            )
            report, exit_code = result.report, result.exit_code
        write_json(report.model_dump(mode="json"), args.output)
        return exit_code
    except KeyboardInterrupt:
        log("drift_interrupted")
        return 130
    except ValidationError as exc:
        log(
            "drift_configuration_error",
            errors=exc.errors(include_input=False, include_context=False),
        )
        return 2
    except (OSError, ValueError, yaml.YAMLError):
        log("drift_input_error")
        return 2
    except Exception as exc:
        log("drift_command_failed", error_type=type(exc).__name__)
        return 1
